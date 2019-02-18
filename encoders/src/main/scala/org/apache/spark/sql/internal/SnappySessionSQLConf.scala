/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.internal

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, TypedConfigBuilder}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder

import scala.reflect.{ClassTag, classTag}

class SQLConfigEntry private(private[sql] val entry: ConfigEntry[_]) {

  def key: String = entry.key

  def doc: String = entry.doc

  def isPublic: Boolean = entry.isPublic

  def defaultValue[T]: Option[T] = entry.defaultValue.asInstanceOf[Option[T]]

  def defaultValueString: String = entry.defaultValueString

  def valueConverter[T]: String => T =
    entry.asInstanceOf[ConfigEntry[T]].valueConverter

  def stringConverter[T]: T => String =
    entry.asInstanceOf[ConfigEntry[T]].stringConverter

  override def toString: String = entry.toString
}

object SQLConfigEntry {

  private def handleDefault[T](entry: TypedConfigBuilder[T],
                               defaultValue: Option[T]): SQLConfigEntry = defaultValue match {
    case Some(v) => new SQLConfigEntry(entry.createWithDefault(v))
    case None => new SQLConfigEntry(entry.createOptional)
  }

  def sparkConf[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
                             isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](ConfigBuilder(key)
        .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](ConfigBuilder(key)
        .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](ConfigBuilder(key)
        .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](ConfigBuilder(key)
        .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](ConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }

  def apply[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
                         isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](SQLConfigBuilder(key)
        .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](SQLConfigBuilder(key)
        .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](SQLConfigBuilder(key)
        .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](SQLConfigBuilder(key)
        .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](SQLConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }
}

trait AltName[T] {

  def name: String

  def altName: String

  def configEntry: SQLConfigEntry

  def defaultValue: Option[T] = configEntry.defaultValue[T]

  def getOption(conf: SparkConf): Option[String] = if (altName == null) {
    conf.getOption(name)
  } else {
    conf.getOption(name) match {
      case s: Some[String] => // check if altName also present and fail if so
        if (conf.contains(altName)) {
          throw new IllegalArgumentException(
            s"Both $name and $altName configured. Only one should be set.")
        } else s
      case None => conf.getOption(altName)
    }
  }

  private def get(conf: SparkConf, name: String,
                  defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.get(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.get(name, defaultValue)).get
    }
  }

  def get(conf: SparkConf): T = if (altName == null) {
    get(conf, name, configEntry.defaultValueString)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, name, configEntry.defaultValueString)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def get(properties: Properties): T = {
    val propertyValue = getProperty(properties)
    if (propertyValue ne null) configEntry.valueConverter[T](propertyValue)
    else defaultValue.get
  }

  def getProperty(properties: Properties): String = if (altName == null) {
    properties.getProperty(name)
  } else {
    val v = properties.getProperty(name)
    if (v != null) {
      // check if altName also present and fail if so
      if (properties.getProperty(altName) != null) {
        throw new IllegalArgumentException(
          s"Both $name and $altName specified. Only one should be set.")
      }
      v
    } else properties.getProperty(altName)
  }

  def unapply(key: String): Boolean = name.equals(key) ||
    (altName != null && altName.equals(key))
}

trait SQLAltName[T] extends AltName[T] {

  private def get(conf: SQLConf, entry: SQLConfigEntry): T = {
    entry.defaultValue match {
      case Some(_) => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[T]])
      case None => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[Option[T]]]).get
    }
  }

  private def get(conf: SQLConf, name: String,
                  defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.getConfString(name, defaultValue)).get
    }
  }

  def get(conf: SQLConf): T = if (altName == null) {
    get(conf, configEntry)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, configEntry)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def getOption(conf: SQLConf): Option[T] = if (altName == null) {
    if (conf.contains(name)) Some(get(conf, name, "<undefined>"))
    else defaultValue
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) Some(get(conf, name, ""))
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else if (conf.contains(altName)) {
      Some(get(conf, altName, ""))
    } else defaultValue
  }

  def set(conf: SQLConf, value: T, useAltName: Boolean = false): Unit = {
    if (useAltName) {
      conf.setConfString(altName, configEntry.stringConverter(value))
    } else {
      conf.setConf[T](configEntry.entry.asInstanceOf[ConfigEntry[T]], value)
    }
  }

  def remove(conf: SQLConf, useAltName: Boolean = false): Unit = {
    conf.unsetConf(if (useAltName) altName else name)
  }
}

