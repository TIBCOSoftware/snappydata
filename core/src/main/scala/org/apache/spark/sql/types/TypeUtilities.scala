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
package org.apache.spark.sql.types

import java.util.Properties

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.CodegenSupport


object TypeUtilities {

  private[spark] val (rddIdField, rddStorageLevelField) = {
    val c = classOf[RDD[_]]
    val fields = c.getDeclaredFields
    val f1 = fields.find(f => f.getName == "_id" || f.getName == "id").get
    val f2 = fields.find(_.getName.endsWith("storageLevel")).get
    f1.setAccessible(true)
    f2.setAccessible(true)
    (f1, f2)
  }

  private[spark] val (parentMethod, parentSetter) = {
    val c = classOf[CodegenSupport]
    val m = c.getDeclaredMethod("parent")
    m.setAccessible(true)
    val s = c.getDeclaredMethod("parent_$eq", classOf[CodegenSupport])
    s.setAccessible(true)
    (m, s)
  }

  def getMetadata[T](key: String, metadata: Metadata): Option[T] = {
    metadata.map.get(key).asInstanceOf[Option[T]]
  }

  def writeMetadata(metadata: Metadata, kryo: Kryo, output: Output): Unit = {
    val map = metadata.map
    if ((metadata eq Metadata.empty) || map.isEmpty) {
      output.writeVarInt(0, true)
    } else {
      output.writeVarInt(map.size, true)
      metadata.map.foreach { case (key, value) =>
        output.writeString(key)
        kryo.writeClassAndObject(output, value)
      }
    }
  }

  def readMetadata(kryo: Kryo, input: Input): Metadata = {
    var mapLen = input.readVarInt(true)
    if (mapLen <= 0) {
      Metadata.empty
    } else {
      val map = Map.newBuilder[String, Any]
      while (mapLen > 0) {
        val key = input.readString()
        val value = kryo.readClassAndObject(input)
        map += key -> value
        mapLen -= 1
      }
      new Metadata(map.result)
    }
  }

  def writeProperties(props: Properties, output: Output): Unit = {
    if (props != null) {
      val keys = props.stringPropertyNames()
      output.writeVarInt(keys.size(), true)
      val keysIterator = keys.iterator()
      while (keysIterator.hasNext) {
        val key = keysIterator.next()
        output.writeString(key)
        output.writeString(props.getProperty(key))
      }
    } else {
      output.writeVarInt(0, true)
    }
  }

  def readProperties(input: Input): Properties = {
    val props = new Properties()
    var numProperties = input.readVarInt(true)
    while (numProperties > 0) {
      val key = input.readString()
      props.setProperty(key, input.readString())
      numProperties -= 1
    }
    props
  }
}
