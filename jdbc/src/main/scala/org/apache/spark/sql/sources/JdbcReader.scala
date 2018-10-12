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
package org.apache.spark.sql.sources

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.{Map => SMap}

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, DataFrameReader, SnappyDataPoolDialect, SparkSession}

/**
 * Implicit class to easily invoke JDBC provider on SparkSession and avoid double query
 * execution of pushdown queries (one for schema determination and other the actual query).
 *
 * Instead of: spark.jdbc.read(jdbcUrl, pushdown query, properties) one can simply do
 * spark.snappySql(query). This will also register dialects that avoid double execution,
 * use proper JDBC driver argument to avoid ClassNotFound errors.
 */
case class JdbcReader(session: SparkSession) extends DataFrameReader(session) {

  SnappyDataPoolDialect.register()
  SparkSession.setActiveSession(session)

  private def getURL(host: String, port: Int): String = port match {
    case _ if port > 0 => s"${Constant.POOLED_THIN_CLIENT_URL}$host[$port]"
    case _ =>
      val conf = session.sparkContext.conf
      val sparkProp = s"${Constant.SPARK_PREFIX}${Constant.CONNECTION_PROPERTY}"
      val hostPort = conf.getOption(sparkProp) match {
        case Some(c) => c
        case None => conf.getOption(Constant.CONNECTION_PROPERTY) match {
          case Some(c) => c
          case None => throw new IllegalStateException(
            s"Neither $sparkProp nor ${Constant.CONNECTION_PROPERTY} set for SnappyData connect")
        }
      }
      s"${Constant.POOLED_THIN_CLIENT_URL}$hostPort"
  }

  /** set the user/password in the property bag if not present and set in session */
  private def initUserPassword(properties: SMap[String, String]): SMap[String, String] = {
    var props = properties
    val conf = session.conf
    var prefix = Constant.STORE_PROPERTY_PREFIX
    (conf.getOption(prefix + Attribute.USERNAME_ATTR) match {
      case None =>
        prefix = Constant.SPARK_STORE_PREFIX
        conf.getOption(prefix + Attribute.USERNAME_ATTR)
      case s => s
    }) match {
      case None =>
      case Some(user) =>
        if (!props.contains(Attribute.USERNAME_ATTR) &&
            !props.contains(Attribute.USERNAME_ALT_ATTR)) {
          props += (Attribute.USERNAME_ATTR -> user)
          conf.getOption(prefix + Attribute.PASSWORD_ATTR) match {
            case Some(password) => props += (Attribute.PASSWORD_ATTR -> password)
            case None =>
          }
        }
    }
    props
  }

  def snappySql(sql: String): DataFrame =
    snappySql(sql, host = null, port = -1, Map.empty[String, String], initCredentials = true)

  def snappySql(sql: String, host: String, port: Int): DataFrame =
    snappySql(sql, host, port, Map.empty[String, String], initCredentials = true)

  def snappySql(sql: String, user: String, password: String): DataFrame =
    snappySql(sql, host = null, port = -1, user, password)

  def snappySql(sql: String, host: String, port: Int, user: String, password: String): DataFrame = {
    snappySql(sql, host, port, Map("user" -> user, "password" -> password),
      initCredentials = false)
  }

  def snappySql(sql: String, host: String, port: Int, properties: Properties): DataFrame =
    snappySql(sql, host, port, properties.asScala, initCredentials = true)

  def snappySql(sql: String, host: String, port: Int,
      properties: scala.collection.Map[String, String]): DataFrame = {
    snappySql(sql, host, port, properties, initCredentials = true)
  }

  private def snappySql(sql: String, host: String, port: Int,
      properties: SMap[String, String], initCredentials: Boolean): DataFrame = {
    option(JDBCOptions.JDBC_URL, getURL(host, port))
    option(JDBCOptions.JDBC_DRIVER_CLASS, Constant.JDBC_CLIENT_POOL_DRIVER)
    option(JDBCOptions.JDBC_TABLE_NAME, s"($sql) queryTable")
    val props = if (initCredentials) initUserPassword(properties) else properties
    if (props.nonEmpty) options(props)
    format("jdbc").load()
  }
}
