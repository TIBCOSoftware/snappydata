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
package org.apache.spark.sql

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.{Map => SMap}

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.sources.JdbcExtendedUtils

/**
 * Implicit class to easily invoke JDBC provider on SparkSession and avoid double query
 * execution of pushdown queries (one for schema determination and other the actual query).
 *
 * Instead of: spark.read.jdbc(jdbcUrl, "(pushdown query) q1", properties) one can simply do
 * spark.snappyQuery(query). This will also register dialects that avoid double execution,
 * use proper JDBC driver argument to avoid ClassNotFound errors. In addition this
 * provides "snappyExecute" implicits for non-query executions that will return an update count.
 */
case class JdbcExecute(session: SparkSession) extends DataFrameReader(session) {

  SnappyDataPoolDialect.register()
  SparkSession.setActiveSession(session)

  private def getURL(host: String, port: Int): String = port match {
    case _ if port > 0 => s"${Constant.POOLED_THIN_CLIENT_URL}$host[$port]"
    case _ => JdbcExtendedUtils.defaultPoolURL(session)
  }

  def snappyQuery(sql: String): DataFrame =
    snappyQuery(sql, host = null, port = -1, Map.empty[String, String], initCredentials = true)

  def snappyQuery(sql: String, host: String, port: Int): DataFrame =
    snappyQuery(sql, host, port, Map.empty[String, String], initCredentials = true)

  def snappyQuery(sql: String, user: String, password: String): DataFrame =
    snappyQuery(sql, host = null, port = -1, user, password)

  def snappyQuery(sql: String, host: String, port: Int,
      user: String, password: String): DataFrame = {
    snappyQuery(sql, host, port, Map(Attribute.USERNAME_ATTR -> user,
      Attribute.PASSWORD_ATTR -> password), initCredentials = false)
  }

  def snappyQuery(sql: String, host: String, port: Int, properties: Properties): DataFrame =
    snappyQuery(sql, host, port, properties.asScala, initCredentials = true)

  def snappyQuery(sql: String, host: String, port: Int,
      properties: scala.collection.Map[String, String]): DataFrame = {
    snappyQuery(sql, host, port, properties, initCredentials = true)
  }

  private def snappyQuery(sql: String, host: String, port: Int,
      properties: SMap[String, String], initCredentials: Boolean): DataFrame = {
    option(JDBCOptions.JDBC_URL, getURL(host, port))
    option(JDBCOptions.JDBC_DRIVER_CLASS, Constant.JDBC_CLIENT_POOL_DRIVER)
    option(JDBCOptions.JDBC_TABLE_NAME, s"($sql) queryTable")
    val props = if (initCredentials) {
      JdbcExtendedUtils.fillUserPassword(properties, session)
    } else properties
    if (props.nonEmpty) options(props)
    format("jdbc").load()
  }

  // execute to return an update count

  def snappyExecute(sql: String): Int =
    snappyExecute(sql, host = null, port = -1, Map.empty[String, String], initCredentials = true)

  def snappyExecute(sql: String, host: String, port: Int): Int =
    snappyExecute(sql, host, port, Map.empty[String, String], initCredentials = true)

  def snappyExecute(sql: String, user: String, password: String): Int =
    snappyExecute(sql, host = null, port = -1, user, password)

  def snappyExecute(sql: String, host: String, port: Int,
      user: String, password: String): Int = {
    snappyExecute(sql, host, port, Map(Attribute.USERNAME_ATTR -> user,
      Attribute.PASSWORD_ATTR -> password), initCredentials = false)
  }

  def snappyExecute(sql: String, host: String, port: Int, properties: Properties): Int =
    snappyExecute(sql, host, port, properties.asScala, initCredentials = true)

  def snappyExecute(sql: String, host: String, port: Int,
      properties: scala.collection.Map[String, String]): Int = {
    snappyExecute(sql, host, port, properties, initCredentials = true)
  }

  private def snappyExecute(sql: String, host: String, port: Int,
      properties: SMap[String, String], initCredentials: Boolean): Int = {
    val opts = Map(
      JDBCOptions.JDBC_URL -> getURL(host, port),
      JDBCOptions.JDBC_DRIVER_CLASS -> Constant.JDBC_CLIENT_POOL_DRIVER,
      // dummy table name which is not used but required by Spark's JDBCOptions
      JDBCOptions.JDBC_TABLE_NAME -> JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME) ++ (
        if (initCredentials) JdbcExtendedUtils.fillUserPassword(properties, session)
        else properties)
    val options = new JDBCOptions(opts)
    val connFactory = JdbcUtils.createConnectionFactory(options)
    val conn = connFactory()
    try {
      val stmt = conn.createStatement()
      val result = stmt.executeUpdate(sql)
      stmt.close()
      result
    } finally {
      conn.close()
    }
  }
}
