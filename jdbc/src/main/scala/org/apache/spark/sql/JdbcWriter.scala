/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import io.snappydata.Constant

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.JdbcExtendedUtils

/**
 * Implicit class to easily invoke DataFrameWriter operations on SnappyData's JDBC provider.
 *
 * Instead of: spark.write.jdbc(url, table, properties) one can simply do
 * spark.write.snappy(table). This will also register dialects for proper type conversions,
 * use proper JDBC driver argument to avoid ClassNotFound errors.
 *
 * In future this will also provide spark.write.snappyPut(table) to perform a PUT INTO.
 */
class JdbcWriter(writer: DataFrameWriter[Row]) {

  private val df = JdbcExtendedUtils.writerDfField.get(writer).asInstanceOf[DataFrame]

  SnappyDataPoolDialect.register()

  def snappy(table: String): Unit = snappy(table, Map.empty)

  def snappy(table: String, connectionProperties: Map[String, String]): Unit =
    snappy(table, connectionProperties, SaveMode.Append)

  def snappy(table: String, connectionProperties: Map[String, String], mode: SaveMode): Unit = {
    val session = df.sparkSession
    val connProps = new Properties()
    connProps.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, Constant.JDBC_CLIENT_POOL_DRIVER)
    val allProps = JdbcExtendedUtils.fillUserPassword(connectionProperties, session)
    if (allProps.nonEmpty) {
      allProps.foreach(p => connProps.setProperty(p._1, p._2))
    }
    writer.mode(mode).jdbc(JdbcExtendedUtils.defaultPoolURL(session), table, connProps)
  }
}
