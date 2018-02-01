/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, DriverManager}

import org.apache.spark.Partition
import org.apache.spark.jdbc.{ConnectionConf, ConnectionConfBuilder, ConnectionUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_DRIVER_CLASS
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SnappySession}

/**
  * This object is used to get an optimized JDBC RDD, which uses pooled connections.
  */
object JDBCRelationUtil {

  def jdbcDF(sparkSession: SnappySession,
      parameters: Map[String, String],
      table: String,
      schema: StructType,
      requiredColumns: Array[String],
      parts: Array[Partition],
      conf : ConnectionConf,
      filters: Array[Filter]): Dataset[Row] = {
    val url = parameters("url")
    val options = new JDBCOptions(url, table, parameters)

    val dialect = JdbcDialects.get(url)
    val quotedColumns = requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    val rdd = new JDBCRDD(
      sparkSession.sparkContext,
      createConnectionFactory(url, conf),
      schema,
      quotedColumns,
      filters,
      parts,
      url,
      options).asInstanceOf[RDD[InternalRow]]

    sparkSession.internalCreateDataFrame(rdd, schema)
  }

  def createConnectionFactory(url: String, conf: ConnectionConf): () => Connection = {
    () => {
      ConnectionUtil.getPooledConnection(url, conf)
    }
  }

  def buildConf(snappySession: SnappySession,
      parameters: Map[String, String]): ConnectionConf = {
    val url = parameters("url")
    val driverClass = {
      val userSpecifiedDriverClass = parameters.get(JDBC_DRIVER_CLASS)
      userSpecifiedDriverClass.foreach(DriverRegistry.register)

      // Performing this part of the logic on the driver guards against the corner-case where the
      // driver returned for a URL is different on the driver and executors due to classpath
      // differences.
      userSpecifiedDriverClass.getOrElse {
        DriverManager.getDriver(url).getClass.getCanonicalName
      }
    }
    new ConnectionConfBuilder(snappySession)
        .setPoolProvider(parameters.getOrElse("poolImpl", "hikari"))
        .setPoolConf("maximumPoolSize", parameters.getOrElse("maximumPoolSize", "10"))
        .setPoolConf("minimumIdle", parameters.getOrElse("minimumIdle", "5"))
        .setDriver(driverClass)
        .setConf("user", parameters.getOrElse("user", ""))
        .setConf("password", parameters.getOrElse("password", ""))
        .setURL(url)
        .build()
  }

  def schema(table: String, parameters: Map[String, String]): StructType = {
    val url = parameters("url")
    val options = new JDBCOptions(url, table, parameters)
    JDBCRDD.resolveTable(options)
  }
}