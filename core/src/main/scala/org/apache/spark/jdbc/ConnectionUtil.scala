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
package org.apache.spark.jdbc

import scala.collection.JavaConverters._

import java.sql.Connection

import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.{SparkContext, SparkEnv}


object ConnectionUtil {

  /**
   * Gives a connection from underlying ConnectionPool with the given name.
   * If no such pool exists it creates a new pool and return a connection.
   * @param name name of the pool
   * @param conf configuration of the pool
   * @return a java.sql.Connection
   */
  def getPooledConnection(name: String, conf: ConnectionConf): Connection = {
    val connectionProps = conf.connProps

    val connProps = SparkEnv.get.executorId match {
      case SparkContext.DRIVER_IDENTIFIER => connectionProps.connProps
      case _ => connectionProps.executorConnProps
    }
    val hikariCP = connectionProps.hikariCP
    val poolProps = connectionProps.poolProps
    val dDialect = connectionProps.dialect

    ConnectionPool.getPoolConnection(name,
      dDialect, poolProps, connProps, hikariCP)
  }

  /**
   * Returns a non pooled connection. Maintaing the connection and closing it gracefully is the
   * task of the user of the API.
   * @param conf Configuration of the connection
   * @return a java.sql.Connection
   */
  def getConnection(conf: ConnectionConf): Connection = {
    val connectionProps = conf.connProps

    val connProps = SparkEnv.get.executorId match {
      case SparkContext.DRIVER_IDENTIFIER => connectionProps.connProps
      case _ => connectionProps.executorConnProps
    }
    val jdbcOptions = new JDBCOptions(connectionProps.url, "", connProps.asScala.toMap)
    JdbcUtils.createConnectionFactory(jdbcOptions)()
  }

}
