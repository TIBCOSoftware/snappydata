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
package io.snappydata.hydra.jdbcPooledDriver

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyContext, SparkSession}

object JdbcPooledDriverSparkApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    System.out.println("SP:Inside JdbcPooledDriverSparkApp")
    val connectionURL = args(args.length - 1)
    System.out.println("The connection url is " + connectionURL)
    Thread.sleep(60000L)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    val pw = new PrintWriter(new FileOutputStream(new File("JdbcPooledDriverSparkApp.out"),
      true))
    val DRIVER_NAME = "io.snappydata.jdbc.ClientPoolDriver"
    val props = new java.util.Properties
    props.setProperty("pool-driverClassName", DRIVER_NAME)
    props.setProperty("driver", DRIVER_NAME)
    val url = "jdbc:snappydata:pool://" + connectionURL
    val tableDF = sparkSession.sqlContext.read.jdbc(url, "employees", props)
    tableDF.show()
    pw.close()
  }
}
