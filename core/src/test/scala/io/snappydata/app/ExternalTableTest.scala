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
package io.snappydata.app

import scala.util.Random

import org.apache.spark.sql._

object ExternalTableTest extends App {
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"
  var externalUrl: String = "jdbc:gemfirexd://localhost:1527"

  if (args.length > 0) {
    option(args.toList)
  }

  val conf = new org.apache.spark.SparkConf().setAppName("ExternalTableTest")
      .set("spark.logConf", "true")
  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    // Intellij compile output path e.g. /wrk/out-snappy/production/MyTests/
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (debug) {
    val agentOpts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    if (executorExtraJavaOptions != null) {
      executorExtraJavaOptions = executorExtraJavaOptions + " " + agentOpts
    } else {
      executorExtraJavaOptions = agentOpts
    }
  }

  if (executorExtraJavaOptions != null) {
    conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
  }

  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = SnappyContext(sc)

  val ddlStr = "YearI INT NOT NULL," +
      "MonthI INT NOT NULL," +
      "DayOfMonth INT NOT NULL," +
      "DepTime INT," +
      "ArrTime INT," +
      "UniqueCarrier CHAR(6) NOT NULL"

  if (new Random().nextBoolean()) {
    snContext.sql("drop table if exists airline")
    snContext.sql(s"create table airline ($ddlStr) " +
        s" using jdbc options (URL '$externalUrl'," +
        "  Driver 'io.snappydata.jdbc.ClientDriver')").collect()
  } else {
    snContext.sql(s"create table if not exists airline ($ddlStr) " +
        s" using jdbc options (URL '$externalUrl'," +
        "  Driver 'com.pivotal.gemfirexd.jdbc.ClientDriver')").collect()
  }

  snContext.sql("insert into airline values(2015, 2, 15, 1002, 1803, 'AA')")
  snContext.sql("insert into airline values(2014, 4, 15, 1324, 1500, 'UT')")

  snContext.sql("select * from airline").show()

  def option(list: List[String]): Boolean = {
    list match {
      case "-set-master" :: value :: tail =>
        setMaster = value
        print(" setMaster " + setMaster)
        option(tail)
      case "-nomaster" :: tail =>
        setMaster = null
        print(" setMaster " + setMaster)
        option(tail)
      case "-set-jars" :: value :: tail =>
        setJars = value
        print(" setJars " + setJars)
        option(tail)
      case "-executor-extraClassPath" :: value :: tail =>
        executorExtraClassPath = value
        print(" executor-extraClassPath " + executorExtraClassPath)
        option(tail)
      case "-executor-extraJavaOptions" :: value :: tail =>
        executorExtraJavaOptions = value
        print(" executor-extraJavaOptions " + executorExtraJavaOptions)
        option(tail)
      case "-debug" :: tail =>
        debug = true
        print(" debug " + debug)
        option(tail)
      case "-external-url" :: value :: tail =>
        externalUrl = value
        print(" external-url " + externalUrl)
        option(tail)
      case opt :: tail =>
        println(" Unknown option " + opt)
        sys.exit(1)
      case Nil => true
    }
  } // end of option
}
