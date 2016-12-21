/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.examples



sealed trait Example {
  def name: String
}

//Snappy job should use this
case class Job(override val name: String, jobClass: String) extends Example

//Examples which should be run by ./bin/run-example should use this
case class RunExample(override val name: String, appClass: String) extends Example

//Simple applications with main method
case class Application(override val name: String, appClass: String) extends Example

//Spark-submit
case class SparkSubmit(override val name: String, appClass: String, confs: Seq[String], appJar: String) extends Example

case class SnappyShell(override val name: String, sqlCommand: Seq[String]) extends Example


object ExampleConf {

  def oldQuickStart(snappyHome: String): Seq[Example] = Seq(

    SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_column_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_row_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_sample_table.sql';",
      s"run '$snappyHome/quickstart/scripts/status_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/oltp_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_approx_queries.sql';",
      "exit;")),

    Job("airlineDataJob",
      "io.snappydata.examples.AirlineDataJob"),

    Job("CreateAndLoadAirlineDataJob",
      "io.snappydata.examples.CreateAndLoadAirlineDataJob"),

    SparkSubmit("AirlineDataApp", appClass = "io.snappydata.examples.AirlineDataSparkApp",
      confs = Seq("snappydata.store.locators=localhost:10334", "spark.ui.port=4041"),
      appJar = s"$snappyHome/examples/jars/quickstart.jar"),

    SparkSubmit("PythonAirlineDataApp", appClass = "",
      confs = Seq("snappydata.store.locators=localhost:10334", "spark.ui.port=4041"),
      appJar = s"$snappyHome/quickstart/python/AirlineDataPythonApp.py")

  )

}
