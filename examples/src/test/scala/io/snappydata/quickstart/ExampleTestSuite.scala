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
package io.snappydata.quickstart

import io.snappydata.SnappyTestRunner

/**
 * Extending SnappyTestRunner. This class tests the old quickstart as well as
 * the examples enumerated in Snappy examples folder
 */
class ExampleTestSuite extends SnappyTestRunner {

  def quickStartJar: String = s"$snappyHome/examples/jars/quickstart.jar"

  val localLead = "localhost:8090"
  val snappyExamples = "org.apache.spark.examples.snappydata"

  test("old quickstart") {

    SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_column_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_row_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_sample_table.sql';",
      s"run '$snappyHome/quickstart/scripts/status_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/oltp_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_approx_queries.sql';",
      "exit;"))


    Job("io.snappydata.examples.AirlineDataJob", localLead, quickStartJar)

    Job("io.snappydata.examples.CreateAndLoadAirlineDataJob", localLead, quickStartJar)

    SparkSubmit("AirlineDataApp", appClass = "io.snappydata.examples.AirlineDataSparkApp", None,
      confs = Seq("snappydata.connection=localhost:1527", "spark.ui.port=4051"),
      appJar = quickStartJar)

    SparkSubmit("PythonAirlineDataApp", appClass = "", None,
      confs = Seq("snappydata.connection=localhost:1527", "spark.ui.port=4051"),
      appJar = s"$snappyHome/quickstart/python/AirlineDataPythonApp.py")

  }

  test("Create Table in Python") {
    SparkSubmit("CreateTable", appClass = "", None,
      confs = Seq.empty[String],
      appJar = s"$snappyHome/quickstart/python/CreateTable.py")
  }

  test("KMeans in Python") {
    SparkSubmit("KMeansWeather", appClass = "", None,
      confs = Seq.empty[String],
      appJar = s"$snappyHome/quickstart/python/KMeansWeather.py")
  }

  test("QuickStart.scala script") {
    SparkShell("spark.sql.catalogImplementation=in-memory" :: Nil,
      "--driver-memory=4g --driver-java-options=" +
      "\"-XX:+UseConcMarkSweepGC\" \"-XX:+UseParNewGC\" \"-XX:+CMSClassUnloadingEnabled\"" +
      " \"-XX:MaxNewSize=1g\"",
      scriptFile = s"$snappyHome/quickstart/scripts/Quickstart.scala")
  }

  test("JDBCExample") {
    RunExample("JDBCExample", "snappydata.JDBCExample")
  }

  test("JDBCWithComplexTypes") {
    RunExample("JDBCWithComplexTypes", "snappydata.JDBCWithComplexTypes")
  }

  test("CollocatedJoinExample") {
    Job(s"$snappyExamples.CollocatedJoinExample",
      localLead, quickStartJar)
  }

  test("CreateColumnTable") {
    Job(s"$snappyExamples.CreateColumnTable", localLead, quickStartJar,
      Seq(s"data_resource_folder=$snappyHome/quickstart/src/main/resources"))
  }

  test("CreatePartitionedRowTable") {
    Job(s"$snappyExamples.CreatePartitionedRowTable",
      localLead, quickStartJar)
  }

  test("CreateReplicatedRowTable") {
    Job(s"$snappyExamples.CreateReplicatedRowTable",
      localLead, quickStartJar)
  }

  test("WorkingWithObjects") {
    Job(s"$snappyExamples.WorkingWithObjects",
      localLead, quickStartJar)
  }

  test("WorkingWithJson") {
    Job(s"$snappyExamples.WorkingWithJson",
      localLead, quickStartJar,
      Seq(s"json_resource_folder=$snappyHome/quickstart/src/main/resources"))
  }

  test("WorkingWithJson With main") {
    RunExample("WorkingWithJson_main", "snappydata.WorkingWithJson",
      Seq(s"$snappyHome/quickstart/src/main/resources"))
  }

  test("SmartConnectorExample") {
    SnappyShell("smartConnectorSetup", Seq("connect client 'localhost:1527';",
      s"CREATE TABLE SNAPPY_COL_TABLE(r1 Integer, r2 Integer) USING COLUMN;",
      s"insert into SNAPPY_COL_TABLE VALUES(1,1);",
      s"insert into SNAPPY_COL_TABLE VALUES(2,2);",
      "exit;"))

    RunExample("SmartConnectorExample",
      "snappydata.SmartConnectorExample",
      Seq("spark.snappydata.connection=localhost:1527"))
  }

  test("StreamingExample") {
    RunExample("StreamingExample", "snappydata.StreamingExample")

  }

  test("SynopsisDataExample") {
    Job(s"$snappyExamples.SynopsisDataExample",
      localLead, quickStartJar,
      Seq(s"data_resource_folder=$snappyHome/quickstart/data"))
  }

}
