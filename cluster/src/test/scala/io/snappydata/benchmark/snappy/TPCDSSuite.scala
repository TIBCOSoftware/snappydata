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

package io.snappydata.benchmark.snappy

import java.io.{File, FileOutputStream, PrintStream}

import io.snappydata.SnappyFunSuite
import org.apache.spark.sql.execution.benchmark.TPCDSQuerySnappyBenchmark
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll


class TPCDSSuite extends SnappyFunSuite
    with BeforeAndAfterAll {

  var tpcdsQueries = Seq[String]()
  var runTPCDSSuite = ""


  val conf =
    new SparkConf()
        .setMaster("local[*]")
        .setAppName("test-sql-context")
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.driver.memory", "1g")
        .set("spark.executor.memory", "1g")
        .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tpcdsQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")
    runTPCDSSuite = System.getenv("TPCDS_SUITE")
    if (runTPCDSSuite == null) {
      println("TPCDS_SUITE should be set as an environment variable in order to run TPCDSSuite")
    }
  }

  // Disabling the test run from precheckin as it takes around an hour.
  // TODO : Add TPCDS tests to be run as a part of smokePerf bt which will run on a dedicated
  // machine.

  test("Test with Snappy") {
    if (runTPCDSSuite.equalsIgnoreCase("true")) {
      val sc = new SparkContext(conf)
      TPCDSQuerySnappyBenchmark.snappy = new SnappySession(sc)
      val dataLocation = "/export/shared/QA_DATA/TPCDS/data"
      val snappyHome = System.getenv("SNAPPY_HOME")
      val snappyRepo = s"$snappyHome/../../.."

      TPCDSQuerySnappyBenchmark.execute(dataLocation,
        queries = tpcdsQueries, true, s"$snappyRepo/spark/sql/core/src/test/resources/tpcds")
    }
  }

  // Disabling the test run from precheckin as it takes around an hour.
  // TODO : Add TPCDS tests to be run as a part of smokePerf bt which will run on a dedicated
  // machine.

  test("Test with Spark") {
    if (runTPCDSSuite.equalsIgnoreCase("true")) {
      TPCDSQuerySnappyBenchmark.spark = SparkSession.builder.config(conf).getOrCreate()
      val dataLocation = "/export/shared/QA_DATA/TPCDS/data"
      val snappyHome = System.getenv("SNAPPY_HOME")
      val snappyRepo = s"$snappyHome/../../..";

      TPCDSQuerySnappyBenchmark.execute(dataLocation,
        queries = tpcdsQueries, false, s"$snappyRepo/spark/sql/core/src/test/resources/tpcds")

    }
  }

  // Disabling the validation for now as this requires the expected result files to be created
  // using stock spark before hand.

  ignore("Validate Results") {

    for (query <- tpcdsQueries) {

      val actualResultsAvailableAt = "path for actual result"
      val expectedResultsAvailableAt = "path for expected result"

      val resultFileStream: FileOutputStream = new FileOutputStream(new File("Comparison.out"))
      val resultOutputStream: PrintStream = new PrintStream(resultFileStream)

      val expectedFile = sc.textFile(s"file://$expectedResultsAvailableAt/Spark_$query.out")
      val actualFile = sc.textFile(s"file://$actualResultsAvailableAt/Snappy_$query.out")

      val expectedLineSet = expectedFile.collect().toList.sorted
      val actualLineSet = actualFile.collect().toList.sorted

      if (!actualLineSet.equals(expectedLineSet)) {
        if (!(expectedLineSet.size == actualLineSet.size)) {
          resultOutputStream.println(s"For $query " +
              s"result count mismatched observed with " +
              s"expected ${expectedLineSet.size} and actual ${actualLineSet.size}")
        } else {
          for ((expectedLine, actualLine) <- expectedLineSet zip actualLineSet) {
            if (!expectedLine.equals(actualLine)) {
              resultOutputStream.println(s"For $query result mismatched observed")
              resultOutputStream.println(s"Expected  : $expectedLine")
              resultOutputStream.println(s"Found     : $actualLine")
              resultOutputStream.println(s"-------------------------------------")
            }
          }
        }
      }
    }
  }
}
