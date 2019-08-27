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

import java.io._
import java.util.TimeZone

import io.snappydata.benchmark.TPCH_Queries
import io.snappydata.benchmark.snappy.tpch.QueryExecutor
import io.snappydata.benchmark.snappy.{SnappyAdapter, TPCH}

import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.util.Benchmark

object DistIndexTestUtils {

  def benchmark(qNum: String, tableSizes: Map[String, Long], snc: SnappyContext, pw: PrintWriter,
      fos: FileOutputStream): Unit
  = {

    val qryProvider = new TPCH with SnappyAdapter
    val query = qNum.toInt

    def executor(str: String) = snc.sql(str)

    val size = qryProvider.estimateSizes(query, tableSizes, executor)
    // scalastyle:off println
    pw.println(s"$qNum size $size")
    val b = new Benchmark(s"JoinOrder optimization", size, minNumIters = 5, output = Some(fos))

    def case1(): Unit = snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
      "false")

    def case2(): Unit = snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
      "true")

    def case3(): Unit = {
      snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
        "true")
    }

    val queryToBeExecuted = TPCH_Queries.getQuery(qNum, false, true)

    def evalSnappyMods(genPlan: Boolean) = QueryExecutor.queryExecution(
      qNum, queryToBeExecuted, snc)._1.foreach(_ => ())
//    def evalSnappyMods(genPlan: Boolean) = TPCH_Snappy.queryExecution(qNum, snc, useIndex = false,
//      genPlan = genPlan)._1.foreach(_ => ())

    def evalBaseTPCH = qryProvider.execute(query, executor)


    b.addCase(s"$qNum baseTPCH index = F", numIters = 0, prepare = case3, cleanup = () => {})(
      _ => evalBaseTPCH)
    //    b.addCase(s"$qNum baseTPCH joinOrder = T", prepare = case2)(i => evalBaseTPCH)
    //    b.addCase(s"$qNum snappyMods joinOrder = F", prepare = case1)(i => evalSnappyMods(false))
    //    b.addCase(s"$qNum snappyMods joinOrder = T", prepare = case2)(i => evalSnappyMods(false))
    b.addCase(s"$qNum baseTPCH index = T", numIters = 0, prepare = case3, cleanup = () => {})(_ =>
      evalBaseTPCH)
    b.run()
  }

  def executeQueriesWithResultValidation(snc: SnappyContext, pw: PrintWriter): Unit = {
    // scalastyle:off println
    val qryProvider = new TPCH with SnappyAdapter

    val queries = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
      "12", "13", "14", "15", "16", "17", "18", "19",
      "20", "21", "22")

    // TPCHUtils.createAndLoadTables(snc, true)

    val existing = snc.getConf(io.snappydata.Property.EnableExperimentalFeatures.name)
    snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name, "true")

    for ((q, i) <- queries.zipWithIndex) {
      val qNum = i + 1
      val (expectedAnswer, _) = qryProvider.execute(qNum, str => {
        pw.println("Query String is : " + str)
        snc.sql(str)
      })
      val queryToBeExecuted = TPCH_Queries.getQuery(qNum.toString, false, true)
      val (newAnswer, df) = QueryExecutor.queryExecution(qNum.toString,
        queryToBeExecuted, snc)
      // val (newAnswer, df) = TPCH_Snappy.queryExecution(q, snc, false, false)
      newAnswer.foreach(pw.println)
      val isSorted = df.logicalPlan.collect { case s: Sort => s }.nonEmpty
      QueryTest.sameRows(expectedAnswer, newAnswer, isSorted).map { results =>
        s"""
           |Results do not match for query: $qNum
           |Timezone: ${TimeZone.getDefault}
           |Timezone Env: ${sys.env.getOrElse("TZ", "")}
           |
           |${df.queryExecution}
           |== Results ==
           |$results
       """.stripMargin
      }
      pw.println(s"Done $qNum")
    }
    snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name, existing)
  }

  def executeQueriesForBenchmarkResults(snc: SnappyContext, pw: PrintWriter, fos:
  FileOutputStream): DataFrame = {

    val queries = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
      "12", "13", "14", "15", "16", "17", "18", "19",
      "20", "21", "22")

    // TPCHUtils.createAndLoadTables(snc, true)

    snc.sql(
      s"""CREATE INDEX idx_orders_cust ON orders(o_custkey)
             options (COLOCATE_WITH 'customer')
          """)

    snc.sql(
      s"""CREATE INDEX idx_lineitem_part ON lineitem(l_partkey)
             options (COLOCATE_WITH 'part')
          """)

    val tables = Seq("nation", "region", "supplier", "customer", "orders", "lineitem", "part",
      "partsupp")

    val tableSizes = tables.map { tableName =>
      (tableName, snc.table(tableName).count())
    }.toMap

    tableSizes.foreach(pw.println)
    queries.foreach(q => benchmark(q, tableSizes, snc, pw, fos))
    snc.sql(s"DROP INDEX idx_orders_cust")
    snc.sql(s"DROP INDEX idx_lineitem_part")
  }

}
