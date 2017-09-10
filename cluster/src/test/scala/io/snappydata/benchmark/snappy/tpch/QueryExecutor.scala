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

package io.snappydata.benchmark.snappy.tpch

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{PreparedStatement, ResultSet}

import io.snappydata.benchmark.TPCH_Queries

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by kishor on 27/10/15.
  */
object QueryExecutor {

  var planFileStream: FileOutputStream = _
  var planPrintStream: PrintStream = _

  def close: Unit = if (planFileStream != null) {
    planPrintStream.close
    planFileStream.close()
  }

  def execute_statement(queryNumber: String, isResultCollection: Boolean, stmt: PreparedStatement,
      warmup: Integer, runsForAverage: Integer, avgPrintStream: PrintStream = null): Unit = {

    var queryFileStream = new FileOutputStream(new File(s"$queryNumber.out"))
    var queryPrintStream = new PrintStream(queryFileStream)

    var rs: ResultSet = null
    try {
      // scalastyle:off println
      println(s"Started executing $queryNumber")
      if (isResultCollection) {
        rs = queryExecution(queryNumber, stmt)
        // queryPrintStream.println(s"$resultFormat")
        val rsmd = rs.getMetaData()
        val columnsNumber = rsmd.getColumnCount();
        var count: Int = 0
        while (rs.next()) {
          count += 1
          for (i <- 1 to columnsNumber) {
            if (i > 1) queryPrintStream.print(",")
            queryPrintStream.print(rs.getString(i))
          }
          queryPrintStream.println()
        }
        println(s"NUmber of results : $count")
        println(s"$queryNumber Result Collected in file $queryNumber.out")
        if (queryNumber.equals("q13")) {
          stmt.execute("drop view ViewQ13")
        }
        if (queryNumber.equals("q15")) {
          stmt.execute("drop view revenue")
        }
      } else {
        var totalTime: Long = 0
        for (i <- 1 to (warmup + runsForAverage)) {
          val startTime = System.currentTimeMillis()
          rs = queryExecution(queryNumber, stmt)
          // rs = stmt.executeQuery(query)
          while (rs.next()) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime
          queryPrintStream.println(s"$iterationTime")
          if (i > warmup) {
            totalTime += iterationTime
          }
          if (queryNumber.equals("q13")) {
            stmt.execute("drop view ViewQ13")
          }
          if (queryNumber.equals("q15")) {
            stmt.execute("drop view revenue")
          }
        }
        queryPrintStream.println(s"${totalTime / runsForAverage}")
        avgPrintStream.println(s"$queryNumber,${totalTime / runsForAverage}")
      }
      println(s"Finished executing $queryNumber")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        e.printStackTrace(queryPrintStream)
        e.printStackTrace(avgPrintStream)
        println(s" Exception while executing $queryNumber in written to file $queryNumber.txt")
      }
    } finally {
      if (isResultCollection) {
        queryPrintStream.close()
        queryFileStream.close()
      }

    }
    rs.close()
  }


  def execute(queryNumber: String, sqlContext: SQLContext, isResultCollection: Boolean,
      isSnappy: Boolean, threadNumber: Int = 1, isDynamic: Boolean = false, warmup: Int = 0,
      runsForAverage: Int = 1, avgPrintStream: PrintStream = null): Unit = {

    val planFileName = if (isSnappy) s"${threadNumber}_Plan_Snappy.out"
            else s"${threadNumber}_Plan_Spark.out"
    val queryFileName = if (isSnappy) s"${threadNumber}_Snappy_$queryNumber.out"
            else s"${threadNumber}_Spark_$queryNumber.out"

    if (planFileStream == null && planPrintStream == null) {
      planFileStream = new FileOutputStream(new File(planFileName))
      planPrintStream = new PrintStream(planFileStream)
    }

    val queryFileStream: FileOutputStream = new FileOutputStream(new File(queryFileName))
    val queryPrintStream: PrintStream = new PrintStream(queryFileStream)

    // scalastyle:off println
    try {
      println(s"Started executing $queryNumber")

      if (isResultCollection) {
        var queryToBeExecuted = TPCH_Queries.getQuery(queryNumber, isDynamic, true)
        // queryPrintStream.println(queryToBeExecuted)
        val (resultSet, _) = queryExecution(queryNumber, queryToBeExecuted, sqlContext, true)
        println(s"$queryNumber : ${resultSet.length}")

        for (row <- resultSet) {
          queryPrintStream.println(row.toSeq.map {
            case d: Double => "%18.4f".format(d).trim()
            case v => v
          }.mkString(","))
        }
        println(s"$queryNumber Result Collected in file $queryFileName")
      } else {
        var totalTime: Long = 0
        queryPrintStream.println(queryNumber)
        for (i <- 1 to (warmup + runsForAverage)) {
          var queryToBeExecuted = TPCH_Queries.getQuery(queryNumber, isDynamic, true)
          // queryPrintStream.println(queryToBeExecuted)
          val startTime = System.currentTimeMillis()
          var cnts: Array[Row] = null
          if (i == 1) {
            cnts = queryExecution(queryNumber, queryToBeExecuted, sqlContext, true)._1
          } else {
            cnts = queryExecution(queryNumber, queryToBeExecuted, sqlContext)._1
          }
          for (s <- cnts) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime
          queryPrintStream.println(s"$iterationTime")
          if (i > warmup) {
            totalTime += iterationTime
          }
          cnts = null
        }
        queryPrintStream.println(s"${totalTime / runsForAverage}")
        avgPrintStream.println(s"$queryNumber,${totalTime / runsForAverage}")
      }
      println(s"Finished executing $queryNumber")
    } catch {
      case e: Exception => {
        e.printStackTrace(queryPrintStream)
        e.printStackTrace(avgPrintStream)
        println(s" Exception while executing $queryNumber in written to file $queryFileName")
      }
    } finally {
      queryPrintStream.close()
      queryFileStream.close()
    }
    // scalastyle:on println
  }

  def printPlan(df: DataFrame, query: String): Unit = {
    // scalastyle:off println
    if (planPrintStream != null) {
      planPrintStream.println(query)
      planPrintStream.println(df.queryExecution.executedPlan)
    } else {
      df.explain(true)
    }
    // scalastyle:on println
  }

  def queryExecution(queryNumber: String, prepStatement: PreparedStatement): ResultSet = {
    val rs: ResultSet = queryNumber match {
      case "15" =>
        prepStatement.execute(TPCH_Queries.getTempQuery15_Original())
        prepStatement.executeQuery(TPCH_Queries.getQuery15_Original())
      case _ =>
        prepStatement.executeQuery()
    }
    rs
  }

  def queryExecution(queryNumber: String, query: String, sqlContext: SQLContext,
      genPlan: Boolean = false): (scala.Array[org.apache.spark.sql.Row], DataFrame) = {
    var queryToBeExecuted = query
    if (queryNumber.equals("15")) {
      val result = sqlContext.sql(queryToBeExecuted)
      // val result = sqlContext.sql(getTempQuery15_1)
      result.createOrReplaceTempView("revenue")
      queryToBeExecuted = TPCH_Queries.getQuery15
    }
    val df = sqlContext.sql(queryToBeExecuted)
    if (genPlan) {
      printPlan(df, queryNumber)
    }
    (df.collect(), df)
  }

}
