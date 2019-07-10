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

package io.snappydata.hydra.consistency

import java.io.PrintWriter
import java.sql.SQLException
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}

import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil.DMLOp

import org.apache.spark.sql.{DataFrame, Row, SnappyContext}


class ConsistencyTest {
  val barrier = new CyclicBarrier(2)

  // scalastyle:off println
  def performOpsAndVerifyConsistency(snc: SnappyContext, pw: PrintWriter, tid: Int, dmlOp: String,
      batchSize: Int, dmlStmt: String, selectStmt: String, tableName: String) : Unit = {
    val pool = Executors.newFixedThreadPool(2)
    pool.execute(new DMLOpsThread(snc, pw, tid, dmlStmt))
    pool.execute(new SelectOpsThread(snc, pw, tid, selectStmt, dmlOp, tableName, batchSize))
    pool.shutdown()
    try
      pool.awaitTermination(2400, TimeUnit.SECONDS)
    catch {
      case ie: InterruptedException =>
        pw.println("Got Exception while waiting for all threads to complete the tasks")
    }
  }

  // scalastyle:off println
  class DMLOpsThread(snc: SnappyContext, pw: PrintWriter, tid: Int, stmt: String) extends Runnable {
    override def run(): Unit = {
      pw.println(s"Executing dml statement $stmt")
      waitForBarrier(tid + "", 2, pw)
      snc.sql(stmt)
    }
  }

  // scalastyle:off println
  class SelectOpsThread(snc: SnappyContext, pw: PrintWriter, tid: Int, stmt: String, op: String,
      tableName: String, batchSize: Int) extends
  Runnable {
    override def run(): Unit = {
      try {
        pw.println("Executing query :" + stmt)
        val beforeDF = snc.sql(stmt)
        waitForBarrier(tid + "", 2, pw)
        val afterDF = snc.sql(stmt)
        pw.println("Verifying the results for atomicity..")
        verifyAtomicity(beforeDF, afterDF, op, tableName, batchSize, pw)
      } catch {
        case se: SQLException =>
          pw.println("Got exception while executing select query", se)
      }
    }
  }

  // scalastyle:off println
  def verifyAtomicity(df_before: DataFrame, df_after: DataFrame, op: String, tableName: String,
      batchSize: Int, pw: PrintWriter) : Boolean = {
    var atomicityCheckFailed: Boolean = false
    var rowCount = 0
    var defaultValue: Int = 0
    val schema = df_before.schema.fieldNames
    val dfBef_list = df_before.collectAsList()
    val dfAft_list = df_after.collectAsList()
    for(i <- 0 until dfBef_list.size()) {
      val rowBf: Row = dfBef_list.get(i)
      val rowAf: Row = dfAft_list.get(i)
      for (j <- 0 until schema.length) {
        val colName: String = schema(i)
        val before_result: Int = rowBf.getInt(i)
        val after_result: Int = rowAf.getInt(i)
        DMLOp.getOperation(op) match {
          case DMLOp.INSERT =>
            defaultValue = -1
            if (colName.toUpperCase.startsWith("COUNT")) {
              rowCount = before_result
              pw.println("Number of rows in table " + tableName + " before " + op + " start: " +
                  before_result + " and number of after " + op + " start : " + after_result)
              val expectedRs = after_result - before_result
              if (!(expectedRs == 0 || expectedRs == batchSize)) atomicityCheckFailed = true
            }
            else if (colName.toUpperCase.startsWith("AVG")) {
              pw.println("Avg of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and avg after " + op + " start : " + after_result)
              val expectedRs =
                ((before_result * rowCount) + (defaultValue * batchSize)) / (rowCount + batchSize)
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
            else if (colName.toUpperCase.startsWith("SUM")) {
              pw.println("Sum of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and sum after " + op + " start : " + after_result)
              val expectedRs = before_result + (defaultValue * batchSize)
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
          case DMLOp.UPDATE =>
            defaultValue = 1
            if (colName.toUpperCase.startsWith("COUNT")) {
              rowCount = before_result
              pw.println("Number of rows in table " + tableName + " before " + op + " start: " +
                  before_result + " and number of after " + op + " start : " + after_result)
              val expectedRs = after_result - before_result
              if (expectedRs == 0) atomicityCheckFailed = true
            }
            else if (colName.toUpperCase.startsWith("AVG")) {
              pw.println("Avg of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and avg after " + op + " start : " + after_result)
              val expectedRs = before_result + defaultValue
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
            else if (colName.toUpperCase.startsWith("SUM")) {
              pw.println("Sum of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and sum after " + op + " start : " + after_result)
              val expectedRs = before_result + (defaultValue * rowCount)
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
          case DMLOp.DELETE =>
            defaultValue = 0
            if (colName.toUpperCase.startsWith("COUNT")) {
              rowCount = before_result
              pw.println("Number of rows in table " + tableName + " before " + op + " start: " +
                  before_result + " and number of after " + op + " start : " + after_result)
              val expectedRs = after_result - before_result
              if (!(expectedRs % batchSize == 0)) atomicityCheckFailed = true
            }
            else if (colName.toUpperCase.startsWith("AVG")) {
              pw.println("Avg of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and avg after " + op + " start : " + after_result)
              // TODO
            }
            else if (colName.toUpperCase.startsWith("SUM")) {
              pw.println("Sum of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and sum after " + op + " start : " + after_result)
            }
          case DMLOp.PUTINTO =>
            defaultValue = -1
            if (colName.toUpperCase.startsWith("COUNT")) {
              rowCount = before_result
              pw.println("Number of rows in table " + tableName + " before " + op + " start: " +
                  before_result + " and number of after " + op + " start : " + after_result)
              val expectedRs = after_result - before_result
              if (!(expectedRs % batchSize == 0)) atomicityCheckFailed = true
            }
            else if (colName.toUpperCase.startsWith("AVG")) {
              pw.println("Avg of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and avg after " + op + " start : " + after_result)
              val expectedRs =
                ((before_result * rowCount) + (defaultValue * batchSize)) / (rowCount + batchSize)
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
            else if (colName.toUpperCase.startsWith("SUM")) {
              pw.println("Sum of column in table " + tableName + " before " + op + " start: " +
                  before_result + " and sum after " + op + " start : " + after_result)
              val expectedRs = before_result + (defaultValue * batchSize)
              if (!(after_result == before_result || after_result == expectedRs)) {
                atomicityCheckFailed = true
              }
            }
          case _ =>
        }
      }
    }
    atomicityCheckFailed
  }

  // scalastyle:off println
  protected def waitForBarrier(barrierName: String, numThreads: Int, pw: PrintWriter): Unit = {
    pw.println("Waiting for " + numThreads + " to meet at barrier " + barrierName)
    barrier.await()
    pw.println("Wait Completed for barrier " + barrierName)
  }
}
