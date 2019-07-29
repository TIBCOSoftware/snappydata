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
import java.sql.{SQLException, Timestamp}
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}

import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil.DMLOp

import org.apache.spark.sql.{DataFrame, Row, SnappySession, SparkSession}


class ConsistencyTest {
  val barrier = new CyclicBarrier(2)

  def printTime: String = {
    "[" + new Timestamp(System.currentTimeMillis()).toString + "] "
  }

  def createSnappySession() : SnappySession = {
    val snappySpark = new SnappySession(SparkSession.builder().getOrCreate().sparkContext);
    snappySpark
  }

  // scalastyle:off println
  def performOpsAndVerifyConsistency(snc: SnappySession, pw: PrintWriter, tid: Int, dmlOp: String,
      batchSize: Int, dmlStmt: String, selectStmt: String, tableName: String) : Unit = {
    val pool = Executors.newFixedThreadPool(2)
    val snappySnDML: SnappySession = createSnappySession()
    val snappySnSelect: SnappySession = createSnappySession()
    pool.execute(new DMLOpsThread(snappySnDML, pw, tid, dmlStmt))
    pool.execute(new SelectOpsThread(snappySnSelect, pw, tid, selectStmt, dmlOp, tableName,
      batchSize))
    pool.shutdown()
    try
      pool.awaitTermination(120, TimeUnit.SECONDS)
    catch {
      case ie: InterruptedException =>
        pw.println(s"${printTime} Got Exception while waiting for threads to complete the tasks.")
        pw.flush()
    } finally {
      snappySnDML.close()
      snappySnSelect.close()
    }
    pw.println(s"${printTime} Done with the execution.")
    pw.flush()
  }

  // scalastyle:off println
  class DMLOpsThread(snc: SnappySession, pw: PrintWriter, tid: Int, stmt: String) extends Runnable {
    override def run(): Unit = {
      pw.println(s"${printTime} Executing dml statement $stmt")
      pw.flush()
      waitForBarrier(tid + "", 2, pw)
      snc.sql(stmt)
      pw.println(s"${printTime} Executed the dml statement")
      pw.flush()
    }
  }

  // scalastyle:off println
  class SelectOpsThread(snc: SnappySession, pw: PrintWriter, tid: Int, stmt: String, op: String,
      tableName: String, batchSize: Int) extends
  Runnable {
    override def run(): Unit = {
      try {
        pw.println("Executing query :" + stmt)
        pw.flush()
        val beforeDF = snc.sql(stmt)
        pw.println(beforeDF.collectAsList())
        waitForBarrier(tid + "", 2, pw)
        val afterDF = snc.sql(stmt)
        pw.println(afterDF.collectAsList())
        pw.println(s"${printTime} Verifying the results for atomicity..")
        pw.flush()
        if (!verifyAtomicity(beforeDF, afterDF, op, tableName, batchSize, pw)) {
          pw.println(s"${printTime} Verified that data is atomic")
        }
        else pw.println(s"${printTime} Failed to get atomic data for $op.")
        pw.flush()
      } catch {
        case se: SQLException =>
          pw.println(s"${printTime} Got exception while executing select query for $op", se)
          pw.flush()
      }
    }
  }

  // scalastyle:off println
  def verifyAtomicity(df_before: DataFrame, df_after: DataFrame, op: String, tableName: String,
      batchSize: Int, pw: PrintWriter) : Boolean = {
    var atomicityCheckFailed: Boolean = false
    var rowCount: Long = 0
    var defaultValue: Int = 0
    try {
      val schema: Array[String] = df_before.schema.fieldNames
      pw.println(schema.mkString(","))
      val dfBef_list = df_before.collectAsList()
      val dfAft_list = df_after.collectAsList()
      for (i <- 0 until dfBef_list.size()) {
        val rowBf: Row = dfBef_list.get(i)
        val rowAf: Row = dfAft_list.get(i)
        for (j <- 0 until schema.length) {
          val colName: String = schema(j)
          val before_result: Double = rowBf.getDouble(j)
          val after_result: Double = rowAf.getDouble(j)
          pw.println(s"${printTime} $colName in table $tableName before $op start: $before_result" +
              s" and $colName after $op start : $after_result")
          DMLOp.getOperation(op) match {
            case DMLOp.INSERT =>
              defaultValue = 1
              if (colName.toUpperCase.startsWith("COUNT")) {
                rowCount = before_result.toLong
                val expectedRs = after_result - before_result
                if (!(expectedRs == 0 || expectedRs == batchSize)) atomicityCheckFailed = true
              } else if (colName.toUpperCase.startsWith("AVG")) {
                val expectedRs =
                  ((before_result * rowCount) + (defaultValue * batchSize)) / (rowCount + batchSize)
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              } else if (colName.toUpperCase.startsWith("SUM")) {
                val expectedRs = before_result + (defaultValue * batchSize)
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              }
            case DMLOp.UPDATE =>
              defaultValue = 1
              if (colName.toUpperCase.startsWith("COUNT")) {
                val expectedRs = after_result - before_result
                if (!(expectedRs == 0)) atomicityCheckFailed = true
              } else if (colName.toUpperCase.startsWith("AVG")) {
                val expectedRs = before_result + defaultValue
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              } else if (colName.toUpperCase.startsWith("SUM")) {
                val expectedRs = before_result + (defaultValue * rowCount)
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              }
            case DMLOp.DELETE =>
              defaultValue = 0
              if (colName.toUpperCase.startsWith("COUNT")) {
                val expectedRs = after_result - before_result
                if (!(expectedRs % before_result == 0)) atomicityCheckFailed = true
              } else if (colName.toUpperCase.startsWith("AVG")) {
                val expectedRs = after_result - before_result
                if (!(expectedRs % before_result == 0)) atomicityCheckFailed = true
              } else if (colName.toUpperCase.startsWith("SUM")) {
                val expectedRs = after_result - before_result
                if (!(expectedRs % before_result == 0)) atomicityCheckFailed = true
              }
            case DMLOp.PUTINTO =>
              defaultValue = 1
              if (colName.toUpperCase.startsWith("COUNT")) {
                rowCount = before_result.toLong
                val expectedRs = after_result - before_result
                if (!(expectedRs % batchSize == 0)) atomicityCheckFailed = true
              } else if (colName.toUpperCase.startsWith("AVG")) {
                val expectedRs =
                  ((before_result * rowCount) + (defaultValue * batchSize)) / (rowCount + batchSize)
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              } else if (colName.toUpperCase.startsWith("SUM")) {
                val expectedRs = before_result + (defaultValue * batchSize)
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true
                }
              }
            case _ =>
          }
          pw.flush()
        }
      }
    } catch {
      case e: Exception => pw.println("Got Exception while verifying data"
          + e.getMessage + e.printStackTrace())
    }
    atomicityCheckFailed
  }

  // scalastyle:off println
  protected def waitForBarrier(barrierName: String, numThreads: Int, pw: PrintWriter): Unit = {
    pw.println("Waiting for " + numThreads + " to meet at barrier " + barrierName)
    pw.flush()
    barrier.await()
    pw.println("Wait Completed for barrier " + barrierName)
    pw.flush()
  }
}
