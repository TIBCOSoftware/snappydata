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
import java.util.concurrent.Executors

import hydra.blackboard.AnyCyclicBarrier
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil.DMLOp
import io.snappydata.hydra.testDMLOps.{SnappyDMLOpsUtil, SnappySchemaPrms}

import org.apache.spark.sql.SnappyContext

object ConsistencyTest extends SnappyDMLOpsUtil {


  def performOpsAndVerifyConsistency(snc: SnappyContext, pw: PrintWriter, tid: Int, dmlOp: String,
      batchSize: Int, dmlStmt: String, selectStmt: String)
  : Unit = {
    val pool = Executors.newFixedThreadPool(2)
    pool.execute(new DMLOpsThread(snc, pw, tid, dmlStmt))
    pool.execute(new SelectOpsThread(snc, pw, tid, selectStmt))
  }

  // scalastyle:off println
  class DMLOpsThread(snc: SnappyContext, pw: PrintWriter, tid: Int, stmt: String) extends Runnable {
    override def run(): Unit = {
      pw.println(s"Executing dml statement $stmt")
      snc.sql(stmt)
    }
  }

  // scalastyle:off println
  class SelectOpsThread(snc: SnappyContext, pw: PrintWriter, tid: Int, stmt: String) extends
  Runnable {
    override def run(): Unit = {
      try {
        pw.println("Executing query :" + stmt)
        val beforeDMLRS = snc.sql(stmt)
        waitForBarrier(tid + "", 2, pw)
        val afterDMLRS = snc.sql(stmt)
        pw.println("Verifying the results for atomicity..")
      } catch {
        case se: SQLException =>
          pw.println("Got exception while executing select query", se)
      }
    }
  }

  // scalastyle:off println
  def verifyAtomicity(rs_before: Int, rs_after: Int, op: String, pw: PrintWriter): Boolean = {
    DMLOp.getOperation(op) match {
      case DMLOp.INSERT =>
      case DMLOp.DELETE =>
      case DMLOp.PUTINTO =>
        pw.println(s"Number of rows before DML start: ${rs_before} and number of rows after " +
            s"DML start : ${rs_after}")
        if (rs_after % 1000 == 0) return true
      case DMLOp.UPDATE =>
        pw.println(s"Avg before update: ${rs_before} and Avg after update started : ${rs_after}")
        if (rs_after % 5 == 0) return true
    }
    false
  }

  // scalastyle:off println
  protected def waitForBarrier(barrierName: String, numThreads: Int, pw: PrintWriter): Unit = {
    if (!SnappySchemaPrms.getIsSingleBucket) {
      val barrier = AnyCyclicBarrier.lookup(numThreads, barrierName)
      pw.println("Waiting for " + numThreads + " to meet at barrier")
      barrier.await()
      pw.println("Wait Completed...")
    }
  }
}
