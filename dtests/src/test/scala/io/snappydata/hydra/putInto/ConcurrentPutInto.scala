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
package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object ConcurrentPutInto extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    // val numThreads = jobConfig.getString("numThreadsForConcExecution").toInt
    val blockSize = jobConfig.getString("blockSize").toLong
    val stepSize = jobConfig.getString("stepSize").toLong
    val pw = new PrintWriter(new FileOutputStream(new File("ConcurrentPutIntoJob.out"), true))
    Try {
      runTasksWithChangingRangeForPutInto(snSession, blockSize, stepSize)
      /*runTasksWithChangingRangeForPutInto(snSession, 100000L, 10000L)
      runTasksWithChangingRangeForPutInto(snSession, 150000L, 20000L)
      runTasksWithChangingRangeForPutInto(snSession, 200000L, 30000L)
      runTasksWithChangingRangeForPutInto(snSession, 500000L, 40000L)
      runTasksWithChangingRangeForPutInto(snSession, 1010000L, 10000L)
      runTasksWithChangingRangeForPutInto(snSession, 100000L, 50000L)*/
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/ConcurrentPutIntoJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  def runTasksWithChangingRangeForPutInto(snSession: SnappySession, blockSize: Long, stepSize: Long): Unit = {
    val globalId = new AtomicInteger()
    val doPut = () => Future {
      val myId = globalId.getAndIncrement()
      for (i <- (myId * 1000) until 1000000) {
        snSession.sql("put into testL select id, " +
            "'biggerDataForInsertsIntoTheTable1_' || id, id * 10.2 " +
            s"from range(${i * stepSize}, ${i * stepSize + blockSize})")
      }
    }
    val doQuery = () => Future {
      val myId = globalId.getAndIncrement()
      for (i <- 0 until 10000000) {
        snSession.sql("select avg(id), max(data), last(data2) from testL " +
            s"where id <> ${myId + i}")
      }
    }
    val putTasks = Array.fill(1)(doPut())
    val queryTasks = Array.fill(1)(doQuery())

    putTasks.foreach(Await.result(_, Duration.Inf))
    queryTasks.foreach(Await.result(_, Duration.Inf))
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}


