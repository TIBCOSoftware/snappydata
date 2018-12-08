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

import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ConcPutIntoTest {

  def concPutInto(primaryLocatorHost: String, primaryLocatorPort: String, numThreads: Integer): Any = {
    val globalId = new AtomicInteger()
    val doPut = () => Future {
      val conn = DriverManager.getConnection("jdbc:snappydata://" + primaryLocatorHost + ":" + primaryLocatorPort)
      val stmt = conn.createStatement()
      val myId = globalId.getAndIncrement()
      val blockSize = 100000L
      val stepSize = 50000L
      for (i <- (myId * 1000) until 1000000) {
        stmt.executeUpdate("put into testL select id, " +
            "'biggerDataForInsertsIntoTheTable1_' || id, id * 10.2 " +
            s"from range(${i * stepSize}, ${i * stepSize + blockSize})")
      }
      stmt.close()
      conn.close()
    }
    val doQuery = () => Future {
      val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527")
      val stmt = conn.createStatement()
      val myId = globalId.getAndIncrement()
      for (i <- 0 until 10000000) {
        stmt.executeQuery("select avg(id), max(data), last(data2) from testL " +
            s"where id <> ${myId + i}")
      }
      stmt.close()
      conn.close()
    }
    val putTasks = Array.fill(numThreads)(doPut())
    val queryTasks = Array.fill(numThreads)(doQuery())

    putTasks.foreach(Await.result(_, Duration.Inf))
    queryTasks.foreach(Await.result(_, Duration.Inf))
  }

  def conSelect(primaryLocatorHost: String, primaryLocatorPort: String, numThreads: Integer): Any = {
    val globalId = new AtomicInteger()
    val doQuery = () => Future {
      val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527")
      val stmt = conn.createStatement()
      val myId = globalId.getAndIncrement()
      for (i <- 0 until 10000000) {
        stmt.executeQuery("select avg(id), max(data), last(data2) from testL " +
            s"where id <> ${myId + i}")
      }
      stmt.close()
      conn.close()
    }
    val queryTasks = Array.fill(numThreads)(doQuery())

    queryTasks.foreach(Await.result(_, Duration.Inf))
  }
}