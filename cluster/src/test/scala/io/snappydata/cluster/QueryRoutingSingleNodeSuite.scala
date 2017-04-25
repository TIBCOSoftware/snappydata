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
package io.snappydata.cluster

import java.sql.DriverManager

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

class QueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = ""
  val tableName = "order_line_col"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
  }

  override def afterAll(): Unit = {
    setDMLMaxChunkSize(default_chunk_size)
    super.afterAll()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def insertRows(numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i, '1')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      println(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def query(): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(
        s"select ol_w_id  from $tableName ")
      var index = 0
      while (rs.next()) {
        rs.getInt(1)
        index += 1
      }
      println("Number of rows read " + index)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  test("test serialization with lesser dml chunk size") {

    snc.sql("create table order_line_col (ol_w_id  integer,ol_d_id STRING) using column " +
        "options( partition_by 'ol_w_id, ol_d_id', buckets '5')")


    serverHostPort = TestUtil.startNetServer()
    println("network server started")
    insertRows(1000)


    (1 to 5).foreach(d => query())
  }
}
