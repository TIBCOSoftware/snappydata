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

import com.gemstone.gemfire.internal.cache.{PartitionedRegion, GemFireCacheImpl}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

class PreparedQueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

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
        stmt.addBatch(s"insert into $tableName values($i, '$i')")
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

    println(serverHostPort)
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
      try {
      val qry = s"select ol_w_id, ol_d_id " +
          s" from $tableName " +
          s" where ol_w_id < ? " +
          s" limit 20" +
          s""

      val prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      val rs = prepStatement.executeQuery

      // val rs = stmt.executeQuery(qry)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        val s = rs.getString(2)
        println(s"row($index) $i $s ")
        index += 1
      }
      println("Number of rows read " + index)
      val reg = Misc.getRegionByPath("/APP/ORDER_LINE_COL", false).asInstanceOf[PartitionedRegion]
      println("reg " + reg)
      val b1 = reg.getDataStore.getAllLocalBucketRegions.iterator().next()
      println("b = " + b1.getName + " and size = " + b1.size());

      val itr = reg.getDataStore.getAllLocalBucketRegions.iterator()
      itr.next()
      val b2 = itr.next()
      println("b = " + b2.getName + " and size = " + b2.size());
      rs.close()

      //Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  test("test serialization with lesser dml chunk size") {

    snc.sql("create table order_line_col (ol_w_id  integer,ol_d_id STRING) using column " +
        "options( partition_by 'ol_w_id, ol_d_id', buckets '2')")


    serverHostPort = TestUtil.startNetServer()
    println("network server started")
    insertRows(2)

    val r = GemFireCacheImpl.getInstance().getPartitionedRegions
    println(r)
    // (1 to 5).foreach(d => query())
    query()
  }
}
