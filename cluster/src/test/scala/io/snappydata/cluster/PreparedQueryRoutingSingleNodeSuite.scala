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

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{Property, SnappyFunSuite}
import org.apache.log4j.{Level, Logger}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf

class PreparedQueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  // Logger.getLogger("org").setLevel(Level.DEBUG)

  val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = ""
  val tableName = "order_line_col"

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    /**
      * Setting local[n] here actually supposed to affect number of reservoir created
      * while sampling.
      *
      * Change of 'n' will influence results if they are dependent on weights - derived
      * from hidden column in sample table.
      */
    new org.apache.spark.SparkConf().setAppName("PreparedQueryRoutingSingleNodeSuite")
        .setMaster("local[6]")
        .set("spark.logConf", "true")
        .set("mcast-port", "4958")
  }

  override def beforeAll(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.testing", "true")
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
  }

  override def afterAll(): Unit = {
    System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.testing")
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
        stmt.addBatch(s"insert into $tableName values($i, $i, '$i')")
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
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    println(serverHostPort)
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
      try {
        val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
            s" from $tableName " +
            s" where ol_int_id < ? " +
            s" and ol_int2_id > 100 " +
            s" and ol_str_id LIKE ? " +
            s" limit 20" +
            s""

      val prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setString(2, "%0")
      val rs = prepStatement.executeQuery

      // val rs = stmt.executeQuery(qry)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        val j = rs.getInt(2)
        val s = rs.getString(3)
        println(s"row($index) $i $j $s ")
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

    snc.sql(s"create table $tableName (ol_int_id  integer," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")


    serverHostPort = TestUtil.startNetServer()
    println("network server started")
    insertRows(1000)

    val r = GemFireCacheImpl.getInstance().getPartitionedRegions
    println(r)
    // (1 to 5).foreach(d => query())
    query()
  }
}
