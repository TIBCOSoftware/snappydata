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

package io.snappydata.benchmark.LoadPerformance

import java.io.{PrintStream, File, FileOutputStream}

import io.snappydata.benchmark.TPCHColumnPartitionedTable

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 29/8/16.
 */
object BulkLoad_Spark {

  def main(args: Array[String]) {
    var loadPerfFileStream: FileOutputStream = new FileOutputStream(new File(s"BulkLoadPerf.out"))
    var loadPerfPrintStream:PrintStream = new PrintStream(loadPerfFileStream)

    val conf = new SparkConf().setAppName("BulkLoad_Spark")

    val props = null
    var isSnappy = false
    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)
    val path = args(0)
    val buckets = args(1)

    snc.sql("DROP TABLE IF EXISTS " + "LINEITEM")
    snc.sql("DROP TABLE IF EXISTS " + "ORDERS")

    TPCHColumnPartitionedTable.testLoadOrderTablePerformance(snc, path, isSnappy, buckets, loadPerfPrintStream)
    TPCHColumnPartitionedTable.testLoadLineItemTablePerformance(snc, path, isSnappy, buckets, loadPerfPrintStream)

    loadPerfPrintStream.close()
    loadPerfFileStream.close()
    sc.stop()

  }
}
