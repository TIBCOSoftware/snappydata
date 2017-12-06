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
package org.apache.spark.sql.execution

import io.snappydata.core.Data
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.snappy._

class ColumnTableMutableAPISuite extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  test("PutInto with RowTable subquery without broadcast") {
    val snc = new SnappySession(sc)
    snc.snappyContext.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    val data1 = Seq(Seq(1, 22, 3), Seq(7, 81, 9),
      Seq(9, 23, 3), Seq(4, 24, 3),
      Seq(5, 6, 7), Seq(88, 88, 88))
    val rdd = sc.parallelize(data1, 2).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    val data2 = Seq(Seq(1, 22, 3), Seq(7, 81, 9),
      Seq(9, 23, 3), Seq(4, 24, 3), Seq(5, 6, 7),
      Seq(8, 8, 8), Seq(88, 88, 90))
    val rdd2 = sc.parallelize(data2, 2).map(s => new Data(s(0), s(1), s(2)))
    val dataDF2 = snc.createDataFrame(rdd2)

    val props = Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1")
    val props1 = Map.empty[String, String]

    Property.ColumnBatchSize.set(snc.sessionState.conf, "50")
    snc.createTable("MY_TABLE", "column", dataDF.schema, props)
    snc.createTable("row_table", "row", dataDF2.schema, props1)

    dataDF.write.insertInto("my_table")
    dataDF2.write.insertInto("row_table")
    val tabdf = snc.table("my_table")

    snc.sql("put into table my_table" +
        "   select * from row_table" +
        "   WITHKEY my_table.col2=row_table.col2")
    tabdf.show

    snc.sql("drop table MY_TABLE")
    snc.sql("drop table row_table")
  }

  test("PutInto with df") {
    val snc = new SnappySession(sc)
    snc.snappyContext.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    val data1 = Seq(Seq(1, 22, 3), Seq(7, 81, 9),
      Seq(9, 23, 3), Seq(4, 24, 3),
      Seq(5, 6, 7), Seq(88, 88, 88))
    val rdd = sc.parallelize(data1, 2).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    val data2 = Seq(Seq(1, 22, 3), Seq(7, 81, 9),
      Seq(9, 23, 3), Seq(4, 24, 3), Seq(5, 6, 7),
      Seq(8, 8, 8), Seq(88, 88, 90))
    val rdd2 = sc.parallelize(data2, 2).map(s => new Data(s(0), s(1), s(2)))
    val dataDF2 = snc.createDataFrame(rdd2)

    val props = Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1")
    val props1 = Map.empty[String, String]

    Property.ColumnBatchSize.set(snc.sessionState.conf, "50")
    snc.createTable("MY_TABLE", "column", dataDF.schema, props)

    dataDF.write.insertInto("my_table")
    val tabdf = snc.table("my_table")

    dataDF2.write.putInto("my_table", usingColumns = Seq("col2"))
    tabdf.show

    snc.sql("drop table MY_TABLE")
  }
}
