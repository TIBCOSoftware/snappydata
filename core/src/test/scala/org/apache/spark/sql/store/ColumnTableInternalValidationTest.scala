/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.store

import scala.util.Try

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfter

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation

class ColumnTableInternalValidationTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter {

  val tableName: String = "ColumnTable"
  val props = Map.empty[String, String]

  after {
    snc.dropTable(tableName, ifExists = true)
    snc.dropTable("ColumnTable2", ifExists = true)
    snc.dropTable("COLUMNTABLE7", ifExists = true)
  }

  test("test the shadow table with eviction options LRUCOUNT on " +
      "compressed table") {
    intercept[AnalysisException] {
      snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'Key1'," +
          "BUCKETS '213'," +
          "REDUNDANCY '2'," +
          "EVICTION_BY 'LRUCOUNT 20')").collect()
    }
    logInfo("Success")
  }

  test("test the shadow table with eviction options " +
      "PARTITION BY PRIMARY KEY on compressed table") {
    intercept[AnalysisException] {
      snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'PRIMARY KEY'," +
          "BUCKETS '213'," +
          "REDUNDANCY '2'," +
          "EVICTION_BY 'LRUCOUNT 20')").collect()
    }
    logInfo("Success")
  }

  test("test the shadow table with NOT NULL Column") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    snc.sql(s"CREATE TABLE $tableName(Key1 INT NOT NULL ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "BUCKETS '100')").collect()
    snc.sql(s"DROP TABLE $tableName")

    logInfo("Success")
  }

  test("test the shadow table with primary key") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")
    intercept[AnalysisException] {
      snc.sql(s"CREATE TABLE $tableName(Key1 INT PRIMARY KEY ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'PRIMARY KEY'," +
          "BUCKETS '100')").collect()
    }
    logInfo("Success")
  }

  // TODO: Need to check insert individually is not working for column table
  // asks for UpdatableRelation
  // withSQLConf doesn't work with sql, as in that case another sqlcontext is used
  test("Test ShadowTable with 1 bucket") {
    Property.ColumnMaxDeltaRows.set(snc.conf, 4)
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).
        asInstanceOf[PartitionedRegion]

    val shadowRegion = Misc.getRegionForTable(ColumnFormatRelation.columnBatchTableName(
      "APP.COLUMNTABLE7"), true).asInstanceOf[PartitionedRegion]

    val data = Seq(Seq(1, 2), Seq(7, 8), Seq(9, 2), Seq(4, 2)) // Seq(5, 6))

    val rdd = sc.parallelize(data, data.length).map(
      s => MyTestData(s.head, s(1)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto("COLUMNTABLE7")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(1,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(2,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(3,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(4,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(5,11)")

    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect()
    assert(r.length == 4)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    assert(region.getColumnMaxDeltaRows == 4)

    assert(region.size == 0)
    assert(shadowRegion.size/3 == 1)
    logInfo("Success")
  }

  test("Test ShadowTable with 2 buckets") {
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '2'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).
        asInstanceOf[PartitionedRegion]

    val shadowRegion = Misc.getRegionForTable(ColumnFormatRelation.columnBatchTableName(
      "APP.COLUMNTABLE7"), true).asInstanceOf[PartitionedRegion]

    val data = Seq(Seq(1, 2), Seq(7, 8), Seq(9, 2), Seq(4, 2), Seq(5, 6))

    val rdd = sc.parallelize(data, data.length).map(
      s => MyTestData(s.head, s(1)))

    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.insertInto("COLUMNTABLE7")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(1,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(2,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(3,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(4,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(5,11)")
    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect()
    assert(r.length == 5)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    // assert(GemFireCacheImpl.getColumnBatchSize == 2)
    // sometimes sizes may be different depending on how are the rows distributed
    if (region.getColumnMaxDeltaRows == 3) {
      assert(region.size > 0)
      assert(shadowRegion.size > 0)
    }
    else {
      assert(region.size == 5)
      assert(shadowRegion.size == 0)
    }

    logInfo("Success")
  }

  test("Test ShadowTable with 1 bucket, single insert") {
    Property.ColumnMaxDeltaRows.set(snc.conf, 4)
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.dropTable("COLUMNTABLE7", ifExists = true)
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).
        asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(ColumnFormatRelation.columnBatchTableName(
      "APP.COLUMNTABLE7"), true).asInstanceOf[PartitionedRegion]

    snc.sql("insert into COLUMNTABLE7 VALUES(1,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(2,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(3,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(4,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(5,11)")

    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect()
    assert(r.length == 5)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    // assert(GemFireCacheImpl.getColumnBatchSize == 2)
    // sometimes sizes may be different depending on how are the rows distributed
    if (region.getColumnMaxDeltaRows == 4) {
      assert(region.size == 1)
      assert(shadowRegion.size/3 == 1)
    }
    else {
      assert(region.size == 5)
      assert(shadowRegion.size/3 == 0)
    }

    logInfo("Success")
  }

  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(snc.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(snc.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => snc.conf.setConfString(key, value)
        case (key, None) => snc.conf.unsetConf(key)
      }
    }
  }
}

case class MyTestData(Key1: Int, Value: Int)
