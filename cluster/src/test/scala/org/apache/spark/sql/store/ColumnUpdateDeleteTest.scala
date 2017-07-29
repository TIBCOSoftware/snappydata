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

package org.apache.spark.sql.store

import io.snappydata.Property

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappySession

/**
 * Tests for updates/deletes on column table.
 */
class ColumnUpdateDeleteTest extends ColumnTablesTestBase {

  override protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf().
        setIfMissing("spark.master", "local[1]").
        setAppName(getClass.getName)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  test("basic update") {
    val session = this.snc.snappySession
    // session.conf.set(Property.ColumnBatchSize.name, "10k")
    session.conf.set(Property.ColumnMaxDeltaRows.name, "200")

    val numElements = 300

    session.sql("create table updateTable (id int, addr string) " +
        "using column options(buckets '1')")
    session.sql("create table updateTable2 (id int, addr string) " +
        "using column options(buckets '1')")

    session.range(numElements).selectExpr("id",
      "concat('addr', cast(id as string))").write.insertInto("updateTable")
    session.range(numElements).selectExpr(s"id + $numElements",
      "concat('addr', cast(id as string))").write.insertInto("updateTable2")

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("updateTable2").count() === numElements)

    session.sql(s"update updateTable set id = id + ($numElements / 2) where id <> 123")
    session.table("updateTable").show()

    session.sql(s"update updateTable set id = id + ($numElements / 2) where id <> 123")
    session.table("updateTable").show()

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("updateTable2").count() === numElements)

    var res = session.sql("select * from updateTable where id = 123").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 123)
    assert(res(0).getString(1) === "addr123")

    res = session.sql("select * from updateTable where id = cast(substr(addr, 5) as int)")
        .collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 123)
    assert(res(0).getString(1) === "addr123")

    res = session.sql("select * from updateTable EXCEPT select * from updateTable2")
        .collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 123)
    assert(res(0).getString(1) === "addr123")
  }

  test("test update for all types") {
    val session = new SnappySession(sc)
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "100k")
    runAllTypesTest(session)
  }
}
