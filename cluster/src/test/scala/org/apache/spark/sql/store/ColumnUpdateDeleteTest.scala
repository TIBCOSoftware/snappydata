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
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // session.conf.set(Property.ColumnMaxDeltaRows.name, "200")

    val numElements = 50000

    session.sql("create table updateTable (id int, addr string, status boolean) " +
        "using column options(buckets '5')")
    session.sql("create table checkTable1 (id int, addr string, status boolean) " +
        "using column options(buckets '5')")
    session.sql("create table checkTable2 (id int, addr string, status boolean) " +
        "using column options(buckets '3')")
    session.sql("create table checkTable3 (id int, addr string, status boolean) " +
        "using column options(buckets '1')")

    session.range(numElements).selectExpr("id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("updateTable")

    // check updates to integer column

    session.range(numElements).selectExpr(s"id + $numElements",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable1")

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable1").count() === numElements)


    session.sql(s"update updateTable set id = id + ($numElements / 2) where id <> 73")
    session.table("updateTable").show()

    session.sql(s"update updateTable set id = id + ($numElements / 2) where id <> 73")
    session.table("updateTable").show()

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable1").count() === numElements)

    var res = session.sql("select * from updateTable where id = 73").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable where id = cast(substr(addr, 5) as int)")
        .collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable1").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")


    // now check updates to string column

    session.sql(s"update updateTable set id = id - $numElements where id <> 73")
    session.range(numElements).selectExpr(s"id",
      "concat(concat('addr', cast(id as string)), '_update')",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable2")

    session.sql(s"update updateTable set addr = concat(addr, '_update') where id <> 32")
    session.table("updateTable").show()

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable2").count() === numElements)

    res = session.sql("select * from updateTable where id = 32").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")

    res = session.sql("select * from updateTable where addr not like '%_update'").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable2").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")


    // lastly to boolean column

    session.range(numElements).selectExpr(s"id",
      "concat(concat('addr', cast(id as string)), '_update')",
      "case when (id % 2) = 1 then true else false end").write.insertInto("checkTable3")

    session.sql(s"update updateTable set status = not status where id <> 87")
    session.table("updateTable").show()

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable3").count() === numElements)

    res = session.sql("select * from updateTable where id = 87").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 87)
    assert(res(0).getString(1) === "addr87_update")

    res = session.sql("select * from updateTable where status <> ((id % 2) = 1)").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 87)
    assert(res(0).getString(1) === "addr87_update")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable3").collect()
    assert(res.length === 2)
    assert(res(0).getInt(0) === 87 || res(1).getInt(0) === 87)
    assert(res(0).getString(1) === "addr87_update" || res(1).getString(1) === "addr87_update")
    assert(res(0).getInt(0) === 32 || res(1).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32" || res(1).getString(1) === "addr32")
  }

  test("test update for all types") {
    /*
    val session = new SnappySession(sc)
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "100k")
    runAllTypesTest(session)
    */
  }
}
