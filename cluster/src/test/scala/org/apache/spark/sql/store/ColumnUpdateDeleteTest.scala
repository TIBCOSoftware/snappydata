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

import org.apache.spark.sql.SnappySession

/**
 * Tests for updates/deletes on column table.
 */
class ColumnUpdateDeleteTest extends ColumnTablesTestBase {

  test("basic update") {
    val session = this.snc.snappySession
    session.conf.set(Property.ColumnBatchSize.name, "10k")

    val numElements = 10000

    session.sql("create table updateTable (id int, addr string) using column")
    session.sql("create table updateTable2 (id int, addr string) using column")

    session.range(numElements).selectExpr("id", "concat('addr', cast(id as string))")
        .write.insertInto("updateTable")
    session.range(numElements).selectExpr("id + 1", "concat('addr', cast(id as string))")
        .write.insertInto("updateTable2")

    // session.sql("update updateTable set id = id + 1")

    try {
      assert(session.table("updateTable").count() === numElements)
      assert(session.table("updateTable2").count() === numElements)

      assert(session.sql("select * from updateTable EXCEPT select * from updateTable2")
          .collect().length === 0)
    } finally {
      Thread.sleep(1000000)
    }
  }

  test("test update for all types") {
    val session = new SnappySession(sc)
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "100k")
    runAllTypesTest(session)
  }
}
