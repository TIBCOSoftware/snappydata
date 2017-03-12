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

import io.snappydata.SnappyFunSuite

/**
 * Update, delete tests for column tables.
 */
class ColumnMutableTest extends SnappyFunSuite {

  test("Simple single row updates") {
    val session = snc.snappySession

    session.sql("CREATE TABLE TableUpdate(CODE INT, " +
        "DESCRIPTION varchar(100)) USING column")

    session.sql("insert into TableUpdate values (5,'test')")
    session.sql("insert into TableUpdate values (6,'test1')")

    val df1 = session.sql("select DESCRIPTION from TableUpdate " +
        "where DESCRIPTION = 'test'")
    assert(df1.count() == 1)

    val d1 = session.sql("select * from TableUpdate")
    assert(d1.count() == 2)

    session.sql("CREATE TABLE TableUpdate2 USING column AS " +
        "(select * from  TableUpdate)")

    val d2 = session.sql("select * from TableUpdate2")
    assert(d2.count() == 2)

    session.sql("update TableUpdate2 set DESCRIPTION ='No#complaints' " +
        "where CODE = 5").collect()

    val df2 = session.sql("select DESCRIPTION from TableUpdate2 " +
        "where DESCRIPTION = 'No#complaints' ")
    assert(df2.count() == 1)

    val df3 = session.sql("select DESCRIPTION from TableUpdate2 " +
        "where DESCRIPTION in ('No#complaints', 'test1') ")
    assert(df3.count() == 2)

    session.dropTable("TableUpdate")
    session.dropTable("TableUpdate2")
  }
}
