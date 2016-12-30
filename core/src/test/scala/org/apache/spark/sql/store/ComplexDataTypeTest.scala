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
package org.apache.spark.sql.store

import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter}

import org.apache.spark.Logging


class ComplexDataTypeTest extends SnappyFunSuite with Logging
with BeforeAndAfter with BeforeAndAfterAll {


  test("Test create and load column table from external table with user specified schema") {

    snc.sql("create table t2(a array<integer>,c map<integer,integer>,d struct<empid:integer," +
        "name:String>,b integer) using row options()");

    snc.sql("create table t3(a integer,b integer) using row options()");

    snc.sql("insert into t3 values(1,2)");
    snc.sql("insert into t2 values select array(1,3),map(2,1),struct(123,'sachin'),4 from t3 " +
        "limit 1");
    /*
    +------+-----------+------------+---+
    |     A|          C|           D|  B|
    +------+-----------+------------+---+
    |[1, 3]|Map(2 -> 1)|[123,sachin]|  4|
    +------+-----------+------------+---+
   */
    val res1 =snc.sql("select * from t2").collect()
    assert(res1(0).getList[Integer](0).get(0) == 1)
    assert(res1(0).getList[Integer](0).get(1) == 3)

    assert(res1(0).getMap[Integer,Integer](1).get(2).get == 1)
    assert(res1(0).getStruct(2).getAs[Integer]("EMPID") == 123)

    snc.sql("select explode(a) from t2").show

    /*
     +---+
     |col|
     +---+
     |  1|
     |  3|
     +---+
   */

    snc.sql("select explode(c) from t2").show

    /*
    +---+-----+
    |key|value|
    +---+-----+
    |  2|    1|
    +---+-----+
     */
    snc.sql("select a[0] from t2").show
    /*

     +----+
     |A[0]|
     +----+
     |   1|
     +----+
     */
    snc.sql("select c[2] from t2").show
    /*
      +----+
      |C[2]|
      +----+
      |   1|
      +----+
     */
    snc.sql("select d.empid,d.name from t2").show

    /*
      +-----+------+
      |EMPID|  NAME|
      +-----+------+
      |  123|sachin|
      +-----+------+
    */

  }


}
