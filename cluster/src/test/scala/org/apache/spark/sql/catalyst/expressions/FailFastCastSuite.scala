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
package org.apache.spark.sql.catalyst.expressions

import io.snappydata.SnappyFunSuite

import org.apache.spark.SparkException

class FailFastCastSuite extends SnappyFunSuite {

  val tableName = "table1"

  override def beforeAll(): Unit = {
    snc.sql("set snappydata.failFastTypeCasting=true")
  }

  override def afterAll(): Unit = {
    snc.sql(s"drop table if exists $tableName")
    snc.sql("set snappydata.failFastTypeCasting=false")
  }

  test("string to numeric type cast") {
    snc.sql(s"create table $tableName (col1 int, col2 string)")
    snc.sql(s"insert into $tableName values(1, 'abc'), (2,'pqr')")
    Seq("byte", "short", "int", "long", "float", "double", "decimal").foreach(numericType =>
      try {
        snc.sql(s"select cast(col2 as $numericType) from $tableName").show()
        fail(s"Should have failed with ${classOf[NumberFormatException].getName}")
      } catch {
        case ex: SparkException
          if (ex.getCause.isInstanceOf[NumberFormatException]) => // ignore as expected
      }
    )
  }
}
