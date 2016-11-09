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

import scala.util.{Failure, Success, Try}

import io.snappydata.SnappyFunSuite
import io.snappydata.core.RefData

import io.snappydata.udf.{udf1, UDF1}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.types.{DataTypes, DataType}

class StringLengthUDF extends udf1[String, Int] {
  override def call(t1: String): Int = t1.length

  override def getDataType: DataType = DataTypes.IntegerType
}

class SnappyUDFTest extends SnappyFunSuite with BeforeAndAfterAll {


  override def beforeAll: Unit = {
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"some $i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")
    snc.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
  }

  override def afterAll: Unit = {
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")
  }

  test("Test UDF API") {

    snc.snappySession.createFunction("APP.strnglen",
      className = "org.apache.spark.sql.store.StringLengthUDF",
      isTemporary = false)

    val query = s"select strnglen(description) from RR_TABLE"
    val udfdf = snc.sql(query)


    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)


    snc.snappySession.dropFunction("APP.strnglen", false, false)

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

  test("Test UDF SQL") {
    snc.snappySession.sql("CREATE FUNCTION APP.strnglen AS org.apache.spark.sql.store.StringLengthUDF")

    val query = s"select strnglen(description) from RR_TABLE"
    val udfdf = snc.sql(query)

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)

    //@TDOD validate the output
    snc.snappySession.sql("DESCRIBE FUNCTION APP.strnglen").collect().foreach(println)
    snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED APP.strnglen").collect().foreach(println)
    snc.snappySession.sql("DESCRIBE FUNCTION strnglen").collect().foreach(println)
    snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED strnglen").collect().foreach(println)
    snc.snappySession.sql("SHOW FUNCTIONS strnglen").collect().foreach(println)

    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")
    //Drop again to check if exists functionality
    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

  test("Test UDF SQL for column tables") {
    snc.snappySession.sql("CREATE FUNCTION APP.strnglen AS org.apache.spark.sql.store.StringLengthUDF")

    val query = s"select strnglen(description) from COL_TABLE"
    val udfdf = snc.sql(query)

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")

  }
}
