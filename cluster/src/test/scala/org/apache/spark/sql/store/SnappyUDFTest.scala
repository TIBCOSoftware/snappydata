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

import java.sql.DriverManager

import scala.util.{Failure, Success, Try}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.core.RefData
import io.snappydata.udf.UDF1
import io.snappydata.{Lead, ServiceManager, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.types.{DataType, DataTypes}

class StringLengthUDF extends UDF1[String, Int] {
  override def call(t1: String): Int = t1.length

  override def getDataType: DataType = DataTypes.IntegerType
}

class SnappyUDFTest extends SnappyFunSuite with BeforeAndAfterAll {

  val query = s"select strnglen(description) from RR_TABLE"
  var serverHostPort :String = null

  override def beforeAll: Unit = {
    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"some $i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")
    snc.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, description String) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")
    serverHostPort = TestUtil.startNetServer()
  }

  override def afterAll: Unit = {
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")
  }

  private def detailedTest(): Unit = {
    assert(snc.snappySession.sessionCatalog.listFunctions("app", "str*").
        find(f => (f._1.toString().contains("strnglen"))).size == 1)


    assert(snc.snappySession.sql("DESCRIBE FUNCTION APP.strnglen").collect().length == 3)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED APP.strnglen").collect().length == 4)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION strnglen").collect().length == 3)
    assert(snc.snappySession.sql("DESCRIBE FUNCTION EXTENDED strnglen").collect().length == 4)
    assert(snc.snappySession.sql("SHOW FUNCTIONS strnglen").collect().length == 1)
    assert(snc.snappySession.sql("SHOW FUNCTIONS APP.strnglen").collect().length == 1)

    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")
    //Drop again to check if exists functionality
    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")

    Try(snc.sql(query).count()) match {
      case Success(df) => throw new AssertionError(" Should not have succeded with dropped function")
      case Failure(error) => // Do nothing
    }
  }

  test("Test UDF API") {

    snc.snappySession.createFunction("APP.strnglen",
      className = "org.apache.spark.sql.store.StringLengthUDF",
      isTemporary = false)

    val udfdf = snc.sql(query)

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    detailedTest

  }

  test("Test UDF SQL") {
    snc.snappySession.sql("CREATE FUNCTION APP.strnglen AS org.apache.spark.sql.store.StringLengthUDF")

    val query = s"select strnglen(description) from RR_TABLE"
    val udfdf = snc.sql(query)

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))
    detailedTest
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

  test("Test Java UDF for column tables") {
    snc.snappySession.sql("CREATE FUNCTION APP.strnglen AS io.snappydata.udf.StringLength")

    val query = s"select strnglen(description) from COL_TABLE"
    val udfdf = snc.sql(query)

    assert(udfdf.collect().forall(r => {
      r.getInt(0) == 6
    }))

    snc.snappySession.sql("DROP FUNCTION IF EXISTS app.strnglen")
  }

  ignore("Test all UDFs"){
    // create a table with 22 cols with int type
    // insert 4 rows with all 1 values
    // create 22 udfs which will sum the cols. assertion will be udf1 = 1 udf2 = 2 etc

  }

  test("Test UDAFs"){
    snc.snappySession.sql(s"CREATE FUNCTION APP.mydoubleavg AS io.snappydata.udf.MyDoubleAvg")
    val query = s"select mydoubleavg(ORDERREF) from COL_TABLE"
    val udfdf = snc.sql(query)
    println(udfdf.queryExecution.executedPlan)
    udfdf.show
  }

  test("Test DDLs from client connection"){
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    try {
      stmt.execute(
        "CREATE FUNCTION APP.strnglen AS org.apache.spark.sql.store.StringLengthUDF")
      stmt.execute("drop FUNCTION APP.strnglen")
    } finally {
      stmt.close()
      conn.close()
    }
  }

}
