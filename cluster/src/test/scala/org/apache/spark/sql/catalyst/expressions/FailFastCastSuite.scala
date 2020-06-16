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

import java.sql.{Date, Timestamp}

import scala.util.Try

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

class FailFastCastSuite extends SnappyFunSuite with BeforeAndAfter {

  protected val tableName = "table1"

  override def beforeAll(): Unit = {
    snc.sql("set snappydata.failOnCastError=true")
    snc.sql(s"drop table if exists $tableName")
    snc.sql(s"create table $tableName (int_col int, string_col string, date_col date," +
        s" decimal_col decimal(5,3), float_col float, double_col double, timestamp_col timestamp)")
    snc.sql(s"truncate table $tableName")
    snc.sql(s"insert into $tableName values(1, 'abc', '01/02/1970', 12.345, 12.345, 12.345," +
        s" '1970-01-02 00:00:00')")
  }

  override def afterAll(): Unit = {
    Try(snc.sql(s"drop table if exists $tableName"))
    Try(snc.sql("set snappydata.failOnCastError=false"))
  }

  test("string to other types cast") {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, BooleanType, DateType,
      TimestampType, DecimalType.SYSTEM_DEFAULT).foreach(numericType =>
      try {
        snc.sql(s"select cast(string_col as ${numericType.simpleString}) from $tableName").show()
        fail("Should have failed due to cast failure.")
      } catch {
        case ex: SparkException if (ex.getCause.isInstanceOf[TypeCastException]) =>
          val expectedMessage = s"Can not cast ${StringType.simpleString} type value 'abc' to" +
              s" ${numericType.simpleString}."
          assertResult(expectedMessage)(ex.getCause.getMessage)
      }
    )
  }

  test("NaN fractional type cast to timestamp") {
    val tableName = "table2"
    val snappy = snc.snappySession
    import snappy.implicits._
    val doubleDf: DataFrame = Seq(Double.NaN).toDF
    snc.sql(s"create table $tableName(value double) using column")
    doubleDf.write.insertInto(tableName)
    try {
      snc.sql(s"select cast(value as timestamp) from $tableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: TypeCastException =>
        val expectedMessage = "Can not cast double type value 'NaN' to timestamp."
        assertResult(expectedMessage)(ex.getMessage)
    } finally {
      snc.sql(s"drop table if exists $tableName")
    }
  }

  // Testing this using spark temp table instead of snappy table as at gemfire layer we
  // don't allow storing Float#PosititiveInfinity value.
  test("Infinity fractional type cast to timestamp") {
    val tmpTableName = "tmp_table"
    val snappy = snc.snappySession
    import snappy.implicits._
    val doubleDf: DataFrame = Seq(Float.PositiveInfinity).toDF
    doubleDf.createOrReplaceTempView(tmpTableName)
    try {
      snc.sql(s"select cast(value as timestamp) from $tmpTableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: TypeCastException =>
        val expectedMessage = "Can not cast double type value 'Infinity' to timestamp."
        assertResult(expectedMessage)(ex.getMessage)
    }
  }

  test("test decimal precision overflow") {
    try {
      snc.sql(s"select cast(decimal_col as decimal(4,3)) from $tableName").show()
      fail("Should have failed due to decimal precision loss.")
    } catch {
      case ex: SparkException if ex.getCause.isInstanceOf[TypeCastException] =>
        val expectedMessage = "Can not cast decimal(5,3) type value '12.345' to decimal(4,3)."
        assertResult(expectedMessage)(ex.getCause.getMessage)
    }
  }

  test("casting date type to decimal type") {
    try {
      snc.sql(s"select cast(date_col as decimal) from $tableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: SparkException if ex.getCause.isInstanceOf[TypeCastException] =>
        val expectedMessage = "Can not cast date type value '1' to decimal(38,18)."
        assertResult(expectedMessage)(ex.getCause.getMessage)
    }
  }

  test("casting timestamp type to a narrower decimal type") {
    try {
      snc.sql(s"select cast(timestamp_col as decimal(2,1)) from $tableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: SparkException if ex.getCause.isInstanceOf[TypeCastException] =>
        val expectedMessage = "Can not cast decimal(6,1) type value '66600.0' to decimal(2,1)."
        assertResult(expectedMessage)(ex.getCause.getMessage)
    }
  }

  test("casting float type to decimal type") {
    try {
      snc.sql(s"select cast(float_col as decimal(2,1)) from $tableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: SparkException if ex.getCause.isInstanceOf[TypeCastException] =>
        // float value is cast to double in the code hence it gets converted to decimal(17,15)
        val expectedMessage = "Can not cast decimal(17,15) type value '12.345000267028809' to" +
            " decimal(2,1)."
        assertResult(expectedMessage)(ex.getCause.getMessage)
    }
  }

  test("casting double type to decimal type") {
    try {
      snc.sql(s"select cast(double_col as decimal(2,1)) from $tableName").show()
      fail("Should have failed due to cast failure.")
    } catch {
      case ex: SparkException if ex.getCause.isInstanceOf[TypeCastException] =>
        val expectedMessage = "Can not cast decimal(5,3) type value '12.345' to decimal(2,1)."
        assertResult(expectedMessage)(ex.getCause.getMessage)
    }
  }
}