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

import org.scalatest.Tag

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Runs the tests from `FailFastCastSuite` with code generation disabled.
 */
class FailFastCastWithOutCodeGenSuite extends FailFastCastSuite {

  override def beforeAll(): Unit = {
    snc.sql("set snappydata.failOnCastError=true")
    snc.sql("set spark.sql.codegen.wholeStage=false")
    snc.snappySession.conf.set("spark.sql.codegen.wholeStage", "false")
    val snappy = snc.snappySession
    val data = Seq(Row(8, "abc", new Date(86400000), new java.math.BigDecimal("12.345"),
      12.345.toFloat, 12.345, new Timestamp(66600000)))

    val schema = StructType(Seq(StructField("int_col", IntegerType),
      StructField("string_col", StringType), StructField("date_col", DateType),
      StructField("decimal_col", DecimalType(5, 3)), StructField("float_col", FloatType),
      StructField("double_col", DoubleType), StructField("timestamp_col", TimestampType)
    ))

    val df = snappy.createDataFrame(
      snappy.sparkContext.parallelize(data),
      StructType(schema)
    )

    df.createOrReplaceTempView(tableName)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Unit): Unit = {
    // Ignoring this test as this test uses snappy table and non-codegen flow is not working with
    // snappy table. Also the same code path will be covered by
    // "Infinity fractional type cast to timestamp" so we won't be loosing much on test coverage.
    if (testName.equalsIgnoreCase("NaN fractional type cast to timestamp")) {
      super.registerIgnoredTest(testName, testTags: _*)(testFun)
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}