/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql

import org.apache.spark.sql.sources.TableScanSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyTableScanSuite
  extends TableScanSuite
    with SharedSnappySessionContext
    with SnappySparkTestUtil {

  // all tests in TableScanSuite will run except for below tests
  // read comments above each test for desc on fixes
  override def excluded: Seq[String] = Seq(
    "exceptions",
    "read the data source tables that do not extend SchemaRelationProvider",
    "SPARK-5196 schema field with comment"
  )

  override def ignored: Seq[String] = Seq(
    "Schema and all fields"
  )

  // SimpleScanSource is external to snappy. changed 'TABLE' to 'EXTERNAL TABLE'
  test("SD:exceptions") {
    // Make sure we do throw correct exception when users use a relation provider that
    // only implements the RelationProvider or the SchemaRelationProvider.
    Seq("TEMPORARY VIEW", "EXTERNAL TABLE").foreach { tableType =>
      val schemaNotAllowed = intercept[Exception] {
        sql(
          s"""
             |CREATE $tableType relationProvierWithSchema (i int)
             |USING org.apache.spark.sql.sources.SimpleScanSource
             |OPTIONS (
             |  From '1',
             |  To '10'
             |)
           """.stripMargin)
      }
      assert(schemaNotAllowed.getMessage.contains("does not allow user-specified schemas"))

      val schemaNeeded = intercept[Exception] {
        sql(
          s"""
             |CREATE $tableType schemaRelationProvierWithoutSchema
             |USING org.apache.spark.sql.sources.AllDataTypesScanSource
             |OPTIONS (
             |  From '1',
             |  To '10'
             |)
           """.stripMargin)
      }
      assert(schemaNeeded.getMessage.contains("A schema needs to be specified when using"))
    }
  }

  // SimpleScanSource is external to snappy. changed 'TABLE' to 'EXTERNAL TABLE'
  test("SD:read the data source tables that do not extend SchemaRelationProvider") {
    Seq("TEMPORARY VIEW", "EXTERNAL TABLE").foreach { tableType =>
      val tableName = "relationProvierWithSchema"
      withTable(tableName) {
        sql(
          s"""
             |CREATE $tableType $tableName
             |USING org.apache.spark.sql.sources.SimpleScanSource
             |OPTIONS (
             |  From '1',
             |  To '10'
             |)
           """.stripMargin)
        checkAnswer(spark.table(tableName), spark.range(1, 11).toDF())
      }
    }
  }

  // according to snappy implementation comment desc needs ' not "
  test("SD:SPARK-5196 schema field with comment") {
    sql(
      """
        |CREATE TEMPORARY VIEW student(name string comment 'SN', age int comment 'SA', grade int)
        |USING org.apache.spark.sql.sources.AllDataTypesScanSource
        |OPTIONS (
        |  from '1',
        |  to '10',
        |  option_with_underscores 'someval',
        |  option.with.dots 'someval'
        |)
      """.stripMargin)

    val planned = sql("SELECT * FROM student").queryExecution.executedPlan
    val comments = planned.schema.fields.map(_.getComment().getOrElse("NO_COMMENT")).mkString(",")
    assert(comments === "SN,SA,NO_COMMENT")
  }
}
