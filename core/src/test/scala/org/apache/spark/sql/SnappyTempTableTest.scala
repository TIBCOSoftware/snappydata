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

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.LogicalRelation


class SnappyTempTableTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter {

  val tableName: String = "TestTempTable"
  val props = Map.empty[String, String]

  after {
    snc.dropTable(tableName, ifExists = true)
  }


  test("test drop table from a simple source") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s =>
      io.snappydata.core.Data(s.head, s(1), s(2)))
    val df = snc.createDataFrame(rdd)

    df.createOrReplaceTempView(tableName)

    val catalog = snc.sessionState.catalog
    val qName = snc.snappySession.tableIdentifier(tableName)
    val plan = catalog.resolveRelation(qName)
    plan match {
      case LogicalRelation(_, _, _) => fail(" A RDD based temp table " +
          "should have been matched with LogicalPlan")
      case _ =>
    }

    val scan = snc.sql(s"select * from $tableName")

    assert(scan.count == 5)

    snc.sql(s"drop table $tableName")

    assert(!snc.sessionState.catalog.tableExists(qName))
  }

  test("test drop table from a relational source") {
    val file = getClass.getResource("/airlineCode_Lookup.csv").getPath
    val df = snc.read
        .format("com.databricks.spark.csv") // CSV to DF package
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load(file)

    df.createOrReplaceTempView(tableName)
    val catalog = snc.sessionState.catalog
    val qName = snc.snappySession.tableIdentifier(tableName)
    val plan = catalog.resolveRelation(qName)
    plan match {
      case LogicalRelation(_, _, _) =>
      case _ => fail("A CSV relation temp table should have been " +
          "matched with LogicalRelation")
    }
    val scan = snc.sql(s"select * from $tableName")
    scan.count()

    snc.sql(s"drop table $tableName")

    assert(!snc.sessionState.catalog.tableExists(qName))
  }
}
