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
package org.apache.spark.sql.store

import java.sql.{DriverManager, SQLException}

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.BeforeAndAfterAll

//scalastyle:ignore

import org.apache.spark.sql.{Row, SaveMode}

/**
 * Tests for non-GFXD JDBC tables.
 */
class JDBCMutableRelationAPISuite
    extends SnappyFunSuite
    with BeforeAndAfterAll {

  val path = "JDBCMutableRelationAPISuite"

  override def beforeAll(): Unit = {
    dirList += path
    super.beforeAll()
    DriverManager.getConnection(s"jdbc:derby:$path;create=true")
  }

  override def afterAll(): Unit = {
    try {
      DriverManager.getConnection(s"jdbc:derby:$path;shutdown=true")
    } catch {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx: SQLException =>
        if (sqlEx.getSQLState != "08006" && sqlEx.getSQLState != "XJ015") {
          throw sqlEx
        }
    } finally {
      super.afterAll()
    }
  }

  test("Create table in an external DataStore in Non-Embedded mode") {
    val props = Map(
      "url" -> s"jdbc:derby:$path",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_1")
    snc.sql("CREATE TABLE TEST_JDBC_TABLE_1(COL1 INTEGER,COL2 INTEGER,COL3 INTEGER)")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("jdbc").mode(SaveMode.Overwrite)
        .options(props).insertInto("TEST_JDBC_TABLE_1")
    val count = dataDF.count()
    assert(count === data.length)
  }

  test("Create table in an external DataStore in Non-Embedded mode using schemaDDL") {
    val props = Map(
      "url" -> s"jdbc:derby:$path",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_2")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
    snc.createTable("TEST_JDBC_TABLE_2", "row", schemaDDL,
      Map.empty[String, String], allowExisting = false)
    dataDF.write.insertInto("TEST_JDBC_TABLE_2")
    val tableDF = snc.sql("select * from TEST_JDBC_TABLE_2")
    val count = tableDF.count()
    assert(count === data.length)

    logInfo(snc.update("TEST_JDBC_TABLE_2", "ITEMREF = 3", Row(99),
      "ITEMREF").toString)

    val cdf = snc.sql("Select * from TEST_JDBC_TABLE_2 where ITEMREF = 99")
    assert(cdf.count() === 3)
  }
}
