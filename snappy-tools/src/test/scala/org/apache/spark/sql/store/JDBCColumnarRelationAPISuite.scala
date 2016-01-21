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

import java.sql.{DriverManager, SQLException}

import io.snappydata.core.Data
import io.snappydata.{SnappyFunSuite}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterAll

/**
 * Simple test to check column tables using non-Gem JDBC driver.
 */
class JDBCColumnarRelationAPISuite
    extends SnappyFunSuite
    with BeforeAndAfterAll {

  private val path = "JDBCColumnarRelationAPISuite"

  override def beforeAll(): Unit = {
    super.beforeAll()
    DriverManager.getConnection(s"jdbc:derby:$path;create=true")
    dirList += path
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
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_2")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).
        saveAsTable("TEST_JDBC_TABLE_2")
    val count = dataDF.count()
    assert(count === data.length)
  }
}
