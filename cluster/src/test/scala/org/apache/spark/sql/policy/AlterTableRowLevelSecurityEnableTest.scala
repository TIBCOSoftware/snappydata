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
package org.apache.spark.sql.policy

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import org.junit.Assert._

import org.apache.spark.sql.SnappyContext

class AlterTableRowLevelSecurityEnableTest extends PolicyTestBase {

  var serverHostPort: String = _

  val props = Map.empty[String, String]
  val tableOwner = "ashahid"
  val colTable = "ColumnTable"
  val rowTable = "RowTable"
  val numElements = 100
  val colTableName: String = s"$tableOwner.$colTable"
  val rowTableName: String = s"$tableOwner.$rowTable"

  var ownerContext: SnappyContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)

    ownerContext = snc.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)

  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, ifExists = true)
    ownerContext.dropTable(rowTableName, ifExists = true)
    super.afterAll()
  }

  test("check rls enable/disable for jdbc client") {
    val conn = getConnection
    try {
      val stmt = conn.createStatement()

      var rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      stmt.execute(s"alter table $rowTableName enable row level security")

      stmt.execute(s"alter table $colTableName enable row level security")

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertTrue(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertTrue(rs.getBoolean(1))

      stmt.execute(s"alter table $rowTableName disable row level security")

      stmt.execute(s"alter table $colTableName disable row level security")

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))
    } finally {
      conn.close()
    }

  }

  test("check rls enable/disable for snappy context") {
    val conn = getConnection
    try {
      val stmt = conn.createStatement()

      var rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      ownerContext.sql(s"alter table $rowTableName enable row level security")

      ownerContext.sql(s"alter table $colTableName enable row level security")

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertTrue(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertTrue(rs.getBoolean(1))

      ownerContext.sql(s"alter table $rowTableName disable row level security")

      ownerContext.sql(s"alter table $colTableName disable row level security")

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${colTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))

      rs = stmt.executeQuery(s"select ROWLEVELSECURITYENABLED from sys.systables " +
          s"where tableschemaname = '${tableOwner.toUpperCase}' and " +
          s"tablename = '${rowTable.toUpperCase}'")

      assert(rs.next())
      assertFalse(rs.getBoolean(1))
    } finally {
      conn.close()
    }
  }


  private def getConnection: Connection = {
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort")
  }

}
