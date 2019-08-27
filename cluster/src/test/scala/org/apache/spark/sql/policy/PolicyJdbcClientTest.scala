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
package org.apache.spark.sql.policy

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.{Attribute, TestUtil}
import org.junit.Assert.{assertEquals, assertFalse}

import org.apache.spark.sql.{Row, SnappySession}

class PolicyJdbcClientTest extends PolicyTestBase {

  var serverHostPort: String = _

  val props = Map.empty[String, String]
  val tableOwner = "ashahid"
  val numElements = 100
  val colTableName: String = s"$tableOwner.ColumnTable"
  val rowTableName: String = s"$tableOwner.RowTable"
  var ownerSession: SnappySession = _

  override protected def systemUser: String = tableOwner

  override def beforeAll(): Unit = {
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerSession = snc.snappySession.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerSession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF = ownerSession.createDataFrame(rdd)

    ownerSession.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerSession.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
    val conn = getConnection()
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"alter table $colTableName enable row level security")
      stmt.execute(s"alter table $rowTableName enable row level security")
    } finally {
      conn.close()
    }

  }

  override def afterAll(): Unit = {
    ownerSession.dropTable(colTableName, ifExists = true)
    ownerSession.dropTable(rowTableName, ifExists = true)
    TestUtil.stopNetServer()
    super.afterAll()
  }

  test("Policy creation on a column table using jdbc client") {
    this.testPolicy(colTableName)
  }

  test("Policy creation on a row table using jdbc client") {
    this.testPolicy(rowTableName)
  }

  private def testPolicy(tableName: String) {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id < 0")
      var rs = stmt.executeQuery(s"select * from $tableName")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0
      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableName")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)
      stmt.execute("drop policy testPolicy1")
    } finally {
      conn.close()
      conn1.close()
    }
  }

  test("Policy application on views on table with policy created before view creation") {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$colTableName for select to current_user using id < 0")
      stmt.execute(s"create policy testPolicy2 on  " +
          s"$rowTableName for select to current_user using id < 0")
      stmt.execute(s"CREATE VIEW col_view  AS SELECT id FROM $colTableName")
      stmt.execute(s"CREATE VIEW row_view  AS SELECT id FROM $rowTableName")
      var rs = stmt.executeQuery(s"select * from col_view")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0

      rs = stmt.executeQuery(s"select * from row_view")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0

      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableOwner.col_view")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)

      rs = stmt1.executeQuery(s"select * from $tableOwner.row_view")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)

      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      stmt.execute("drop view col_view")
      stmt.execute("drop view row_view")
      conn.close()
      conn1.close()
    }
  }

  test("Policy application on views on table with policy created after view creation") {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"CREATE VIEW col_view  AS SELECT id FROM $colTableName")
      stmt.execute(s"CREATE VIEW row_view  AS SELECT id FROM $rowTableName")
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$colTableName for select to current_user using id < 0")
      stmt.execute(s"create policy testPolicy2 on  " +
          s"$rowTableName for select to current_user using id < 0")
      var rs = stmt.executeQuery(s"select * from col_view")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0

      rs = stmt.executeQuery(s"select * from row_view")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0

      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableOwner.col_view")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)

      rs = stmt1.executeQuery(s"select * from $tableOwner.row_view")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)

      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      stmt.execute("drop view col_view")
      stmt.execute("drop view row_view")
      conn.close()
      conn1.close()
    }
  }

  test("test policy not applied for update | delete on row table - SNAP-2576") {
    this.updateOrDeleteOntableWithPolicy("row")
  }

  test("test policy not applied for update | delete on column table - SNAP-2576") {
    this.updateOrDeleteOntableWithPolicy("column")
  }

  private def updateOrDeleteOntableWithPolicy(tableType: String): Unit = {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    val stmt1 = conn1.createStatement()
    ownerSession.sql(s"CREATE TABLE temp (username String, id Int) " +
        s" USING $tableType ")
    val seq = Seq("USERX" -> 4, "USERX" -> 5, "USERX" -> 6, "USERY" -> 7,
      "USERY" -> 8, "USERY" -> 9)
    val rdd = sc.parallelize(seq)

    val dataDF = ownerSession.createDataFrame(rdd)

    dataDF.write.insertInto("temp")

    stmt.execute(s"create policy testPolicy1 on  " +
        s" temp for select to current_user using " +
        s" id < 0")

    stmt.execute("alter table temp enable row level security")


    val q1 = s"select * from $tableOwner.temp"
    var rs = stmt1.executeQuery(q1)
    assertFalse(rs.next())

    var n = stmt1.executeUpdate(s"update $tableOwner.temp set " +
        s"username = 'USERZ' where username = 'USERX'")

    assertEquals(3, n)

    rs = stmt.executeQuery(s"select * from temp where username = 'USERZ'")
    n = 0
    while (rs.next()) {
      n += 1
    }
    assertEquals(3, n)

    n = stmt1.executeUpdate(s"delete from $tableOwner.temp  where username = 'USERZ'")
    assertEquals(3, n)
    rs = stmt.executeQuery(s"select * from temp where username = 'USERZ'")
    assertFalse(rs.next())

    ownerSession.sql("drop policy testPolicy1")
    ownerSession.sql(s"drop table temp")
  }

  test("test multiple policies application using snappy context on column table") {
    this.testMultiplePolicy(colTableName)
  }

  test("test multiple policies application using snappy context on row table") {
    this.testMultiplePolicy(rowTableName)
  }

  test("Test plan invalidation when queries & policy creation are mixed on column table") {
    this.testMultiplePolicyCreationWithQuery(colTableName)
  }

  test("Test plan invalidation when queries & policy creation are mixed on row table") {
    this.testMultiplePolicyCreationWithQuery(rowTableName)
  }

  test("test policy recreation on column table ENT:38") {
    this.testPolicyRecreation(colTableName)
  }

  test("test policy recreation on row table ENT:38") {
    this.testPolicyRecreation(colTableName)
  }

  def testPolicyRecreation(tableName: String): Unit = {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    val stmt1 = conn1.createStatement()
    try {

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)


      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)

      stmt.execute(s"alter table $tableName disable row level security")
      stmt.execute("drop policy testPolicy1")
      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      stmt.execute(s"alter table $tableName enable row level security")
      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)

      stmt.execute("drop policy testPolicy1")

    } finally {
      conn.close()
      conn1.close()
    }
  }


  def testMultiplePolicyCreationWithQuery(tableName: String): Unit = {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    val stmt1 = conn1.createStatement()
    try {

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)


      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)


      stmt.execute(s"create policy testPolicy2 on  " +
          s"$tableName for select to current_user using id < 30")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(4, rsSize)
      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      conn.close()
      conn1.close()
    }
  }

  private def testMultiplePolicy(tableName: String) {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      stmt.execute(s"create policy testPolicy2 on  " +
          s"$tableName for select to current_user using id < 30")

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0
      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(4, rsSize)
      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      conn.close()
      conn1.close()
    }
  }


  test("old query plan invalidation on enabling rls on column table using jdbc client") {
    this.testQueryPlanInvalidationOnRLSEnbaling(colTableName)
  }

  test("old query plan invalidation on enabling rls on row table using jdbc client") {
    this.testQueryPlanInvalidationOnRLSEnbaling(rowTableName)
  }

  test("syspolicies table/vti") {
    // create some policies on column & row tables
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$colTableName for select to current_user using id > 10")

      stmt.execute(s"create policy testPolicy2 on  " +
          s"$rowTableName for select to current_user using id < 30")

      stmt.execute(s"create policy testPolicy3 on  " +
          s"$rowTableName for select to current_user using id < 70")

      val expectedColumns = List("NAME", "SCHEMANAME", "TABLENAME",
        "POLICYFOR", "APPLYTO", "FILTER", "OWNER")
      val expectedResults = Map("TESTPOLICY1" -> (tableOwner.toUpperCase,
          colTableName.toUpperCase.substring(colTableName.indexOf('.') + 1),
          "select", "current_user", "id > 10",
          tableOwner.toUpperCase),
        "TESTPOLICY2" -> (tableOwner.toUpperCase,
            rowTableName.toUpperCase.substring(rowTableName.indexOf('.') + 1),
            "select", "current_user", "id < 30",
            tableOwner.toUpperCase),
        "TESTPOLICY3" -> (tableOwner.toUpperCase,
            rowTableName.toUpperCase.substring(rowTableName.indexOf('.') + 1),
            "select", "current_user", "id < 70",
            tableOwner.toUpperCase)
      )

      // check using session
      val ds = ownerSession.sql("select * from sys.syspolicies")
      val rows = ds.collect()
      assert(expectedColumns === ds.schema.map(_.name.toUpperCase))
      assert(expectedResults.toSeq.sortBy(_._1).map(p => Row(p._1, p._2._1, p._2._2,
        p._2._3, p._2._4, p._2._5, p._2._6)) === rows.toSeq.sortBy(_.getString(0)))

      val rs = stmt.executeQuery("select * from sys.syspolicies")
      val rsmd = rs.getMetaData
      assertEquals(expectedColumns.size, rsmd.getColumnCount)
      for (i <- 1 to rsmd.getColumnCount) {
        assert(expectedColumns.contains(rsmd.getColumnName(i)))
      }

      var actualNumRows = 0
      while (rs.next()) {
        actualNumRows += 1
        assert(expectedResults.contains(rs.getString("NAME")))
        val expectedRow = expectedResults(rs.getString("NAME"))
        assertEquals(expectedRow._1, rs.getString("SCHEMANAME"))
        assertEquals(expectedRow._2, rs.getString("TABLENAME"))
        assertEquals(expectedRow._3, rs.getString("POLICYFOR"))
        assertEquals(expectedRow._4, rs.getString("APPLYTO"))
        assertEquals(expectedRow._5, rs.getString("FILTER"))
        assertEquals(expectedRow._6, rs.getString("OWNER"))
      }
      assertEquals(expectedResults.size, actualNumRows)

      // check the connection metadata apis are not getting polluted
      // with policies
      val md = conn.getMetaData
      val tableTypes = md.getTableTypes
      // table type should not include policy
      while (tableTypes.next()) {
        val tt = tableTypes.getString(1)
        assert(tt.toLowerCase.indexOf("policy") == -1)
        assert(tt.toLowerCase.indexOf("policies") == -1)
      }

      val rs1 = md.getTables(null, null, "%", null)
      // should find the SYS.SYSPOLICIES table in meta-data
      var foundSysPolicies = false
      while (rs1.next()) {
        if (rs1.getString("TABLE_NAME") == "SYSPOLICIES") {
          foundSysPolicies = true
          assert(rs1.getString("TABLE_SCHEM") === "SYS")
          assert(rs1.getString("TABLE_TYPE") === "VIRTUAL TABLE")
        }
      }
      assert(foundSysPolicies, "Failed to find SYS.SYSPOLICIES table in meta-data")

      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
      stmt.execute("drop policy testPolicy3")
    } finally {
      conn.close()
    }
  }

  private def testQueryPlanInvalidationOnRLSEnbaling(tableName: String): Unit = {
    // first disable RLS
    ownerSession.sql(s"alter table $tableName disable row level security")
    // now create a policy
    ownerSession.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")
    val conn = getConnection(Some(tableOwner))

    val conn1 = getConnection(Some("UserX"))
    try {

      val q = s"select * from $tableName where id > 70"
      val stmt1 = conn1.createStatement()
      var rs = stmt1.executeQuery(q)
      var numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)
      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)
      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      val stmt = conn.createStatement()
      rs = stmt.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      // Now enable RLS

      stmt.execute(s"alter table $tableName enable row level security")
      rs = stmt.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(0, numRows)
      ownerSession.sql("drop policy testPolicy1")
    } finally {
      conn1.close()
      conn.close()
    }

  }

  test("Drop table with policies using JDBC client") {
    val seq2 = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd2 = sc.parallelize(seq2)

    val dataDF2 = ownerSession.createDataFrame(rdd2)

    val colTableName2: String = s"$tableOwner.ColumnTable2"
    val rowTableName2: String = s"$tableOwner.RowTable2"
    val colTableName3: String = s"$tableOwner.ColumnTable3"

    ownerSession.sql(s"CREATE TABLE $colTableName2 (name String, id Int) " +
        s" USING column ")
    ownerSession.sql(s"CREATE TABLE $rowTableName2 (name String, id Int) " +
        s" USING row ")
    ownerSession.sql(s"CREATE TABLE $colTableName3 (name String, id Int) " +
        s" USING column ")

    dataDF2.write.insertInto(colTableName2)
    dataDF2.write.insertInto(rowTableName2)
    dataDF2.write.insertInto(colTableName3)

    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    try {
      stmt.execute(s"alter table $colTableName2 enable row level security")
      stmt.execute(s"alter table $rowTableName2 enable row level security")
      stmt.execute(s"alter table $colTableName3 enable row level security")

      stmt.execute(s"create policy testPolicy1_for_ColumnTable3 on  " +
          s"$colTableName3 for select to current_user using id > 11")
      stmt.execute(s"create policy testPolicy2_for_ColumnTable3 on  " +
          s"$colTableName3 for select to current_user using id < 22")

      testDropTable(colTableName2, stmt)
      testDropTable(rowTableName2, stmt)

      // colTableName3 was not dropped, so policies should exist
      assert(checkIfPoliciesOnTableExist(colTableName3))

      testDropTable(colTableName3, stmt)
    } finally {
      conn.close()
    }

  }

  private def testDropTable(tableName: String, stmt: Statement) {
    stmt.execute(s"create policy testPolicy11 on  " +
        s"$tableName for select to current_user using id > 11")
    stmt.execute(s"create policy testPolicy22 on  " +
        s"$tableName for select to current_user using id < 22")
    stmt.execute(s"drop table $tableName")
    assert(!checkIfPoliciesOnTableExist(tableName), s"Policy for $tableName should not be present")
  }

  // return true if a policy exists for a table else false
  private def checkIfPoliciesOnTableExist(tableName: String): Boolean = {
    val policies = Misc.getMemStore.getExternalCatalog.getPolicies()
    val it = policies.listIterator()
    while (it.hasNext) {
      val p = it.next()
      //      println("Actual tablename:" + tableName + ", tableName in policy:" + p.tableName)
      if ((p.schemaName + "." + p.tableName).equalsIgnoreCase(tableName)) {
        return true
      }
    }
    false
  }

  private def getConnection(user: Option[String] = None): Connection = {
    val props = new Properties()
    if (user.isDefined) {
      props.put(Attribute.USERNAME_ATTR, user.get)
    }
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort", props)
  }

}
