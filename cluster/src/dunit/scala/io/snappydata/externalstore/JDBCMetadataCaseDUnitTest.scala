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
package io.snappydata.externalstore

import java.sql.{Connection, DatabaseMetaData}

import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedDatabaseMetaData.METADATACASE_LOWER_PROP
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, SerializableRunnable}
import org.junit.Assert.assertEquals

import org.apache.spark.Logging

class JDBCMetadataCaseDUnitTest(s: String) extends ClusterManagerTestBase(s)
    with Logging {

  val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort

  // using mixed case name to cover case insensitivity scenarios
  private val table1 = "tABle1"
  private val table2 = "tABle2"
  private val table3 = "tABle3"
  val schema = "Schema1"

  override def beforeClass(): Unit = {
    super.beforeClass()
    System.setProperty(METADATACASE_LOWER_PROP, "true")
    DistributedTestBase.invokeInEveryVM(new SerializableRunnable() {
      override def run(): Unit = System.setProperty(METADATACASE_LOWER_PROP, "true")
    })
  }

  override def afterClass(): Unit = {
    super.afterClass()
    System.clearProperty(METADATACASE_LOWER_PROP)
    DistributedTestBase.invokeInEveryVM(new SerializableRunnable() {
      override def run(): Unit = System.clearProperty(METADATACASE_LOWER_PROP)
    })
  }

  def testJDBCMetadataCase_queryRoutingOn(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("create schema " + schema)
        stmt.execute("create table " + schema + "." + table1 +
            "(id integer primary key, col1 string, col2 long)")
        stmt.execute("create table " + schema + "." + table2 + "(id integer , fs string)")
        stmt.execute("create table " + table3 + "(id integer , fs string)")
      } finally {
        stmt.close()
      }
      val dbmd = conn.getMetaData

      // JDBC metadata APIs should return result in lower case when query routing is true
      // i.e. for external connections
      testMetadataAPIs(dbmd, (s: String) => s.toLowerCase, checkShortTableType = true)

    } finally {
      cleanup(conn)
    }
  }

  // For internal connections we need to continue using uper case metadata as some internal code
  // is written with upper case assumption. We don't want to affect that code. Internal connection
  // is identified based on status of query routing as for internal connections query routing is
  // always disabled.
  def testJDBCMetadataCase_queryRoutingOff(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1, disableQueryRouting = true)
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("create schema " + schema)
        stmt.execute("create table " + schema + "." + table1 +
            "(id integer primary key, col1 string, col2 long)")
        stmt.execute("create table " + schema + "." + table2 + "(id integer , fs string)")
        stmt.execute("create table " + table3 + "(id integer , fs string)")
      } finally {
        stmt.close()
      }
      val dbmd = conn.getMetaData

      // DDBC metadata APIs should return result in upper case when query routing is false
      // i.e. for internal connections
      testMetadataAPIs(dbmd, (s: String) => s.toUpperCase)

    } finally {
      cleanup(conn)
    }
  }

  private def testMetadataAPIs(dbmd: DatabaseMetaData, matchFunction: String => String,
      checkShortTableType: Boolean = false): Unit = {
    testGetTables(dbmd, matchFunction, checkShortTableType)
    testGetSchemas(dbmd, matchFunction)
    testGetColumns(dbmd, matchFunction)
    testGetTableTypes(dbmd, matchFunction)
    testGetTypeInfo(dbmd, matchFunction)
  }

  private def testGetTypeInfo(dbmd: DatabaseMetaData, matchFunction: String => String): Unit = {
    val typeInfoRS = dbmd.getTypeInfo
    var resultSetSize = 0
    while (typeInfoRS.next()) {
      val typeName = typeInfoRS.getString("TYPE_NAME")
      val literalPrefix = typeInfoRS.getString("LITERAL_PREFIX")
      val literalSuffix = typeInfoRS.getString("LITERAL_SUFFIX")
      val createParams = typeInfoRS.getString("CREATE_PARAMS")
      val localTypeName = typeInfoRS.getString("LOCAL_TYPE_NAME")

      logInfo(s"Type info - typeName:$typeName, literalPrefix:$literalPrefix," +
          s" literalSuffix:$literalSuffix, createParam:$createParams," +
          s" localTypeName:$localTypeName")

      assertEquals(matchFunction(typeName), typeName)
      if (literalPrefix != null) assertEquals(matchFunction(literalPrefix), literalPrefix)
      if (literalSuffix != null) assertEquals(matchFunction(literalSuffix), literalSuffix)
      // not asserting below condition because createParams are hard-coded to be in small
      // case in metadata.properties file.
      // if (createParams != null) assertEquals(matchFunction(createParams), createParams)
      assertEquals(matchFunction(localTypeName), localTypeName)

      resultSetSize += 1
    }

    assertEquals(26, resultSetSize)
  }

  private def testGetTableTypes(dbmd: DatabaseMetaData, matchFunction: String => String): Unit = {
    val tableTypesRS = dbmd.getTableTypes

    var resultSetSize = 0
    while (tableTypesRS.next()) {
      val tableType = tableTypesRS.getString("TABLE_TYPE")
      assertEquals(matchFunction(tableType), tableType)
      resultSetSize += 1
    }
    assertEquals(11, resultSetSize)
  }

  private def testGetColumns(dbmd: DatabaseMetaData, matchFunction: String => String): Unit = {
    var resultSetSize = 0
    val columnsRS = dbmd.getColumns(null, schema, table1, "cOL%")
    while (columnsRS.next()) {
      val columnName = columnsRS.getString("COLUMN_NAME")
      val tableName = columnsRS.getString("table_name")
      val schemaName = columnsRS.getString("TABLE_SCHEM")
      val typeName = columnsRS.getString("TYPE_NAME")
      val isAutoIncrement = columnsRS.getString("IS_AUTOINCREMENT")
      val isNullable = columnsRS.getString("IS_AUTOINCREMENT")

      logInfo(s"Column details - columnName:$columnName, tableName:$tableName," +
          s" schemaName:$schemaName, typeName:$typeName," +
          s" isAutoIncrement:$isAutoIncrement, isNullable:$isNullable ")

      assertEquals(matchFunction(table1), tableName)
      assertEquals(matchFunction(schema), schemaName)
      assertEquals(matchFunction(columnName), columnName)
      assertEquals(matchFunction(typeName), typeName)
      assertEquals(matchFunction(isAutoIncrement), isAutoIncrement)
      assertEquals(matchFunction(isNullable), isNullable)


      resultSetSize += 1
    }
    columnsRS.close()


    // result set should contain only two columns matching specified column name pattern,
    // schema name pattern and table name pattern
    assertEquals(2, resultSetSize)
  }

  private def testGetSchemas(dbmd: DatabaseMetaData, matchFunction: String => String): Unit = {
    var resultSetSize = 0
    val schemaRS = dbmd.getSchemas(null, schema)
    while (schemaRS.next) {
      val schemaName = schemaRS.getString("TABLE_SCHEM")
      // schemaName should be in lower case
      assertEquals(matchFunction(schema), schemaName)

      resultSetSize += 1
    }
    schemaRS.close()

    // result set should contain only one table which belongs to app schema as we have specified
    // schema pattern
    assertEquals(1, resultSetSize)
  }

  private def testGetTables(dbmd: DatabaseMetaData, matchFunction: String => String,
      checkShortTableType: Boolean = false): Unit = {
    // passing schema pattern in mixed case to ensure that schema pattern handling is
    // case-insensitive
    val tableRS = dbmd.getTables(null, schema, table1,
      Array[String]("TABLE"))

    var resultSetSize = 0
    while (tableRS.next) {
      // making sure that column data can be retrieved with lower case column name
      val tableName = tableRS.getString("table_name")
      val schemaName = tableRS.getString("TABLE_SCHEM")
      val tableType = tableRS.getString("TABLE_TYPE")
      val remarks = tableRS.getString("REMARKS")

      // tableName and schemaName should be in lower case
      assertEquals(matchFunction(table1), tableName)
      assertEquals(matchFunction(schema), schemaName)
      assertEquals(matchFunction(tableType), tableType)
      if (checkShortTableType) assertEquals(tableType, "table")
      else assertEquals(matchFunction(tableType), "ROW TABLE")
      assertEquals(matchFunction(remarks), remarks)

      resultSetSize += 1
    }
    tableRS.close()

    // result set should contain only one table matching the specified table name and schema
    // pattern
    assertEquals(1, resultSetSize)
  }

  private def cleanup(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(s"drop table if exists $schema.$table1")
    stmt.execute(s"drop table if exists $schema.$table2")
    stmt.execute(s"drop table if exists $table3")
    // executed with and without query-routing=false, hence explicit "restrict"
    stmt.execute(s"drop schema if exists $schema restrict")
    stmt.close()
    conn.close()
  }
}
