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

import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedDatabaseMetaData.METADATACASE_UPPER_PROP
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.junit.Assert.assertEquals

import org.apache.spark.Logging

class JDBCMetadataCaseDUnitTest(s: String) extends ClusterManagerTestBase(s)
    with Logging {

  val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort

  private val table = "t1"

  def testLowercaseMetadata(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("drop table if exists " + table)
        stmt.execute("create table " + table + "(id integer, fs string) using column")
      } finally {
        stmt.close()
      }
      val dbmd = conn.getMetaData
      val tableRS = dbmd.getTables(null, "app", null,
        Array[String]("TABLE"))

      while (tableRS.next) {
        // making sure that column data can be retrieved with lower case column name
        val tableName = tableRS.getString("table_name")
        val schemaName = tableRS.getString("TABLE_SCHEM")
        // tableName and schemaName should be in lower case
        assertEquals(tableName.toLowerCase, tableName)
        assertEquals(schemaName.toLowerCase, schemaName)
      }

      val schemaRS = dbmd.getSchemas(null, "APP")
      while (schemaRS.next) {
        val schemaName = schemaRS.getString("TABLE_SCHEM")
        // schemaName should be in lower case
        assertEquals(schemaName.toLowerCase, schemaName)
      }
    } finally {
      val stmt = conn.createStatement()
      try {
        stmt.execute("drop table if exists " + table)
      } finally {
        stmt.close()
      }
      conn.close()
    }
  }

  // For internal connections we need to continue using uper case metadata as some internal code
  // is written with upper case assumption. We don't want to affect that code. Internal connection
  // is identified based on status of query routing as for internal connections query routing is
  // always disabled.
  def testLowercaseMetadata_internalConnections(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1, disableQueryRouting = true)
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("drop table if exists " + table)
        stmt.execute("create table " + table + "(id integer, fs string)")
      } finally {
        stmt.close()
      }
      val dbmd = conn.getMetaData
      val rs = dbmd.getTables(null.asInstanceOf[String], "app", null.asInstanceOf[String],
        Array[String]("TABLE"))

      while (rs.next) {
        val tableName = rs.getString("table_name")
        val schemaName = rs.getString("TABLE_SCHEM")
        // tableName and schemaName should be in upper case
        assertEquals(tableName.toUpperCase, tableName)
        assertEquals(schemaName.toUpperCase, schemaName)
      }

      val schemaRS = dbmd.getSchemas(null, "APP")
      while (schemaRS.next) {
        val schemaName = schemaRS.getString("TABLE_SCHEM")
        // schemaName should be in upper case
        assertEquals(schemaName.toUpperCase(), schemaName)
      }
    } finally {
      val stmt = conn.createStatement()
      try {
        stmt.execute("drop table if exists " + table)
      } finally {
        stmt.close()
      }
      conn.close()
    }
  }
}
