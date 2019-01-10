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
package org.apache.spark.sql.store

import java.sql.DriverManager

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite

/**
 * Same as core [[MetadataTest]] but using JDBC connection.
 */
class SQLMetadataTest extends SnappyFunSuite {

  private var netPort = 0

  override def beforeAll(): Unit = {
    super.beforeAll()
    assert(this.snc !== null)
    // start a local network server
    netPort = TestUtil.startNetserverAndReturnPort()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestUtil.stopNetServer()
  }

  test("SYS tables/VTIs") {
    val session = this.snc.snappySession
    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      val stmt = conn.createStatement()
      MetadataTest.testSYSTablesAndVTIs(SnappyFunSuite.resultSetToDataset(session, stmt),
        netServers = Seq(s"localhost/127.0.0.1[$netPort]"))
      stmt.close()
    } finally {
      conn.close()
    }
  }

  test("DESCRIBE, SHOW and EXPLAIN") {
    val session = this.snc.snappySession
    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      val stmt = conn.createStatement()
      MetadataTest.testDescribeShowAndExplain(SnappyFunSuite.resultSetToDataset(session, stmt),
        usingJDBC = true)
      stmt.close()
    } finally {
      conn.close()
    }
  }

  test("DSID joins with SYS tables") {
    val session = this.snc.snappySession
    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      val stmt = conn.createStatement()
      MetadataTest.testDSIDWithSYSTables(SnappyFunSuite.resultSetToDataset(session, stmt),
        Seq(s"localhost/127.0.0.1[$netPort]"))
      stmt.close()
    } finally {
      conn.close()
    }
  }
}
