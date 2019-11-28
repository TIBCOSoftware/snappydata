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

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter

class CatalogVersionTest extends SnappyFunSuite with BeforeAndAfter {
  val testTable = "catalog_version_test_table"
  val testView = "catalog_version_test_view"

  before {
    snc.sql(s"DROP TABLE IF EXISTS $testTable")
    snc.sql(s"DROP VIEW IF EXISTS $testView")
  }

  after {
    snc.sql(s"DROP TABLE IF EXISTS $testTable")
    snc.sql(s"DROP VIEW IF EXISTS $testView")
  }

  test("catalog version update") {
    val initialVersion = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    snc.sql(
      s"CREATE TABLE IF NOT EXISTS $testTable (ID INT, NAME STRING)")
    val v1 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    assertResult(initialVersion + 1)(v1)

    snc.sql(s"CREATE TABLE IF NOT EXISTS $testTable (ID INT, NAME STRING)")
    val v2 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Another execution of query should not update version as the table is already created
    // earlier.
    assertResult(initialVersion + 1)(v2)

    snc.sql(s"INSERT INTO $testTable VALUES(1,'NAME1')")
    snc.sql(s"UPDATE $testTable SET NAME='XYZ' WHERE ID = 1")
    snc.sql(s"DELETE FROM $testTable WHERE ID = 1")
    val v3 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // DML statement should not update catalog version
    assertResult(initialVersion + 1)(v3)

    snc.sql(s"CREATE OR REPLACE VIEW $testView AS SELECT * FROM $testTable")
    val v4 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // View creation should increment catalog version
    assertResult(initialVersion + 2)(v4)

    snc.sql(s"CREATE OR REPLACE VIEW $testView AS SELECT id FROM $testTable")
    val v5 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Replacing the same view should increment catalog version
    assertResult(initialVersion + 3)(v5)

    snc.sql(s"DROP VIEW $testView")
    val v6 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Drop view should increment catalog version
    assertResult(initialVersion + 4)(v6)

    snc.sql(s"DROP TABLE $testTable")
    val v7 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Drop table should increment catalog version
    assertResult(initialVersion + 5)(v7)
  }

  test("catalog version update for 'create table if not exist ...' with primary keys") {
    val initialVersion = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (ID INT, NAME STRING, PRIMARY KEY (ID)) USING ROW""")
    val v1 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Version is incremented by one as when query contains primary keys, an extra alter table is
    // performed after actually creating table.
    // See org.apache.spark.sql.SnappySession.updatePrimaryKeyDetails
    assertResult(initialVersion + 2)(v1)

    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (ID INT, NAME STRING, PRIMARY KEY (ID)) USING ROW""")
    val v2 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Another execution of query should not update version as the table is already created
    // earlier.
    assertResult(initialVersion + 2)(v2)
  }
}
