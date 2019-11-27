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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class CatalogVersionTest extends SnappyFunSuite with BeforeAndAfter with BeforeAndAfterAll {
  val testTable = "catalog_version_test_table"


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  before {
    snc.sql(s"drop table if exists $testTable")
  }

  after {
    snc.sql(s"drop table if exists $testTable")
  }

  test("catalog version update for 'create table if not exist ...' with primary keys") {
    val initialVersion = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (id int, name string, primary key (id)) using row""")
    val v1 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Version is incremented by one as when query contains primary keys, an extra alter table is
    // performed after actually creating table.
    // See org.apache.spark.sql.SnappySession.updatePrimaryKeyDetails
    assertResult(initialVersion + 2)(v1)

    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (id int, name string, primary key (id)) using row""")
    val v2 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Another execution of query should not update version as the table is already created
    // earlier.
    assertResult(initialVersion + 2)(v2)
  }


  test("catalog version update for 'create table if not exist ...' without primary keys") {
    val initialVersion = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (id int, name string) using row""")
    val v1 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    assertResult(initialVersion + 1)(v1)

    snc.sql(s"""CREATE TABLE IF NOT EXISTS $testTable
     (id int, name string) using row""")
    val v2 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Another execution of query should not update version as the table is already created
    // earlier.
    assertResult(initialVersion + 1)(v2)

    snc.sql(s"insert into $testTable values(1,'name1')")
    snc.sql(s"update $testTable set name='xyz' where id = 1")
    snc.sql(s"delete from $testTable where id = 1")
    val v3 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // DML statement should not update catalog version
    assertResult(initialVersion + 1)(v3)

    snc.sql(s"drop table $testTable")
    val v4 = GemFireXDUtils.getGfxdAdvisor.getMyProfile.getCatalogSchemaVersion
    // Drop table should increment catalog version
    assertResult(initialVersion + 2)(v4)



  }
}
