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

import io.snappydata.Property.PlanCaching
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession

class PlanCachingTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll{

  var planCaching : Boolean = false

  override def beforeAll(): Unit = {
    planCaching = PlanCaching.get(snc.sessionState.conf)
    PlanCaching.set(snc.sessionState.conf, true)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    PlanCaching.set(snc.sessionState.conf, planCaching)
    snc.sql("drop table if exists tcol")
    super.afterAll()
  }

  test("SNAP-2716 - Wrong column names in results due to aliases being ignored in plan caching"){
    val cacheMap = SnappySession.getPlanCache.asMap()
    cacheMap.clear()
    snc.sql("create table tcol(col1 int not null)")
    snc.sql("select col1, case when col1 < 10 then 'small' else 'big' " +
        "end as value_category from tcol")
    val dataFrame = snc.sql("select col1, case when col1 < 10 then 'small' else 'big' " +
        "end as tmp_value_category from tcol;")

    val columns = dataFrame.schema.fields.map(_.name)

    assert(columns sameElements Array("col1", "tmp_value_category"))

    assert(cacheMap.size() == 2)

    snc.sql("select col1, case when col1 < 11 then 'small' else 'big' " +
        "end as tmp_value_category from tcol;")

    assert(cacheMap.size() == 2)
  }
}
