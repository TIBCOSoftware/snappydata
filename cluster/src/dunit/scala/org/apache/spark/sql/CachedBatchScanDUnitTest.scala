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
package org.apache.spark.sql

import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.sql.execution.columnar.ColumnTableScan


class CachedBatchScanDUnitTest(s: String) extends ClusterManagerTestBase(s){

  def testCachedBatchSkipping(): Unit = {

    val snContext = SnappyContext(sc)
    val ddlStr = "YearI INT NOT NULL," +
        "MonthI INT NOT NULL," +
        "DayOfMonth INT NOT NULL," +
        "DepTime INT," +
        "ArrDelay INT," +
        "UniqueCarrier CHAR(6) NOT NULL"

    snContext.sql(s"create table if not exists airline ($ddlStr) " +
          s" using column options (Buckets '2')").collect()

    for (i <- 1 to 1000 ) {
      snContext.sql(s"insert into airline values(2015, 2, 15, 1002, $i, 'AA')")
    }

    val df_allCachedBatchesScan = snContext.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where  ArrDelay < 1001 " +
          "group by UniqueCarrier order by arrivalDelay")

    val df_noCachedBatchesScan = snContext.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay > 1001  " +
          "group by UniqueCarrier order by arrivalDelay")

    val df_someCachedBatchesScan = snContext.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay < 200  " +
          "group by UniqueCarrier order by arrivalDelay")

    df_allCachedBatchesScan.count
    df_noCachedBatchesScan.count
    df_someCachedBatchesScan.count

    val (scanned, skipped) = findCachedBatchStats(df_allCachedBatchesScan)
    assert(skipped == 0, "All Cached batches should have been scanned")

    val (scanned1, skipped1) = findCachedBatchStats(df_allCachedBatchesScan)
    assert(scanned1 == skipped1, "No Cached batches should have been scanned")

    val (scanned2, skipped2) = findCachedBatchStats(df_someCachedBatchesScan)
    assert(skipped2 > 0, "Some Cached batches should have been scanned")
    assert(scanned2 != skipped2, "Some Cached batches should have been scanned - comparison")

  }
  private def findCachedBatchStats(df: DataFrame): (Long, Long) = {
    val physical = df.queryExecution.sparkPlan
    val operator = physical.find(_.isInstanceOf[ColumnTableScan]).get
    (operator.asInstanceOf[ColumnTableScan].metrics("cachedBatchesSeen").value,
        operator.asInstanceOf[ColumnTableScan].metrics("cachedBatchesSkipped").value)
  }

}
