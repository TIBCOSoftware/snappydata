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
import org.apache.spark.sql.execution.metric.SQLMetrics


class CachedBatchScanDUnitTest(s: String) extends ClusterManagerTestBase(s){

  def testCachedBatchSkipping(): Unit = {

    val snc = SnappyContext(sc)
    val ddlStr = "YearI INT NOT NULL," +
        "MonthI INT NOT NULL," +
        "DayOfMonth INT NOT NULL," +
        "DepTime INT," +
        "ArrDelay INT," +
        "UniqueCarrier CHAR(6) NOT NULL"

    snc.sql(s"create table if not exists airline ($ddlStr) " +
          s" using column options (Buckets '2')").collect()

    for (i <- 1 to 100 ) {
      snc.sql(s"insert into airline values(2015, 2, 15, 1002, $i, 'AA')")
    }

    // ***Check for the case when all the cached batches are scanned ****
    var previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    var df_allCachedBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where  ArrDelay < 101 " +
          "group by UniqueCarrier order by arrivalDelay")

    df_allCachedBatchesScan.count

    var executionIds =
      snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    var executionId = executionIds.head

    val (scanned1, skipped1) =
      findCachedBatchStats(df_allCachedBatchesScan, snc.snappySession, executionId)
    assert(skipped1 == 0, "All Cached batches should have been scanned")
    assert(scanned1 > 0, "All Cached batches should have been scanned")

    // ***Check for the case when all the cached batches are skipped****
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_noCachedBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay > 101  " +
          "group by UniqueCarrier order by arrivalDelay")

    df_noCachedBatchesScan.count

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned2, skipped2) =
      findCachedBatchStats(df_allCachedBatchesScan, snc.snappySession, executionId)
    assert(scanned2 == skipped2, "No Cached batches should have been scanned")
    assert(skipped2 > 0, "No Cached batches should have been scanned")

    // ***Check for the case when some of the cached batches are scanned ****
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_someCachedBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay < 20  " +
          "group by UniqueCarrier order by arrivalDelay")

    df_someCachedBatchesScan.count

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned3, skipped3) =
      findCachedBatchStats(df_allCachedBatchesScan, snc.snappySession, executionId)

    assert(skipped3 > 0, "Some Cached batches should have been scanned")
    assert(scanned3 != skipped3, "Some Cached batches should have been scanned - comparison")
  }

  private def findCachedBatchStats(df: DataFrame,
      sc: SnappySession, executionId: Long): (Long, Long) = {

    val metricValues = sc.sharedState.listener.getExecutionMetrics(executionId)
    var a = (sc.sharedState.listener.getRunningExecutions ++
        sc.sharedState.listener.getCompletedExecutions ).filter(x => {
      x.executionId == executionId
    })
    val seenid = a.head.accumulatorMetrics.filter(x =>
    {x._2.name == "cached batches seen"}).head._1
    val skippedid = a.head.accumulatorMetrics.filter(x =>
    {x._2.name == "cached batches skipped by the predicate"}).head._1

    (metricValues.filter(_._1 == seenid).head._2.toInt,
        metricValues.filter(_._1 == skippedid).head._2.toInt)
  }


}
