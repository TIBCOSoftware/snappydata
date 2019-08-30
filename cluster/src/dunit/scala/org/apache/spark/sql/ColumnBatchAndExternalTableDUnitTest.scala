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
package org.apache.spark.sql


import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Property
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableCallable}
import io.snappydata.util.TestUtils
import org.scalatest.Assertions

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}

case class TestRecord(col1: Int, col2: Int, col3: Int)

class ColumnBatchAndExternalTableDUnitTest(s: String) extends ClusterManagerTestBase(s)
    with Assertions with Logging {

  def _testColumnBatchSkipping(): Unit = {

    val snc = SnappyContext(sc)
    val ddlStr = "YearI INT NOT NULL," +
        "MonthI INT NOT NULL," +
        "DayOfMonth INT NOT NULL," +
        "DepTime INT," +
        "ArrDelay INT," +
        "UniqueCarrier CHAR(6) NOT NULL"

    // reduce the batch size to ensure that multiple are created

    snc.sql(s"create table if not exists airline ($ddlStr) " +
        s" using column options (Buckets '2', COLUMN_BATCH_SIZE '400')")

    import snc.implicits._

    val ds = snc.createDataset(sc.range(1, 101).map(i =>
      AirlineData(2015, 2, 15, 1002, i.toInt, "AA" + i)))
    ds.write.insertInto("airline")

    // ***Check for the case when all the column batches are scanned ****
    var previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_allColumnBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where  ArrDelay < 101 " +
          "group by UniqueCarrier order by arrivalDelay")

    df_allColumnBatchesScan.count()

    var executionIds =
      snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    var executionId = executionIds.head

    val (scanned1, skipped1) =
      findColumnBatchStats(df_allColumnBatchesScan, snc.snappySession, executionId)
    assert(skipped1 == 0, "All Column batches should have been scanned")
    assert(scanned1 > 0, "All Column batches should have been scanned")

    // ***Check for the case when all the column batches are skipped****
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_noColumnBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay > 101  " +
          "group by UniqueCarrier order by arrivalDelay")

    df_noColumnBatchesScan.count()

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned2, skipped2) =
      findColumnBatchStats(df_allColumnBatchesScan, snc.snappySession, executionId)
    assert(scanned2 == skipped2, "No Column batches should have been scanned")
    assert(skipped2 > 0, "No Column batches should have been scanned")

    // ***Check for the case when some of the column batches are scanned ****
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_someColumnBatchesScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where ArrDelay < 20  " +
          "group by UniqueCarrier order by arrivalDelay")

    df_someColumnBatchesScan.count()

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned3, skipped3) =
      findColumnBatchStats(df_allColumnBatchesScan, snc.snappySession, executionId)

    assert(skipped3 > 0, "Some Column batches should have been skipped")
    assert(scanned3 != skipped3, "Some Column batches should have been skipped - comparison")

    // check for StartsWith predicate with MAX/MIN handling

    // first all batches chosen
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_allColumnBatchesLikeScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where UniqueCarrier like 'AA%' " +
          "group by UniqueCarrier order by arrivalDelay")

    var count = df_allColumnBatchesLikeScan.count()
    assert(count == 100, s"Unexpected count = $count, expected 100")

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned4, skipped4) =
      findColumnBatchStats(df_allColumnBatchesLikeScan, snc.snappySession, executionId)

    assert(skipped4 == 0, "No Column batches should have been skipped")
    assert(scanned4 > 0, "All Column batches should have been scanned")

    // next some batches skipped
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_someColumnBatchesLikeScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where UniqueCarrier like 'AA1%' " +
          "group by UniqueCarrier order by arrivalDelay")

    count = df_someColumnBatchesLikeScan.count()
    assert(count == 12, s"Unexpected count = $count, expected 12")

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned5, skipped5) =
      findColumnBatchStats(df_someColumnBatchesLikeScan, snc.snappySession, executionId)

    assert(skipped5 > 0, "Some Column batches should have been skipped")
    assert(scanned5 != skipped5, "Some Column batches should have been skipped - comparison")

    // last all batches skipped
    previousExecutionIds = snc.sharedState.listener.executionIdToData.keySet

    val df_noColumnBatchesLikeScan = snc.sql(
      "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier " +
          "from AIRLINE where UniqueCarrier like 'AA0%' " +
          "group by UniqueCarrier order by arrivalDelay")

    count = df_noColumnBatchesLikeScan.count()
    assert(count == 0, s"Unexpected count = $count, expected 0")

    executionIds =
        snc.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)

    executionId = executionIds.head

    val (scanned6, skipped6) =
      findColumnBatchStats(df_noColumnBatchesLikeScan, snc.snappySession, executionId)

    assert(scanned6 == skipped6, "No Column batches should have been returned")
    assert(skipped6 > 0, "No Column batches should have been returned")
  }

  private def findColumnBatchStats(df: DataFrame,
      sc: SnappySession, executionId: Long): (Long, Long) = {

    val metricValues = sc.sharedState.listener.getExecutionMetrics(executionId)
    val a = (sc.sharedState.listener.getRunningExecutions ++
        sc.sharedState.listener.getCompletedExecutions).filter(x => {
      x.executionId == executionId
    })
    val seenid = a.head.accumulatorMetrics.filter(x => {
      x._2.name == "column batches seen"
    }).head._1
    val skippedid = a.head.accumulatorMetrics.filter(x => {
      x._2.name == "column batches skipped by the predicate"
    }).head._1

    (metricValues.filter(_._1 == seenid).head._2.toInt,
        metricValues.filter(_._1 == skippedid).head._2.toInt)
  }


  def testCreateColumnTablesFromOtherTables(): Unit = {
    val tempRowTableProps = "BUCKETS '16', PARTITION_BY 'COL2'"
    executeTestWithOptions(Map("BUCKETS" -> "8", "PARTITION_BY" -> "COL1", "REDUNDANCY" -> "1"),
      Map.empty, tempRowTableProps)
    executeTestWithOptions(Map.empty, Map("BUCKETS" -> "16"), tempRowTableProps,
      "BUCKETS '8', PARTITION_BY 'COL1', REDUNDANCY '1'")
  }

  def executeTestWithOptions(rowTableOptions: Map[String, String] = Map.empty[String, String],
      colTableOptions: Map[String, String] = Map.empty[String, String],
      tempRowTableOptions: String = "", tempColTableOptions: String = ""): Unit = {

    val snc = SnappyContext(sc)
    val rowTable = "rowTable"
    val colTable = "colTable"


    snc.sql("DROP TABLE IF EXISTS " + rowTable)
    snc.sql("DROP TABLE IF EXISTS " + colTable)
    Property.ColumnBatchSize.set(snc.sessionState.conf, "30k")
    val rdd = sc.parallelize(
      (1 to 113999).map(i => TestRecord(i, i + 1, i + 2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(rowTable, "row", dataDF.schema, rowTableOptions)
    dataDF.write.insertInto(rowTable)

    snc.createTable(colTable, "column", dataDF.schema, colTableOptions)
    dataDF.write.format("column").mode(SaveMode.Append).options(colTableOptions)
        .saveAsTable(colTable)

    val tempRowTableName = "testRowTable"
    val tempColTableName = "testcolTable"


    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql(s"CREATE TABLE " + tempRowTableName + s" using row options($tempRowTableOptions)  AS" +
        s" (SELECT col1 ,col2  FROM " + rowTable + ")")
    val testResults1 = snc.sql("SELECT * FROM " + tempRowTableName).collect()
    assert(testResults1.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults1.length}")


    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql("CREATE TABLE " + tempRowTableName + s" using row options($tempRowTableOptions) AS " +
        s"(SELECT col1 ,col2  FROM " + colTable + ")")
    val testResults2 = snc.sql("SELECT * FROM " + tempRowTableName).collect()
    assert(testResults2.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults2.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT col1 ,col2 FROM " + tempRowTableName + ")")

    val testResults3 = snc.sql("SELECT * FROM " + tempColTableName).collect()
    assert(testResults3.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults3.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT col1 ,col2 FROM " + colTable + ")")

    val testResults4 = snc.sql("SELECT * FROM " + tempColTableName).collect()
    assert(testResults4.length == 113999, s"Expected row count is 113999 while actual count is" +
        s"${testResults4.length}")


    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT t1.col1 ,t1.col2 FROM " + colTable + " t1," + rowTable +
        " t2 where t1.col1=t2.col2)")

    // Expected count will be 113998 as first row will not match
    val testResults5 = snc.sql("SELECT * FROM " + tempColTableName).collect()
    assert(testResults5.length == 113998, s"Expected row count is 113998 while actual count is" +
        s"${testResults5.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)

    snc.sql("DROP TABLE IF EXISTS " + rowTable)
    snc.sql("DROP TABLE IF EXISTS " + colTable)
  }

  def testSessionConfigForCPUsPerTask(): Unit = {
    val netPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort)
    val conn = getANetConnection(netPort)
    val stmt = conn.createStatement()

    val sparkCores = TestUtils.defaultCores * 3 // three executors
    val usableHeap = vm1.invoke(new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        Long.box(Misc.getGemFireCache.getResourceManager.getHeapMonitor
            .getThresholds.getCriticalThresholdBytes)
      }
    }).asInstanceOf[java.lang.Long].longValue()
    val implicitCpusToTasks = math.max(1, math.ceil(math.max(
      128.0 * 1024.0 * 1024.0 * TestUtils.defaultCores / usableHeap,
      TestUtils.defaultCores.toDouble / Runtime.getRuntime.availableProcessors())).toInt)
    val buckets = math.max(128, sparkCores)
    val targetDataFile = "airlineParquet"

    // initialize data
    val srcDataFile = getClass.getResource("/2015-trimmed.parquet").getPath
    stmt.execute(s"create table airline_staging using parquet options (path '$srcDataFile')")
    stmt.execute(s"create table airline using column options (buckets '$buckets') " +
        "as select * from airline_staging")
    stmt.execute("drop table airline_staging")
    // create parquet file with large number of partitions though total size is small
    stmt.execute(s"create table airline_staging using parquet options (path '$targetDataFile') " +
        s"as select * from airline")

    var maxTasksStarted = 0
    var activeTasks = 0

    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
        if (taskStart.taskInfo ne null) {
          activeTasks += 1
          if (activeTasks > maxTasksStarted) {
            maxTasksStarted = activeTasks
          }
        }
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
        if ((taskEnd.taskInfo ne null) && taskEnd.stageAttemptId != -1) {
          activeTasks -= 1
        }
      }
    }
    sc.listenerBus.addListener(listener)

    // ---- Check explicit spark.task.cpus setting takes effect in embedded mode -----

    // force linking of buckets to partitions to avoid de-linking complications
    stmt.execute("set snappydata.linkPartitionsToBuckets = true")
    // first confirm that number of tasks created at a time are same as sparkCores by default
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline")
    // should not deviate much though in rare cases few tasks can start/end slightly out of order
    assert(maxTasksStarted < sparkCores * 2)
    assert(maxTasksStarted > sparkCores / 2)
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline")
    assert(maxTasksStarted < sparkCores * 2)
    assert(maxTasksStarted > sparkCores / 2)

    // now check that max tasks are reduced with the session setting
    stmt.execute("set spark.task.cpus = 2")
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline")
    assert(maxTasksStarted < sparkCores)
    assert(maxTasksStarted > sparkCores / 4)

    // ---- Check implicit spark.task.cpus get set for file scans/inserts ----
    logInfo(s"Expected implicit spark.task.cpus = $implicitCpusToTasks")
    stmt.execute("set spark.task.cpus")
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline")
    assert(maxTasksStarted < sparkCores * 2)
    assert(maxTasksStarted > sparkCores / 2)

    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline_staging")
    assert(maxTasksStarted < sparkCores * 2 / implicitCpusToTasks)
    assert(maxTasksStarted > sparkCores / (2 * implicitCpusToTasks))
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline_staging")
    assert(maxTasksStarted < sparkCores * 2 / implicitCpusToTasks)
    assert(maxTasksStarted > sparkCores / (2 * implicitCpusToTasks))

    // ---- Check explicit spark.task.cpus overrides implicit spark.task.cpus ----
    stmt.execute("set spark.task.cpus = 1")
    maxTasksStarted = 0
    activeTasks = 0
    stmt.execute("select avg(depDelay) from airline_staging")
    assert(maxTasksStarted < sparkCores * 2)
    assert(maxTasksStarted > sparkCores / 2)

    stmt.execute("drop table airline_staging")
    stmt.execute("drop table airline")

    sc.listenerBus.removeListener(listener)

    stmt.close()
    conn.close()
  }
}

case class AirlineData(year: Int, month: Int, dayOfMonth: Int,
    depTime: Int, arrDelay: Int, carrier: String)
