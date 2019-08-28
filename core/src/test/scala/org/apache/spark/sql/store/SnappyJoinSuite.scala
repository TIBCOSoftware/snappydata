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

import io.snappydata.SnappyFunSuite
import io.snappydata.core.{RefData, TestData2}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.{CartesianProductExec, HashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.{PartitionedPhysicalScan, QueryExecution, RowDataSourceScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SnappyContext, SnappySession}

case class TestDatak(key1: Int, value: String, ref: Int)

class SnappyJoinSuite extends SnappyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    snc.conf.clear()
    super.afterAll()
  }

  val props = Map.empty[String, String]

  test("Replicated table join with PR Table") {

    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"$i")))
    snc.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String)")

    refDf.write.insertInto("RR_TABLE")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE")

    snc.sql("CREATE TABLE PR_TABLE(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row " +
        "options (" +
        "PARTITION_BY 'OrderRef')")

    val dimension = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE")

    val countDf = snc.sql("select * from PR_TABLE P JOIN RR_TABLE R " +
        "ON P.ORDERREF = R.ORDERREF")

    val qe = new QueryExecution(snc.snappySession, countDf.logicalPlan)
    val plan = qe.executedPlan
    val lj = plan collectFirst {
      case lc: HashJoinExec => lc
    }
    lj.getOrElse(sys.error(s"Can't find Local join in a 1 partitioned relation"))

    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    logInfo("Time taken = " + (t2 - t1))

    val projectDF = snc.sql("select ORDERID, P.DESCRIPTION, R.DESCRIPTION " +
        "from PR_TABLE P JOIN RR_TABLE R ON P.ORDERREF = R.ORDERREF")
    assert(projectDF.columns.length === 3)

    val sumDF = snc.sql("select SUM(ORDERID)from PR_TABLE P JOIN RR_TABLE R " +
        "ON P.ORDERREF = R.ORDERREF")
    assert(sumDF.collect()(0).getLong(0) === ((1000 * 1001) / 2))
  }

  test("Replicated table join with Replicated Table") {

    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"$i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE1")

    snc.sql("CREATE TABLE RR_TABLE1(OrderRef INT NOT NULL, " +
        "description String) USING row options()")

    refDf.write.insertInto("RR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS RR_TABLE2")

    snc.sql("CREATE TABLE RR_TABLE2(OrderId INT NOT NULL," +
        "description String, OrderRef INT) USING row options()")

    val dimension = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension)
    dimensionDf.write.insertInto("RR_TABLE2")

    val countDf = snc.sql(
      "select * from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    val qe = new QueryExecution(snc.snappySession, countDf.logicalPlan)

    val lj = qe.executedPlan collectFirst {
      case lc: HashJoinExec => lc
    }
    lj.getOrElse(sys.error(s"Can't find Local join in a 1 partitioned relation"))

    val t1 = System.currentTimeMillis()
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
    val t2 = System.currentTimeMillis()
    logInfo("Time taken = " + (t2 - t1))

    val projectDF = snc.sql("select R.ORDERID, P.DESCRIPTION, R.DESCRIPTION " +
        "from RR_TABLE1 P JOIN RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(projectDF.columns.length === 3)

    val sumDF = snc.sql("select SUM(R.ORDERID)from RR_TABLE1 P JOIN " +
        "RR_TABLE2 R ON P.ORDERREF = R.ORDERREF")
    assert(sumDF.collect()(0).getLong(0) === ((1000 * 1001) / 2))

  }

  test("Join multiple replicated tables followed by a partitioned table") {

    val rdd = sc.parallelize((1 to 5).map(i => RefData(i, s"$i")))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE1")

    snc.sql("CREATE TABLE RR_TABLE1(OrderRef INT NOT NULL, " +
        "description String) USING row options()")

    refDf.write.insertInto("RR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS RR_TABLE2")

    snc.sql("CREATE TABLE RR_TABLE2(OrderId INT NOT NULL," +
        "description String, OrderRef INT) USING row options()")

    val dimension = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))
    val dimensionDf = snc.createDataFrame(dimension)
    dimensionDf.write.insertInto("RR_TABLE2")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE")

    snc.sql("CREATE TABLE PR_TABLE(OrderId INT,description String, " +
        "OrderRef INT) USING column " +
        "options (" +
        "PARTITION_BY 'OrderId')")

    val dimension_pr = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))
    val dimensionDf_pr = snc.createDataFrame(dimension_pr)
    dimensionDf_pr.write.insertInto("PR_TABLE")

    val countDf = snc.sql("select * from RR_TABLE1 R1, RR_TABLE2 R2, " +
        "PR_TABLE P where R1.ORDERREF = R2.ORDERREF AND P.OrderId = R2.OrderId")
    val qe = new QueryExecution(snc.snappySession, countDf.logicalPlan)

    qe.executedPlan foreach {
      case SortMergeJoinExec(_, _, _, _, _, _) =>
        throw new Exception("This should have been a local join")
      case _ =>
    }
    assert(countDf.count() === 1000) // Make sure aggregation is working with local join
  }


  test("Check shuffle in operations with partition pruning"){
    val t1 = "t1"
    val t2 = "t2"

    snc.setConf[Long](SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    snc.sql(s"create table $t1 (ol_1_int_id  integer," +
        s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
        "options( partition_by 'ol_1_int_id', buckets '16')")
    snc.sql(s"create table $t2 (ol_1_int_id  integer," +
        s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
        "options( partition_by 'ol_1_int_id', buckets '16')")

    var df = snc.sql(s"select sum(ol_1_int2_id)  from $t1 where ol_1_int_id=1")
    checkForShuffle(df.logicalPlan, snc , shuffleExpected = false)

    // with limit
    df = snc.sql(s"select sum(ol_1_int2_id)  from $t1 where ol_1_int_id=1 limit 1")
    checkForShuffle(df.logicalPlan, snc , shuffleExpected = false)

    df = snc.sql(s"update $t1 set ol_1_str_id = '3' where ol_1_int_id in (" +
        s"select ol_1_int_id from $t2 where $t2.ol_1_int_id=1)")

    checkForShuffle(df.logicalPlan, snc , shuffleExpected = false)

    snc.dropTable("t1");
    snc.dropTable("t2");

  }

  /**
   * This method is very specific to  PartitionedDataSourceScan and
   * snappy join improvements
   */
  def checkForShuffle(plan: LogicalPlan, snc: SnappyContext,
      shuffleExpected: Boolean): Unit = {

    val qe = new QueryExecution(snc.snappySession, plan)
    // logInfo(qe.executedPlan)
    val lj = qe.executedPlan collect {
      case ex: Exchange => ex
    }
    if (shuffleExpected) {
      if (lj.isEmpty) sys.error(s"Shuffle Expected , but was not found")
    } else {
      lj.foreach(a => a.child.collect {
        // this means no Exhange should have child as PartitionedPhysicalRDD
        case p: PartitionedPhysicalScan => sys.error(
          s"Did not expect exchange with partitioned scan with same partitions")
        case p: RowDataSourceScanExec => sys.error(
          s"Did not expect RowDataSourceScanExec with PartitionedDataSourceScan")
        case _ => // do nothing, may be some other Exchange and not with scan
      })
    }
  }

  def checkForBroadcast(df: DataFrame, session: SnappySession,
      broadcastExpected: Boolean): Unit = {
    val lj = df.queryExecution.executedPlan collect {
      case ex: BroadcastExchangeExec => ex
    }
    if (broadcastExpected) {
      if (lj.isEmpty) sys.error("Broadcast expected but was not found")
    } else {
      if (lj.nonEmpty) sys.error(s"Broadcast not expected but was found: $lj")
    }
  }

  test("Row PR table join with PR Table") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    snc.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE1")

    snc.sql("CREATE TABLE PR_TABLE1(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row " +
        "options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")

    snc.sql("CREATE TABLE PR_TABLE2(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE1')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE2")

    partitionToPartitionJoinAssertions(snc, "PR_TABLE1", "PR_TABLE2")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")
  }

  test("Row PR table join with PR Table without colocation") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE22")

    snc.sql("CREATE TABLE PR_TABLE22(OrderId INT, description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    refDf.write.insertInto("PR_TABLE22")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE23")

    snc.sql("CREATE TABLE PR_TABLE23(OrderId INT ,description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE23")

    val excatJoinKeys = snc.sql(s"select P.OrderRef, P.description from " +
        s"PR_TABLE22 P JOIN PR_TABLE23 R ON P.OrderId = R.OrderId AND " +
        s"P.OrderRef = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = false)

    assert(excatJoinKeys.count() === 500)
  }

  test("Column PR table join with PR Table") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE3")

    snc.sql("CREATE TABLE PR_TABLE3(OrderId INT, description String, " +
        "OrderRef INT) USING column " +
        "options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    refDf.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE3")

    val countdf = snc.sql("select * from PR_TABLE3")
    assert(countdf.count() == 1000)

    snc.sql("DROP TABLE IF EXISTS PR_TABLE4")

    snc.sql("CREATE TABLE PR_TABLE4(OrderId INT ,description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE3')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE4")
    val countdf1 = snc.sql("select * from PR_TABLE4")
    assert(countdf1.count() == 1000)
    partitionToPartitionJoinAssertions(snc, "PR_TABLE3", "PR_TABLE4")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE4")
  }

  test("Column PR table join with PR Table without colocation") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE20")

    snc.sql("CREATE TABLE PR_TABLE20(OrderId INT, description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    refDf.write.insertInto("PR_TABLE20")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE21")

    snc.sql("CREATE TABLE PR_TABLE21(OrderId INT ,description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE21")

    val excatJoinKeys = snc.sql(s"select P.OrderRef, P.description from " +
        s"PR_TABLE20 P JOIN PR_TABLE21 R ON P.OrderId = R.OrderId AND " +
        s"P.OrderRef = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = false)

    assert(excatJoinKeys.count() === 500)
  }

  test("Column PR table join with Non user mentioned PR Table") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE5")

    snc.sql("CREATE TABLE PR_TABLE5(OrderId INT, description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE5")

    val countdf = snc.sql("select * from PR_TABLE5")
    assert(countdf.count() == 1000)

    snc.sql("DROP TABLE IF EXISTS PR_TABLE6")

    snc.sql("CREATE TABLE PR_TABLE6(OrderId INT ,description String, " +
        "OrderRef INT) USING column options()")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE6")
    val countdf1 = snc.sql("select * from PR_TABLE6")
    assert(countdf1.count() == 1000)
    val excatJoinKeys = snc.sql(s"select P.OrderRef, P.description from " +
        s"PR_TABLE5 P JOIN PR_TABLE6 R ON P.OrderId = R.OrderId AND " +
        s"P.OrderRef = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = true)
  }

  test("Column PR table join with Row PR Table") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE7")

    snc.sql("CREATE TABLE PR_TABLE7(OrderId INT, description String, " +
        "OrderRef INT) USING row " +
        "options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE7")

    val countdf = snc.sql("select * from PR_TABLE7")
    assert(countdf.count() == 1000)

    snc.sql("DROP TABLE IF EXISTS PR_TABLE8")

    snc.sql("CREATE TABLE PR_TABLE8(OrderId INT ,description String, " +
        "OrderRef INT) USING column " +
        "options (" +
        "PARTITION_BY 'OrderId, OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE7')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE8")
    val countdf1 = snc.sql("select * from PR_TABLE8")
    assert(countdf1.count() == 1000)
    val excatJoinKeys = snc.sql(s"select P.ORDERREF, P.DESCRIPTION from " +
        s"PR_TABLE7 P JOIN PR_TABLE8 R ON P.ORDERID = R.OrderId AND " +
        s"P.ORDERREF = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = false)
    assert(excatJoinKeys.count() === 500)

    snc.sql("DROP TABLE IF EXISTS PR_TABLE8")
  }

  test("Row PR table join with PR Table with unequal partitions") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val snc = SnappyContext(sc)
    snc.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE9")

    snc.sql("CREATE TABLE PR_TABLE9(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE9")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE10")

    snc.sql("CREATE TABLE PR_TABLE10(OrderId INT NOT NULL,description String," +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "BUCKETS '213')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.format("row").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE10")
    val excatJoinKeys = snc.sql(s"select P.ORDERREF, P.DESCRIPTION from " +
        s"PR_TABLE9 P JOIN PR_TABLE10 R ON P.ORDERID = R.OrderId AND " +
        s"P.ORDERREF = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = true)
    assert(excatJoinKeys.count() === 500)
  }

  test("More than two table joins") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE11")

    snc.sql("CREATE TABLE PR_TABLE11(OrderId INT NOT NULL,description String," +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE11")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE12")

    snc.sql("CREATE TABLE PR_TABLE12(OrderId INT NOT NULL,description String," +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf2 = snc.createDataFrame(dimension2)
    dimensionDf2.write.insertInto("PR_TABLE12")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE13")

    snc.sql("CREATE TABLE PR_TABLE13(OrderId INT NOT NULL,description String," +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "BUCKETS '213')")

    val dimension3 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf3 = snc.createDataFrame(dimension3)
    dimensionDf3.write.insertInto("PR_TABLE13")

    val excatJoinKeys = snc.sql(s"select P.ORDERREF, P.DESCRIPTION from " +
        s"PR_TABLE11 P ,PR_TABLE12 R, PR_TABLE13 Q where" +
        s" P.ORDERID = R.OrderId AND P.ORDERREF = R.OrderRef " +
        s"AND " +
        s"R.ORDERID = Q.OrderId AND R.ORDERREF = Q.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = true)
    assert(excatJoinKeys.count() === 500)

    // check CROSS JOIN without requiring spark.sql.crossJoin.enabled property
    val crossJoin = snc.sql("select P.ORDERREF, P.DESCRIPTION from " +
        "PR_TABLE11 P INNER JOIN PR_TABLE12 R " +
        "ON (P.ORDERID = R.OrderId AND P.ORDERREF = R.OrderRef) " +
        "CROSS JOIN PR_TABLE13 Q")
    // expected cartesian product
    val qe = new QueryExecution(snc.snappySession, crossJoin.logicalPlan)
    assert(qe.executedPlan.find(_.isInstanceOf[CartesianProductExec]).isDefined)
    assert(crossJoin.count() === 500000)
  }

  test("SnappyAggregation partitioning") {

    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestDatak(i % 10, i.toString, i % 10)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE20")

    snc.sql("CREATE TABLE PR_TABLE20(OrderId INT, description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef')")
    refDf.write.insertInto("PR_TABLE20")
    val groupBy1 = snc.sql(s"select  OrderRef, orderId from pr_table20 group by OrderRef, orderId")
    checkForShuffle(groupBy1.logicalPlan, snc, shuffleExpected = false)
    val groupBy2 = snc.sql(s"select  orderId, sum(orderRef) from pr_table20 group by orderId")
    checkForShuffle(groupBy2.logicalPlan, snc, shuffleExpected = true)
  }

  private def loadTables(tableType: String, primaryKey: String,
      partitioning: String, colocation: String): Unit = {

    snc.sql(s"create table trade.customers" +
        s" (cid int not null $primaryKey, cust_name varchar(100), " +
        s"since date, addr varchar(100), tid int) " +
        s"USING $tableType OPTIONS ($partitioning)")

    snc.sql("create table trade.securities (sec_id int not null," +
        " symbol varchar(10) not null, price decimal (30, 20)," +
        " exchange varchar(10) not null, tid int," +
        " constraint sec_pk primary key (sec_id)," +
        " constraint sec_uq unique (symbol, exchange)," +
        " constraint exc_ch check" +
        " (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))")

    snc.sql(s"create table trade.networth (cid int not null $primaryKey, " +
        s"cash decimal (30, 20), securities decimal (30, 20), " +
        s"loanlimit int, availloan decimal (30, 20),  tid int) " +
        s"USING $tableType OPTIONS ($partitioning$colocation)")

    val contraint = if (primaryKey.isEmpty) "" else s", constraint portf_pk primary key (cid, sid)"
    snc.sql("create table trade.portfolio (cid int not null," +
        " sid int not null, qty int not null," +
        " availQty int not null, subTotal decimal(30,20)," +
        s" tid int$contraint)" +
        s" USING $tableType OPTIONS ($partitioning$colocation)")

    snc.sql(s"create table trade.sellorders (oid int not null $primaryKey," +
        " cid int, sid int, qty int, ask decimal (30, 20), order_time timestamp," +
        " status varchar(10), tid int)" +
        s" USING $tableType OPTIONS ($partitioning$colocation)")

    snc.sql(s"insert into trade.customers values(1,'abc','2012-01-14','abc-xyz',1)")
    snc.sql(s"insert into trade.customers values(2,'aaa','2012-01-14','aaa-xyz',1)")
    snc.sql(s"insert into trade.customers values(3,'bbb','2012-02-14','abb-xyz',1)")
    snc.sql(s"insert into trade.customers values(4,'ccc','2012-02-16','ccc-xyz',1)")
    snc.sql(s"insert into trade.customers values(5,'ddd','2012-01-16','ddd-xyz',1)")
    snc.sql(s"insert into trade.customers values(6,'eee','2012-03-17','eee-xyz',1)")

    snc.sql("insert into trade.securities values(1,'abc',10.2,'amex',1)")
    snc.sql("insert into trade.securities values(2,'aaa',10.2,'amex',1)")
    snc.sql("insert into trade.securities values(3,'bbb',2.3,'nye',1)")
    snc.sql("insert into trade.securities values(4,'ccc',1.2,'nye',1)")
    snc.sql("insert into trade.securities values(5,'ddd',5.4,'lse',1)")
    snc.sql("insert into trade.securities values(6,'eee',15.4,'lse',1)")

    snc.sql("insert into trade.portfolio values(1,1,10,8,11.2,1)")
    snc.sql("insert into trade.portfolio values(2,2,12,11,13.2,1)")
    snc.sql("insert into trade.portfolio values(3,3,13,12,15.4,1)")
    snc.sql("insert into trade.portfolio values(4,4,15,14,17.6,1)")
    snc.sql("insert into trade.portfolio values(5,5,20,12,14.2,1)")
    snc.sql("insert into trade.portfolio values(6,6,17,10,12.2,1)")

    snc.sql("insert into trade.sellorders values(1,1,1,10,10.2,'2012-01-14 13:18:42.658','open',1)")
    snc.sql("insert into trade.sellorders values(2,2,2,8,10.2,'2012-01-14 13:18:42.658','open',1)")
    snc.sql("insert into trade.sellorders values(3,3,3,9,13.2,'2012-01-14 13:18:42.658','open',1)")
    snc.sql("insert into trade.sellorders values(4,4,4,12,13.2,'2012-01-14 13:18:42.658','open',1)")
    snc.sql("insert into trade.sellorders values(5,5,5,15,13.2,'2012-01-14 13:18:42.658','open',1)")
    snc.sql("insert into trade.sellorders values(6,6,6,19,15.2,'2012-01-14 13:18:42.658','open',1)")

    snc.sql(s"insert into trade.networth values(1,10.2,11.2,10000,5000,1)")
    snc.sql(s"insert into trade.networth values(2,10.2,11.2,10000,5000,1)")
    snc.sql(s"insert into trade.networth values(3,13.2,11.2,15000,8000,1)")
    snc.sql(s"insert into trade.networth values(4,13.2,11.2,12000,3000,1)")
    snc.sql(s"insert into trade.networth values(5,13.2,14.2,20000,10000,1)")
    snc.sql(s"insert into trade.networth values(6,15.2,12.2,25000,15000,1)")
  }

  private def checkQueries_2451(): Unit = {

    var df = snc.sql(s" select f.cid, cust_name, f.sid," +
        s" so.sid, so.qty, subTotal, oid, order_time, ask" +
        s" from trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so " +
        s"where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal >13 and c.cid>3 and f.tid = 1")

    assert(df.collect().size === 2)

    df = snc.sql(s" select f.cid, cust_name, f.sid, so.sid," +
        s" so.qty, subTotal, oid, order_time, ask from" +
        s" trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so" +
        s" where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal >13 and c.cid>1 and f.tid = 1")
    assert(df.collect().size === 4)

    df = snc.sql(s"select n.cid, cust_name, n.securities, n.cash, n.tid, " +
        s"c.cid from trade.customers c, trade.networth n where  n.cid = c.cid" +
        s" and n.tid = 1 and c.cid > 3")
    assert(df.collect().size === 3)
    df = snc.sql(s"select n.cid, cust_name, n.securities, n.cash, n.tid, c.cid" +
        s" from trade.customers c, trade.networth n where n.cid = c.cid" +
        s" and n.tid = 1 and c.cid > 5")
    assert(df.collect().size === 1)
  }

  private def dropTables(): Unit = {
    snc.sql("drop table trade.sellorders")
    snc.sql("drop table trade.portfolio")
    snc.sql("drop table trade.networth")
    snc.sql("drop table trade.securities")
    snc.sql("drop table trade.customers")
  }

  test("SNAP-2451") {

    loadTables("ROW", "primary key", "partition_by 'cid'", ", colocate_with 'trade.customers'")

    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold = -1")
    // snc.sql(s"set snappydata.sql.hashJoinSize=-1")

    checkQueries_2451()

    dropTables()
    loadTables("COLUMN", "", "", "")

    checkQueries_2451()
    var df = snc.sql(s" select f.cid, cust_name, f.sid, so.sid," +
        s" so.qty, subTotal, oid, order_time, ask from" +
        s" trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so" +
        s" where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal > 4 and c.cid = 1 and f.tid = 1")
    assert(df.collect().size === 1)
    df = snc.sql(s" select f.cid, cust_name, f.sid, so.sid," +
        s" so.qty, subTotal, oid, order_time, ask from" +
        s" trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so" +
        s" where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal > 4 and c.cid = 2 and f.tid = 1")
    assert(df.collect().size === 1)

    dropTables()
    loadTables("COLUMN", "", "partition_by 'cid'", ", colocate_with 'trade.customers'")

    checkQueries_2451()
    df = snc.sql(s" select f.cid, cust_name, f.sid, so.sid," +
        s" so.qty, subTotal, oid, order_time, ask from" +
        s" trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so" +
        s" where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal > 4 and c.cid = 1 and f.tid = 1")
    var result = df.collect()
    assert(result.length === 1)
    df = snc.sql(s" select f.cid, cust_name, f.sid, so.sid," +
        s" so.qty, subTotal, oid, order_time, ask from" +
        s" trade.customers c," +
        s" trade.portfolio f," +
        s" trade.sellorders so" +
        s" where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid" +
        s" and subTotal > 4 and c.cid = 2 and f.tid = 1")
    result = df.collect()
    assert(result.length === 1)
    dropTables()
  }

  test("SNAP-2443") {
    val testDF = snc.range(100000).selectExpr("id")
    var splits = testDF.randomSplit(Array(0.7, 0.3))
    var randomTraining = splits(0)
    var randomTesting = splits(1)
    randomTraining.createOrReplaceTempView("randomTraining")
    var summary = snc.sql("select sum(1) from randomTraining")
    val one = summary.collect()(0)(0).asInstanceOf[Long]
    splits = testDF.randomSplit(Array(0.5, 0.5))
    randomTraining = splits(0)
    randomTesting = splits(1)
    randomTraining.createOrReplaceTempView("randomTraining")
    summary = snc.sql("select sum(1) from randomTraining")
    val two = summary.collect()(0)(0).asInstanceOf[Long]
    assert(two < one)
  }

  test("SNAP-2656") {
    val snc = this.snc
    val session = snc.snappySession
    // set broadcast back to default due to changes by previous tests
    session.sql("set spark.sql.autoBroadcastJoinThreshold=10000000")

    session.sql("create table t1 (c1 int, c2 int) options (partition_by 'c1')")
    session.sql("create table t2 (c1 int, c2 int) options (partition_by 'c1')")
    session.sql("insert into t1 values (2, 10), (2, 20), (3, 30)")
    session.sql("insert into t2 values (1, 10), (2, 20), (3, 30)")
    session.sql("create table t3 (c1 int, c2 int, c3 string) using column " +
        "options (partition_by 'c1,c2')")
    session.sql("create table t4 (c1 int, c2 int, c3 string) using column " +
        "options (partition_by 'c1,c2')")
    session.sql("insert into t3 values (1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three')")
    session.sql("insert into t4 values (2, 10, 'two1'), (2, 20, 'two'), (3, 30, '3')")

    val q1 = "select t1.*, t2.c2 from t1 join t2 on (t1.c1 = t2.c1) " +
        "where t1.c2 = (select max(c2) from t2 where t1.c1 = t2.c1)"
    val q2 = "select t1.*, t2.c2 from t1 join t2 on (t1.c1 = t2.c1 and t1.c2 = t2.c2) " +
        "where t1.c2 = (select max(c2) from t2 where t1.c1 = t2.c1)"
    val q3 = "select t1.*, t2.c2 from t1 join t2 on (t1.c1 = t2.c1) " +
        "where t1.c2 = (select max(c2) from t2 where t1.c1 = t2.c1 and t1.c2 = t2.c2)"
    val q4 = "select t1.*, t2.c2 from t1 join t2 on (t1.c1 = t2.c1 and t1.c2 = t2.c2) " +
        "where t1.c2 = (select max(c2) from t2 where t1.c1 = t2.c1 and t1.c2 = t2.c2)"

    val q5 = "select t4.*, t3.c3 from t3 join t4 on (t3.c1 = t4.c1 and t3.c2 = t4.c2) " +
        "where t3.c3 = (select max(c3) from t4 where t3.c1 = t4.c1 and t3.c2 = t4.c2)"
    val res5 = Array(Row(2, 20, "two", "two"))
    val q6 = "select t4.*, t3.c3 from t4 join t3 on (t4.c2 = t3.c2 and t4.c1 = t3.c1) " +
        "where t3.c2 = (select max(c2) from t4 where t4.c1 = t3.c1 and t4.c2 = t3.c2)"
    val res6 = Array(Row(2, 20, "two", "two"), Row(3, 30, "3", "three"))
    val q7 = "select t4.*, t3.c3 from t3 join t4 on (t3.c1 = t4.c1) " +
        "where t3.c2 = (select max(c2) from t4 where t3.c1 = t4.c1 and t3.c2 = t4.c2)"
    val q8 = "select t4.*, t3.c3 from t4 join t3 on (t4.c1 = t3.c1 and t4.c3 = t3.c3) " +
        "where t3.c2 = (select max(c2) from t4 where t4.c1 = t3.c1 and t4.c2 = t3.c2)"
    val res5_8 = Array(res5, res6, Row(2, 10, "two1", "two") +: res6, res5)

    // all queries will result in broadcast plans due to small size
    for (q <- Seq(q1, q2, q3, q4)) {
      val df = session.sql(q)
      checkForBroadcast(df, session, broadcastExpected = true)
      val result = df.collect()
      assert(result.sortBy(_.getInt(0)) === Array(Row(2, 20, 20), Row(3, 30, 30)))
    }
    for ((q, i) <- Seq(q5, q6, q7, q8).zipWithIndex) {
      val df = session.sql(q)
      checkForBroadcast(df, session, broadcastExpected = true)
      val result = df.collect()
      assert(result.sortBy(r => r.getInt(0) * 100 + r.getInt(1)) === res5_8(i))
    }

    // turning off broadcast should ensure it does not result in a shuffle in any of the queries
    session.sql("set spark.sql.autoBroadcastJoinThreshold=-1")

    for (q <- Seq(q1, q2, q3, q4)) {
      val df = session.sql(q)
      checkForBroadcast(df, session, broadcastExpected = false)
      checkForShuffle(df.logicalPlan, snc, shuffleExpected = false)
      val result = df.collect()
      assert(result.sortBy(_.getInt(0)) === Array(Row(2, 20, 20), Row(3, 30, 30)))
    }
    for ((q, i) <- Seq(q5, q6, q7, q8).zipWithIndex) {
      val df = session.sql(q)
      checkForBroadcast(df, session, broadcastExpected = false)
      // exchange is expected when join keys are a subset or partially overlap partitioning keys
      checkForShuffle(df.logicalPlan, snc, shuffleExpected = i >= 2)
      // exactly two shuffles for last two
      if (i >= 2) {
        assert(df.queryExecution.executedPlan.collect {
          case _: Exchange => true
        }.length === 2)
      }
      val result = df.collect()
      assert(result.sortBy(r => r.getInt(0) * 100 + r.getInt(1)) === res5_8(i))
    }

    session.sql("drop table t1")
    session.sql("drop table t2")
    session.sql("drop table t3")
    session.sql("drop table t4")
  }

  def partitionToPartitionJoinAssertions(snc: SnappyContext,
      t1: String, t2: String): Unit = {

    val nullableEquality = snc.sql(s"select P.OrderRef, P.description from " +
        s"$t1 P JOIN $t2 R ON P.OrderId = R.OrderId" +
        s" AND P.OrderRef <=> R.OrderRef")
    // TODO Why an exchange is needed for coalesce.
    checkForShuffle(nullableEquality.logicalPlan, snc, shuffleExpected = true)
    assert(nullableEquality.count() === 500)

    val withCoalesce = snc.sql(s"select P.OrderRef, P.description from " +
        s"$t1 P JOIN $t2 R ON P.OrderId = R.OrderId" +
        s" AND coalesce(P.OrderRef,0) = coalesce(R.OrderRef,0)")
    // TODO Why an exchange is needed for coalesce.

    checkForShuffle(withCoalesce.logicalPlan, snc, shuffleExpected = true)
    assert(withCoalesce.count() === 500)

    val excatJoinKeys = snc.sql(s"select P.OrderRef, P.description from " +
        s"$t1 P JOIN $t2 R ON P.OrderId = R.OrderId AND P.OrderRef = R.OrderRef")
    checkForShuffle(excatJoinKeys.logicalPlan, snc, shuffleExpected = false)
    // Make sure aggregation is working with non-shuffled joins
    assert(excatJoinKeys.count() === 500)

    // Reverse the join keys
    val reverseJoinSQL = if (isDifferentJoinOrderSupported) {
      "select P.OrderRef, P.description from " +
          s"$t1 P JOIN $t2 R ON P.OrderRef = R.OrderRef " +
          "AND P.OrderId = R.OrderId"
    } else {
      "select P.OrderRef, P.description from " +
          s"$t1 P JOIN $t2 R ON P.OrderId = R.OrderId " +
          "AND P.OrderRef = R.OrderRef"
    }
    val reverseJoinKeys = snc.sql(reverseJoinSQL)
    checkForShuffle(reverseJoinKeys.logicalPlan, snc, shuffleExpected = false)

    // Partial join keys
    val partialJoinKeys = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P JOIN $t2 R ON P.OrderRef = R.OrderRef")
    checkForShuffle(partialJoinKeys.logicalPlan, snc, shuffleExpected = true)

    // More join keys than partitioning keys
    val moreJoinSQL = if (isDifferentJoinOrderSupported) {
      "select P.OrderRef, P.description from " +
          s"$t1 P JOIN $t2 R ON P.OrderRef = R.OrderRef AND " +
          "P.OrderId = R.OrderId AND P.description = R.description"
    } else {
      "select P.OrderRef, P.description from " +
          s"$t1 P JOIN $t2 R ON P.OrderId = R.OrderId AND " +
          "P.OrderRef = R.OrderRef AND P.description = R.description"
    }
    val moreJoinKeys = snc.sql(moreJoinSQL)
    checkForShuffle(moreJoinKeys.logicalPlan, snc,
      shuffleExpected = !isDifferentJoinOrderSupported)


    val leftSemijoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P LEFT SEMI JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(leftSemijoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(leftSemijoinDF.count() === 500)

    val innerJoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P INNER JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(innerJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(innerJoinDF.count() === 500)

    val leftJoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P LEFT JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(leftJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(leftJoinDF.count() == 1000)

    val rightJoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P RIGHT JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(rightJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(rightJoinDF.count() == 1000)

    val leftOuterJoinDF = snc.sql("select P.OrderRef, P.description " +
        s"from $t1 P LEFT OUTER JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(leftOuterJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(leftOuterJoinDF.count() == 1000)

    val rightOuterJoinDF = snc.sql("select P.OrderRef, P.description " +
        s"from $t1 P RIGHT OUTER JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(rightOuterJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(rightOuterJoinDF.count() == 1000)

    val fullJoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P FULL JOIN $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(fullJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(fullJoinDF.count() == 1500)

    val fullOuterJoinDF = snc.sql("select P.OrderRef, P.description from " +
        s"$t1 P FULL OUTER JOIN   $t2 R ON P.OrderId = R.OrderId " +
        "AND P.OrderRef = R.OrderRef")
    // We don't expect a shuffle here
    checkForShuffle(fullOuterJoinDF.logicalPlan, snc, shuffleExpected = false)
    assert(fullOuterJoinDF.count() == 1500)
  }

  protected def isDifferentJoinOrderSupported: Boolean = true
}
