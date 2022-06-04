/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package io.snappydata

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CyclicBarrier, Executors, SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import io.snappydata.SnappyFunSuite.checkAnswer
import io.snappydata.test.dunit.{DistributedTestBase, SerializableRunnable}
import org.scalatest.Assertions

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.{Row, SnappySession}

/**
 * Common tests for updates/deletes on column table.
 */
object ColumnUpdateDeleteTests extends Assertions with Logging {

  def testBasicUpdate(session: SnappySession): Unit = {
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // session.conf.set(Property.ColumnMaxDeltaRows.name, "200")

    val numElements = 50000

    session.sql("drop table if exists updateTable")
    session.sql("drop table if exists checkTable1")
    session.sql("drop table if exists checkTable2")
    session.sql("drop table if exists checkTable3")

    session.sql("create table updateTable (id int, addr string, status boolean) " +
        "using column options(buckets '4')")
    session.sql("create table checkTable1 (id int, addr string, status boolean) " +
        "using column options(buckets '4')")
    session.sql("create table checkTable2 (id int, addr string, status boolean) " +
        "using column options(buckets '2')")
    session.sql("create table checkTable3 (id int, addr string, status boolean) " +
        "using column options(buckets '1')")

    session.range(numElements).selectExpr("id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("updateTable")

    // check updates to integer column

    session.range(numElements).selectExpr(s"id + $numElements",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable1")

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable1").count() === numElements)


    session.sql(
      s"""update updateTable set id = cast((id + ($numElements / 2)) as int)
         | where id <> 73""".stripMargin)
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))
    session.sql(
      s"""update updateTable set id = cast((id + ($numElements / 2)) as int)
         | where id <> 73""".stripMargin)
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable1").count() === numElements)

    var res = session.sql("select * from updateTable where id = 73").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable where id = cast(substr(addr, 5) as int)")
        .collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable1").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")


    // now check updates to string column

    session.sql(s"update updateTable set id = id - $numElements where id <> 73")
    session.range(numElements).selectExpr(s"id",
      "concat(concat('addr', cast(id as string)), '_update')",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable2")

    session.sql(s"update updateTable set addr = concat(addr, '_update') where id <> 32")
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable2").count() === numElements)

    res = session.sql("select * from updateTable where id = 32").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")

    res = session.sql("select * from updateTable where addr not like '%_update'").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable2").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 32)
    assert(res(0).getString(1) === "addr32")


    // lastly to boolean column

    session.range(numElements).selectExpr(s"id",
      "concat(concat('addr', cast(id as string)), '_update')",
      "case when (id % 2) = 1 then true else false end").write.insertInto("checkTable3")

    session.sql(s"update updateTable set status = not status where id <> 87")
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable3").count() === numElements)

    res = session.sql("select * from updateTable where id = 87").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 87)
    assert(res(0).getString(1) === "addr87_update")
    assert(res(0).getBoolean(2) === false)

    res = session.sql("select * from updateTable where status <> ((id % 2) = 1)").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 87)
    assert(res(0).getString(1) === "addr87_update")
    assert(res(0).getBoolean(2) === false)

    res = session.sql("select * from updateTable EXCEPT select * from checkTable3").collect()
    assert(res.length === 2)
    assert(res.toSet === Set(Row(87, "addr87_update", false), Row(32, "addr32", false)))

    // check BroadcastNestedLoopJoin
    res = session.sql("select u.* from updateTable u, checkTable3 c where " +
        "u.id < 100 and c.id < 100 and (u.status <> c.status or u.addr <> c.addr)").collect()
    assert(res.length === 9902)

    // also with multiple updates leading to delta merges
    session.sql("truncate table checkTable3")
    session.range(numElements).selectExpr(s"id",
      "concat(concat('addr', cast(id as string)), '_update')",
      "case when (id % 2) = 1 then false else true end").write.insertInto("checkTable3")

    session.sql(s"update updateTable set status = not status where id <> 39")

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable3").count() === numElements)

    res = session.sql("select * from updateTable where id = 39").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 39)
    assert(res(0).getString(1) === "addr39_update")
    assert(res(0).getBoolean(2) === true)

    res = session.sql("select * from updateTable where status = ((id % 2) = 1)").collect()
    assert(res.length === 2)
    assert(res.toSet === Set(Row(39, "addr39_update", true), Row(87, "addr87_update", true)))

    res = session.sql("select * from updateTable EXCEPT select * from checkTable3").collect()
    assert(res.length === 3)
    assert(res.toSet === Set(Row(39, "addr39_update", true),
      Row(87, "addr87_update", true), Row(32, "addr32", true)))

    // check no caching for BroadcastNestedLoopJoin
    res = session.sql("select u.* from updateTable u, checkTable3 c where " +
        "u.id < 100 and c.id < 100 and (u.status <> c.status or u.addr <> c.addr)").collect()
    assert(res.length === 9903)

    session.sql("drop table updateTable")
    session.sql("drop table checkTable1")
    session.sql("drop table checkTable2")
    session.sql("drop table checkTable3")

    session.conf.unset(Property.ColumnBatchSize.name)
  }

  def testDeltaStats(session: SnappySession): Unit = {
    session.sql("drop table if exists test1")
    session.sql("create table test1 (col1 long, col2 long) using column " +
        "options (buckets '1', column_batch_size '50')")
    // size of batch ensured so that both rows fall in same batch
    session.range(2).selectExpr("(id  + 1) * 10", "(id + 1) * 100").write.insertInto("test1")

    checkAnswer(session.sql("select * from test1"), Seq(Row(10L, 100L), Row(20L, 200L)))

    // update should change the delta stats else many point queries below will fail
    session.sql("update test1 set col1 = 100 where col2 = 100")

    checkAnswer(session.sql("select * from test1"), Seq(Row(100L, 100L), Row(20L, 200L)))
    checkAnswer(session.sql("select * from test1 where col1 = 100"), Seq(Row(100L, 100L)))
    checkAnswer(session.sql("select * from test1 where col2 = 100"), Seq(Row(100L, 100L)))

    // check for merging of delta stats
    session.sql("update test1 set col1 = 200 where col1 = 20")
    checkAnswer(session.sql("select * from test1"), Seq(Row(100L, 100L), Row(200L, 200L)))
    checkAnswer(session.sql("select * from test1 where col1 = 200"), Seq(Row(200L, 200L)))
    checkAnswer(session.sql("select * from test1 where col2 = 200"), Seq(Row(200L, 200L)))
    session.sql("update test1 set col1 = col1 * 10 where col1 = 100 or col2 = 200")
    checkAnswer(session.sql("select * from test1"), Seq(Row(1000L, 100L), Row(2000L, 200L)))
    checkAnswer(session.sql("select * from test1 where col1 = 1000"), Seq(Row(1000L, 100L)))
    checkAnswer(session.sql("select * from test1 where col2 = 100"), Seq(Row(1000L, 100L)))
    checkAnswer(session.sql("select * from test1 where col1 = 2000"), Seq(Row(2000L, 200L)))
    checkAnswer(session.sql("select * from test1 where col2 = 200"), Seq(Row(2000L, 200L)))

    // also check for other column
    session.sql("update test1 set col2 = 10 where col1 = 1000")
    checkAnswer(session.sql("select * from test1"), Seq(Row(1000L, 10L), Row(2000L, 200L)))
    checkAnswer(session.sql("select * from test1 where col1 = 1000"), Seq(Row(1000L, 10L)))
    checkAnswer(session.sql("select * from test1 where col2 = 10"), Seq(Row(1000L, 10L)))
    session.sql("update test1 set col2 = 20 where col2 = 200")
    checkAnswer(session.sql("select * from test1"), Seq(Row(1000L, 10L), Row(2000L, 20L)))
    checkAnswer(session.sql("select * from test1 where col1 = 2000"), Seq(Row(2000L, 20L)))
    checkAnswer(session.sql("select * from test1 where col2 = 20"), Seq(Row(2000L, 20L)))
    session.sql("update test1 set col2 = col2 * 100 where col1 = 2000 or col2 = 10")
    checkAnswer(session.sql("select * from test1"), Seq(Row(1000L, 1000L), Row(2000L, 2000L)))
    checkAnswer(session.sql("select * from test1 where col1 = 1000"), Seq(Row(1000L, 1000L)))
    checkAnswer(session.sql("select * from test1 where col2 = 1000"), Seq(Row(1000L, 1000L)))
    checkAnswer(session.sql("select * from test1 where col1 = 2000"), Seq(Row(2000L, 2000L)))
    checkAnswer(session.sql("select * from test1 where col2 = 2000"), Seq(Row(2000L, 2000L)))

    session.sql("drop table test1")
  }

  def testBasicDelete(session: SnappySession): Unit = {
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // session.conf.set(Property.ColumnMaxDeltaRows.name, "200")

    session.sql("drop table if exists updateTable")
    session.sql("drop table if exists checkTable1")
    session.sql("drop table if exists checkTable2")
    session.sql("drop table if exists checkTable3")

    session.sql("create table updateTable (id int, addr string, status boolean) " +
        "using column options(buckets '4', partition_by 'addr')")
    session.sql("create table checkTable1 (id int, addr string, status boolean) " +
        "using column options(buckets '2')")
    session.sql("create table checkTable2 (id int, addr string, status boolean) " +
        "using column options(buckets '8')")
    session.sql("create table checkTable3 (id int, addr string, status boolean) " +
        "using column options(buckets '2')")

    for (_ <- 1 to 3) {
      testBasicDeleteIter(session)

      session.sql("truncate table updateTable")
      session.sql("truncate table checkTable1")
      session.sql("truncate table checkTable2")
      session.sql("truncate table checkTable3")
    }

    session.sql("drop table updateTable")
    session.sql("drop table checkTable1")
    session.sql("drop table checkTable2")
    session.sql("drop table checkTable3")

    session.conf.unset(Property.ColumnBatchSize.name)
  }

  def testBasicDeleteIter(session: SnappySession): Unit = {

    val numElements = 50000

    session.range(numElements).selectExpr("id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("updateTable")

    // check deletes

    session.range(numElements).filter("(id % 10) <> 0").selectExpr(s"id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable1")

    assert(session.table("updateTable").count() === numElements)
    assert(session.table("checkTable1").count() === (numElements * 9) / 10)

    session.sql(s"delete from updateTable where (id % 10) = 0")

    assert(session.table("updateTable").count() === (numElements * 9) / 10)
    assert(session.table("updateTable").collect().length === (numElements * 9) / 10)

    var res = session.sql("select * from updateTable EXCEPT select * from checkTable1").collect()
    assert(res.length === 0)


    // now check deletes after updates to columns

    session.range(numElements).filter("(id % 10) <> 0").selectExpr(s"id + $numElements",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable2")

    session.sql(
      s"""update updateTable set id = cast( (id + ($numElements / 2)) as int)
         | where id <> 73""".stripMargin)
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))
    session.sql(
      s"""update updateTable set id = cast( (id + ($numElements / 2)) as int)
         | where id <> 73""".stripMargin)
    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))

    assert(session.table("updateTable").count() === (numElements * 9) / 10)
    assert(session.table("updateTable").collect().length === (numElements * 9) / 10)

    res = session.sql("select * from updateTable where id = 73").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable where id = cast(substr(addr, 5) as int)")
        .collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    res = session.sql("select * from updateTable EXCEPT select * from checkTable2").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    // more deletes on the same rows as updates

    session.range(numElements).filter("(id % 5) <> 0").selectExpr(s"id + $numElements",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable3")

    session.sql(s"delete from updateTable where (cast(substr(addr, 5) as int) % 5) = 0")

    assert(session.table("updateTable").count() === (numElements * 8) / 10)
    assert(session.table("updateTable").collect().length === (numElements * 8) / 10)

    res = session.sql("select * from updateTable EXCEPT select * from checkTable3").collect()
    assert(res.length === 1)
    assert(res(0).getInt(0) === 73)
    assert(res(0).getString(1) === "addr73")

    // lastly delete everything and check there is nothing in table
    session.sql("delete from updateTable")
    assert(session.sql("select * from updateTable").collect().length === 0)
  }

  def testSNAP1925(session: SnappySession): Unit = {
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "10k")

    val numElements = 50000

    session.sql("drop table if exists order_details")
    session.sql("create table order_details (OrderID int, ProductID int," +
        "UnitPrice double, Quantity smallint, Discount double, tid int) " +
        "using column options(partition_by 'OrderID', buckets '8')")

    session.range(numElements).selectExpr("id", "id + 2", "1.0", "2", "rand()", "id + 1")
        .write.insertInto("order_details")

    session.sql("UPDATE order_details SET UnitPrice = UnitPrice * 1.1 WHERE tid = 6")

    var result = session.sql("select UnitPrice, tid from order_details where tid <> 6").collect()
    assert(result.length === numElements - 1)
    assert(result.toSeq.filter(_.getDouble(0) != 1.0) === Nil)

    result = session.sql("select UnitPrice from order_details where tid = 6").collect()
    assert(result.length === 1)
    assert(result(0).getDouble(0) == 1.1)

    session.sql("UPDATE order_details SET UnitPrice = UnitPrice * 1.1 WHERE tid <> 6")

    result = session.sql("select UnitPrice from order_details where tid = 6").collect()
    assert(result.length === 1)
    assert(result(0).getDouble(0) == 1.1)
    result = session.sql("select UnitPrice, tid from order_details where tid <> 6").collect()
    assert(result.length === numElements - 1)
    assert(result.toSeq.filter(_.getDouble(0) != 1.1) === Nil)
    result = session.sql("select UnitPrice, tid from order_details").collect()
    assert(result.length === numElements)
    assert(result.toSeq.filter(_.getDouble(0) != 1.1) === Nil)


    session.sql("UPDATE order_details SET UnitPrice = 1.1 WHERE tid <> 11")

    result = session.sql("select UnitPrice from order_details where tid = 11").collect()
    assert(result.length === 1)
    assert(result(0).getDouble(0) == 1.1)
    result = session.sql("select UnitPrice, tid from order_details where tid <> 6").collect()
    assert(result.length === numElements - 1)
    assert(result.toSeq.filter(_.getDouble(0) != 1.1) === Nil)
    result = session.sql("select UnitPrice, tid from order_details").collect()
    assert(result.length === numElements)
    assert(result.toSeq.filter(_.getDouble(0) != 1.1) === Nil)

    session.sql("drop table order_details")
    session.conf.unset(Property.ColumnBatchSize.name)
  }

  def testSNAP1926(session: SnappySession): Unit = {
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "10k")

    val numElements = 50000

    session.sql("drop table if exists customers")
    session.sql("CREATE TABLE CUSTOMERS (CUSTOMERID VARCHAR(100), COMPANYNAME VARCHAR(100), " +
        "CONTACTNAME VARCHAR(100), CONTACTTITLE VARCHAR(100), ADDRESS VARCHAR(100), " +
        "CITY VARCHAR(100), REGION VARCHAR(100), POSTALCODE VARCHAR(100), " +
        "COUNTRY VARCHAR(100), PHONE VARCHAR(100), FAX VARCHAR(100), TID INTEGER) " +
        "using column options(partition_by 'City,Country', buckets '8')")

    session.range(numElements).selectExpr("id", "id + 1", "id + 2", "id + 3", "id + 4",
      "id + 5", "id + 6", "id + 7", "id + 8", "id + 9", "id + 10", "id % 20")
        .write.insertInto("customers")

    session.sql("delete from customers where CustomerID IN (SELECT min(CustomerID) " +
        "from customers where tid=10) AND tid=10")

    var result = session.sql("select CustomerID, tid from customers where tid = 10").collect()
    assert(result.length === (numElements / 20) - 1)
    result = session.sql("select CustomerID, tid from customers").collect()
    assert(result.length === numElements - 1)

    session.sql("drop table customers")
    session.conf.unset(Property.ColumnBatchSize.name)
  }

  def testConcurrentOps(session: SnappySession): Unit = {
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // session.conf.set(Property.ColumnMaxDeltaRows.name, "200")

    session.sql("drop table if exists updateTable")
    session.sql("drop table if exists checkTable1")
    session.sql("drop table if exists checkTable2")
    session.sql("drop table if exists checkTable3")

    session.sql("create table updateTable (id int, addr string, status boolean) " +
        "using column options(buckets '4')")
    session.sql("create table checkTable1 (id int, addr string, status boolean) " +
        "using column options(buckets '2')")
    session.sql("create table checkTable2 (id int, addr string, status boolean) " +
        "using column options(buckets '8')")

    // avoid rollover in updateTable during concurrent updates
    val avoidRollover = new SerializableRunnable() {
      override def run(): Unit = {
        if (GemFireCacheImpl.getInstance ne null) {
          val pr = Misc.getRegionForTable("APP.UPDATETABLE", false)
              .asInstanceOf[PartitionedRegion]
          if (pr ne null) {
            pr.getUserAttribute.asInstanceOf[GemFireContainer].fetchHiveMetaData(true)
            pr.setColumnBatchSizes(10000000, 10000, 1000)
          }
        }
      }
    }
    DistributedTestBase.invokeInEveryVM(avoidRollover)
    avoidRollover.run()

    for (_ <- 1 to 3) {
      testConcurrentOpsIter(session)

      session.sql("truncate table updateTable")
      session.sql("truncate table checkTable1")
      session.sql("truncate table checkTable2")
    }

    // cleanup
    session.sql("drop table updateTable")
    session.sql("drop table checkTable1")
    session.sql("drop table checkTable2")
    session.conf.unset(Property.ColumnBatchSize.name)
  }

  def testConcurrentOpsIter(session: SnappySession): Unit = {
    val numElements = 100000
    val concurrency = 8
    // each thread will update/delete after these many rows
    val step = 10

    session.range(numElements).selectExpr("id", "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("updateTable")

    // expected results after updates in this table
    val idUpdate = s"id + ($numElements / 2)"
    val idSet = s"case when (id % $step) < $concurrency then id + ($numElements / 2) else id end"
    val addrSet = s"case when (id % $step) < $concurrency " +
        s"then concat('addrUpd', cast(($idUpdate) as string)) " +
        s"else concat('addr', cast(id as string)) end"
    session.range(numElements).selectExpr(idSet, addrSet,
      "case when (id % 2) = 0 then true else false end").write.insertInto("checkTable1")

    // expected results after updates and deletes in this table
    session.table("checkTable1").filter(s"(id % $step) < ${step - concurrency}")
        .write.insertInto("checkTable2")

    val exceptions = new TrieMap[Thread, Throwable]
    val executionContext = ExecutionContext.fromExecutorService(
      Executors.newFixedThreadPool(concurrency + 2))

    // concurrent updates to different rows but same batches
    val barrier = new CyclicBarrier(concurrency)
    var tasks = Array.tabulate(concurrency)(i => Future {
      var waited = false
      try {
        val snappy = new SnappySession(session.sparkContext)
        var res = snappy.sql("select count(*) from updateTable").collect()
        assert(res(0).getLong(0) === numElements)

        barrier.await()
        waited = true
        res = snappy.sql(s"update updateTable set id = cast( $idUpdate as int), " +
            s"addr = concat('addrUpd', cast(($idUpdate) as string)) " +
            s"where (id % $step) = $i").collect()
        assert(res.map(_.getLong(0)).sum > 0)
      } catch {
        case t: Throwable =>
          logError(t.getMessage, t)
          exceptions += Thread.currentThread() -> t
          if (!waited) barrier.await()
          throw t
      }
    }(executionContext))
    tasks.foreach(Await.ready(_, Duration(300, "s")))

    assert(exceptions.isEmpty, s"Failed with exceptions: $exceptions")

    logInfo(session.table("updateTable").limit(20).collect().mkString("\n"))

    var res = session.sql(
      "select * from updateTable EXCEPT select * from checkTable1").collect()
    assert(res.length === 0)

    // concurrent deletes
    tasks = Array.tabulate(concurrency)(i => Future {
      var waited = false
      try {
        val snappy = new SnappySession(session.sparkContext)
        var res = snappy.sql("select count(*) from updateTable").collect()
        assert(res(0).getLong(0) === numElements)

        barrier.await()
        waited = true
        res = snappy.sql(
          s"delete from updateTable where (id % $step) = ${step - i - 1}").collect()
        assert(res.map(_.getLong(0)).sum > 0)
      } catch {
        case t: Throwable =>
          logError(t.getMessage, t)
          exceptions += Thread.currentThread() -> t
          if (!waited) barrier.await()
          throw t
      }
    }(executionContext))
    tasks.foreach(Await.ready(_, Duration(300, "s")))

    assert(exceptions.isEmpty, s"Failed with exceptions: $exceptions")

    res = session.sql(
      "select * from updateTable EXCEPT select * from checkTable2").collect()
    assert(res.length === 0)
  }

  def testSNAP2124(session: SnappySession): Unit = {
    val filePath = getClass.getResource("/sample_records.json").getPath
    session.sql("CREATE TABLE domaindata (cntno_l string,cntno_m string," +
        "day1 string,day2 string,day3 string,day4 string,day5 string," +
        "day6 string,day7 string,dr string,ds string,email string," +
        "id BIGINT NOT NULL,idinfo_1 string,idinfo_2 string,idinfo_3 string," +
        "idinfo_4 string,lang_1 string,lang_2 string,lang_3 string,name string) " +
        "USING COLUMN OPTIONS (PARTITION_BY 'id',BUCKETS '40', COLUMN_BATCH_SIZE '10')")
    session.read.json(filePath).write.insertInto("domaindata")

    var ds = session.sql("select ds, dr from domaindata where id = 40L")
    SnappyFunSuite.checkAnswer(ds, Seq(Row("['cbcinewsemail.com']", "[]")))
    ds = session.sql("select ds, dr from domaindata where id = 418")
    SnappyFunSuite.checkAnswer(ds, Seq(Row("['taskbuckes.com']", "[]")))

    // check for escape character (\) and two single-quote escape in string literal

    ds = session.sql("UPDATE domaindata SET ds = '''cbcin''.com\\']', dr = '[]' WHERE id = 40")
    // below checks both the result and partition pruning (only one row)
    SnappyFunSuite.checkAnswer(ds, Seq(Row(1)))

    ds = session.sql("select ds, dr from domaindata where id = 40")
    // below checks both the result and partition pruning (only one row)
    assert(ds.rdd.getNumPartitions === 1)
    SnappyFunSuite.checkAnswer(ds, Seq(Row("'cbcin'.com']", "[]")))

    ds = session.sql("UPDATE domaindata SET ds = '\\'taskbuck''.com''', dr = '[]' WHERE id = 418")
    // below checks both the result and partition pruning (only one row)
    SnappyFunSuite.checkAnswer(ds, Seq(Row(1)))

    ds = session.sql("select ds, dr from domaindata where id = 418")
    // below checks both the result and partition pruning (only one row)
    assert(ds.rdd.getNumPartitions === 1)
    SnappyFunSuite.checkAnswer(ds, Seq(Row("'taskbuck'.com'", "[]")))

    ds = session.sql("delete from domaindata where id = 40")
    // below checks both the result and partition pruning (only one row)
    assert(ds.rdd.getNumPartitions === 1)
  }

  def testConcurrentUpdateDeleteForCompaction(session: SnappySession,
      numRows: Int = 200000): Unit = {
    assert(numRows % 5000 === 0)
    val barrier = new CyclicBarrier(2)
    // run both versions for partitioned table vs non-partitioned table concurrently
    val runPartitioned = Future(testConcurrentUpdateDeleteForCompactionBody(
      session, partitioned = true, numRows, barrier))(ExecutionContext.global)
    val runNonPartitioned = Future(testConcurrentUpdateDeleteForCompactionBody(
      session.newSession(), partitioned = false, numRows, barrier))(ExecutionContext.global)
    Await.result(runPartitioned, Duration.Inf)
    Await.result(runNonPartitioned, Duration.Inf)
  }

  private def testConcurrentUpdateDeleteForCompactionBody(session: SnappySession,
      partitioned: Boolean, numRows: Int, barrier: CyclicBarrier): Unit = {
    // General idea is to have multiple threads perform deletes and updates stepping through
    // a range of values (different for each thread) to finally delete all data in the table
    // with query threads confirming integrity of the results as well as column batch sizes

    // Threads update/delete slices across multiple blocks in a single iteration and continue
    // to next slice in the next iteration. This is to ensure that multiple column batches get
    // compacted multiple times ultimately landing up into the delta row buffer. The distribution
    // looks like below ("slice" is the marked area across blocks being updated/deleted in current
    // iteration by that thread, while the blocks are the entire data that will be updated/deleted
    // by the thread across all iterations). The data has been laid out as 2-dimensional for ease
    // of visualization while the memory layout will be each of these rows of blocks laid out
    // one after another sequentially in order.
    //
    //      .----------------------
    //     /                       Thread 1 updated blocks
    //    /          .-------------
    //   /          /              Thread 2 updated blocks
    //  / slice    / slice    .----
    // /   | |    /   | |    /
    // .---+-+----.---+-+----.---+-+----. ...
    // |   |*|    |   |*|    |   |*|    |
    // |---+-+----|---+-+----|---+-+----|
    // |   |*|    |   |*|    |   |*|    |
    // |---+-+----|---+-+----|---+-+----|
    // |   |*|    |   |*|    |   |*|    |
    // |---+-+----|---+-+----|---+-+----|
    //
    // ...
    //
    // |   |*|    |   |*|    |   |*|    |
    // .---+-+----.---+-+----.---+-+----.

    val tableName = s"testCompaction_${if (partitioned) "part" else "nopart"}"

    val numUpdateRowsInSingleOp = 1000
    val consecutiveRowsInOp = 10
    val numUpdateThreads = 10
    val numQueryThreads = 1
    val totalThreads = numUpdateThreads + numQueryThreads
    val updatedRows = numRows * 4 / 5
    // fraction overlapping = (3/5) / (4/5)
    val overlappingRowsInSingleOp = numUpdateRowsInSingleOp * 3 / 4
    val remainingRows = numRows - updatedRows
    val updateIterations = updatedRows / (numUpdateThreads * numUpdateRowsInSingleOp)

    val numUpdateBlocks = numUpdateRowsInSingleOp / consecutiveRowsInOp
    val consecutiveRows = updateIterations * consecutiveRowsInOp
    val totalConsecutiveRows = numUpdateThreads * consecutiveRows

    def setConf(key: String, value: String): Unit = {
      // set in SparkConf so that all sessions inherit these values
      val conf = Utils.getInternalSparkConf(session.sparkContext)
      if (value ne null) {
        conf.set(key, value)
        session.conf.set(key, value)
      } else {
        conf.remove(key)
        session.conf.unset(key)
      }
    }

    // sparkConf changes should not be concurrent
    synchronized {
      setConf(Property.ColumnBatchSize.name, "100k")
      setConf(Property.ColumnMaxDeltaRows.name, "500")

      val partitionClause = if (partitioned) ", partition_by 'id'" else ""
      session.sql(s"create table $tableName (id long, data string, seq long) using column " +
          s"options (buckets '32'$partitionClause) " +
          s"as select id, 'data_' || id, monotonically_increasing_id() from range($numRows)")
    }

    // wait at the barrier to avoid one of the threads running DMLs while other is still doing
    // CREATE TABLE causing failure due to CatalogStaleException
    barrier.await()

    val numDeleted = new AtomicInteger(0)
    val numDeletedAndInProgress = new AtomicInteger(0)
    val numUpdated = new AtomicInteger(0)
    val numUpdatedAndInProgress = new AtomicInteger(0)
    val queriesRunning = new AtomicBoolean(true)

    val pool = new ThreadPoolExecutor(totalThreads, totalThreads * 2,
      60, TimeUnit.SECONDS, new SynchronousQueue[Runnable]())
    implicit val context: ExecutionContext = ExecutionContext.fromExecutorService(pool)

    def condition(startId: Long): String = {
      s"id >= $startId and ((id - $startId) % $totalConsecutiveRows) < $consecutiveRowsInOp " +
          s"and ((id - $startId) / $totalConsecutiveRows) < $numUpdateBlocks"
    }

    val doUpdateAndDelete = (i: Int) => Future {
      val snappy = session.newSession()
      // use ranges of size "numConsecutiveRowsInSingleOp" spread across batches in a single delete
      for (iteration <- 0 until updateIterations) {
        if (i == 0 && ((iteration % 10 == 0) || iteration == updateIterations - 1)) {
          // scalastyle:off println
          println(
            s"Running iteration for partitioned=$partitioned ${iteration + 1} / $updateIterations")
          // scalastyle:on println
        }
        logInfo(s"Thread $i running iteration for partitioned=$partitioned " +
            s"${iteration + 1} / $updateIterations")

        // delete in upper (numRows - remainingRows) block while update in lower one
        val deleteStartId = consecutiveRowsInOp * iteration + consecutiveRows * i
        val updateStartId = deleteStartId + remainingRows

        numUpdatedAndInProgress.getAndAdd(numUpdateRowsInSingleOp)
        var rows = snappy.sql(s"update $tableName set data = 'updated_' || data, seq = seq + 1 " +
            s"where ${condition(updateStartId)}").collect()
        assert(rows.map(_.getLong(0)).sum === numUpdateRowsInSingleOp)
        numUpdated.getAndAdd(numUpdateRowsInSingleOp)
        // numUpdated represents the lower bound so reduce the overlapping rows that will be
        // deleted in advance
        numUpdated.getAndAdd(-overlappingRowsInSingleOp)

        numDeletedAndInProgress.getAndAdd(numUpdateRowsInSingleOp)
        rows = snappy.sql(s"delete from $tableName " +
            s"where ${condition(deleteStartId)}").collect()
        assert(rows.map(_.getLong(0)).sum === numUpdateRowsInSingleOp)
        numDeleted.getAndAdd(numUpdateRowsInSingleOp)
        // reduce updated count by overlapping rows that were deleted
        numUpdatedAndInProgress.getAndAdd(-overlappingRowsInSingleOp)
      }
      snappy.clear()
    }
    val doQuery = () => Future {
      val snappy = session.newSession()
      var lastNumDeleted = 0
      while (queriesRunning.get()) {
        var rows: Array[Row] = null

        // check no duplicates seen in queries
        rows = snappy.sql(
          s"select count(*) from $tableName group by id having count(*) > 1").collect()
        assert(rows.length === 0)

        val lowerDeleted = numDeleted.get()
        if (lastNumDeleted != lowerDeleted) {
          rows = snappy.sql(s"select count(*) from $tableName").collect()
          // val upperDeleted = numDeletedAndInProgress.get()
          assert(rows.length === 1)
          // val rowCount = rows(0).getLong(0)
          // selects can fall behind deletes by quite a bit, so below checks are disabled
          // assert(rowCount >= numRows - upperDeleted)
          // assert(rowCount <= numRows - lowerDeleted)
          lastNumDeleted = lowerDeleted
        }
      }
    }

    val updateFutures = (0 until numUpdateThreads).map(doUpdateAndDelete)
    val queryFutures = (0 until numQueryThreads).map(_ => doQuery())
    try {
      updateFutures.foreach(Await.result(_, Duration.Inf))

      assert(numDeleted.get() === updatedRows)
      assert(numDeletedAndInProgress.get() === updatedRows)
      assert(numUpdated.get() === remainingRows)
      assert(numUpdatedAndInProgress.get() === remainingRows)

      queriesRunning.set(false)
      queryFutures.foreach(Await.result(_, Duration.Inf))

      // check the final results
      var rows = session.sql(s"select count(*) from $tableName").collect()
      assert(rows.length === 1)
      assert(rows(0).getLong(0) === remainingRows)
      rows = session.sql(s"select count(*) from $tableName where data like 'update_%'").collect()
      assert(rows.length === 1)
      assert(rows(0).getLong(0) === remainingRows)
      rows = session.sql(
        s"select count(*) from $tableName where data not like 'update_%'").collect()
      assert(rows.length === 1)
      assert(rows(0).getLong(0) === 0)
    } catch {
      case t: Throwable =>
        queriesRunning.set(false)
        throw t
    } finally {
      pool.shutdownNow()
      // wait at the barrier again to avoid one of the threads running DMLs while other
      // dropping the table causing failure due to CatalogStaleException
      barrier.await()
    }

    synchronized {
      session.sql(s"drop table $tableName")

      setConf(Property.ColumnMaxDeltaRows.name, null)
      setConf(Property.ColumnBatchSize.name, null)
    }
  }
}
