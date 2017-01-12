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

package io.snappydata.cluster

import java.sql.{ResultSet, SQLException, SQLSyntaxErrorException, Connection, DriverManager}

import scala.util.Random

import com.gemstone.org.jgroups.oswego.concurrent.Callable
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo
import com.pivotal.gemfirexd.internal.engine.{GemFireXDQueryObserver, GemFireXDQueryObserverAdapter, GemFireXDQueryObserverHolder}
import com.pivotal.gemfirexd.internal.impl.sql.rules.ExecutionEngineArbiter
import com.pivotal.gemfirexd.internal.impl.sql.rules.ExecutionEngineRule.ExecutionEngine
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.test.dunit.{DistributedTestBase, AvailablePortHelper, SerializableRunnable}
import io.snappydata.test.util.TestException
import junit.framework.Assert

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.sql.types.Decimal

/**
  * Tests for query routing from JDBC client driver.
  */
class ExecutionEngineArbiterDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging with ExecutionEngineArbiterTestBase {

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    // setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def testExecutionEngineForDistinctQueries(): Unit = {
    distinctExecutionEngineRule(SnappyContext())
  }


  def testExecutionEngineForSpecialOuterJoinQueries(): Unit = {
    outerJoinExecutionEngineRule(SnappyContext())
  }


  def testExecutionEngineForGroupByQueries(): Unit = {
    groupByExecutionEngineRule(SnappyContext())
  }


  def testExecutionEngineForReplicatedTableQueries(): Unit = {
    replicatedTableExecutionEngineRule(SnappyContext())
  }


  def testExecutionEngineForUnionAndDistinct(): Unit = {
    distinctExecutionEngineRule(SnappyContext())
  }

  def testExecutionEngineQueryHint(): Unit = {
    queryHint(SnappyContext())
  }

  def testExecutionEngineQueryHintWithException(): Unit = {
    queryHintWithException(SnappyContext())
  }

  def testExecutionEngineTableWithGetAllConvertible(): Unit = {
    queryGetAllConvertibleEngineRule(SnappyContext())
  }

  def testExecutionEngineTableWithIndex(): Unit = {
    queryIndexEngineRule(SnappyContext())
  }

  def testPrimaryKeyWithIndex(): Unit = {
    queryPrimaryWithIndex(SnappyContext())
  }

  def testMultipleQueryHint(): Unit = {
    queryWithMultipleHint(SnappyContext())
  }

  override def startNetServer: String = {
    val port = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      port)
    s"localhost:$port"
  }

  override def stopNetServer: Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "stopNetworkServers")
  }

  override def setObserver(executeOnSpark: Boolean, query: String): Unit = {
    val hook = new SerializableRunnable {
      override def run() {
        val executionEngineObserver: GemFireXDQueryObserver = new GemFireXDQueryObserverAdapter() {

          override def testExecutionEngineDecision(queryInfo: QueryInfo, engine:
          ExecutionEngine, queryText: String): Unit = {

            if (queryText.equals(query)) {
              println("SKSK callback getting invoked for follwoing query" + query + " queryText :" + queryText)
              if (executeOnSpark) {
                println("SKSK callback getting invoked for follwoing query : asserting spark")
                assert(engine == ExecutionEngine.SPARK)
              }
              else {
                println("SKSK callback getting invoked for follwoing query : asserting store")
                assert(engine == ExecutionEngine.STORE)
              }
            }
          }
        }

        GemFireXDQueryObserverHolder.setInstance(executionEngineObserver)

      }
    }

    hook.run()
    vm0.invoke(hook)
    vm1.invoke(hook)
    vm2.invoke(hook)
    vm3.invoke(hook)
  }

//  override def setTestHook(): Unit = {
//    val hook = new SerializableRunnable {
//      override def run() {
//        ExecutionEngineArbiter.setTestHookCostThreshold(100)
//      }
//    }
//    hook.run()
//    vm0.invoke(hook)
//    vm1.invoke(hook)
//    vm2.invoke(hook)
//    vm3.invoke(hook)
//  }


}

trait ExecutionEngineArbiterTestBase {
  def setObserver(executeOnSpark: Boolean, query: String);

  def startNetServer: String

  def stopNetServer: Unit

  //def setTestHook: Unit

  def createRowTableAndInsertData(snc: SnappyContext, tableName: String,
      props: Map[String, String] = Map.empty): Unit = {
    val sc = snc.sparkContext
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s =>
      Data(s.head, s(1).toString, Decimal(s(1).toString + '.' + s(2))))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append)
        .saveAsTable(tableName)
  }


  def outerJoinExecutionEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable1"
    val testTable1 = "testTable2"
    val testsubQueryTable1 = "testTable3"

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable1, Map("PARTITION_BY" -> "COL1"))
    createRowTableAndInsertData(snc, testsubQueryTable1, Map("PARTITION_BY" -> "COL1"))

    val serverHostPort = startNetServer

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    runAndValidateQuery(conn, true, s"select t.col1  from $testTable t  " +
        s"LEFT OUTER JOIN $testTable1 t1 on t.col1 = t1.col1 ")

    val s = conn.createStatement()
    s.execute(s"drop table $testTable")
    s.execute(s"drop table $testTable1")
    s.execute(s"drop table $testsubQueryTable1")

    s.close()
    conn.close()

    stopNetServer
  }

  def runAndValidateQuery(conn: Connection, isSparkExecution: Boolean, query:
  String, isUpdate: Boolean = false): Unit = {
    setObserver(isSparkExecution, query)
    val s = conn.createStatement()
    if (isUpdate) s.executeUpdate(query)
    else {
      s.execute(query)
    }
    s.close()
  }

//  def runAndValidateQueryForCostBasedRouting(conn: Connection, isSparkExecution: Boolean, query:
//  String, isUpdate: Boolean = false): Unit = {
//    setTestHook
//    runAndValidateQuery(conn, isSparkExecution, query, isUpdate)
//  }

  def distinctExecutionEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable1"
    val testTable2 = "testTable2"

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable2, Map("PARTITION_BY" -> "COL1"))

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    runAndValidateQuery(conn, true, s"select distinct col2 from $testTable2")
    runAndValidateQuery(conn, true, s" select col2 from $testTable where col1 in " +
        s"(select distinct col1 from $testTable2)")
    runAndValidateQuery(conn, true, s"select distinct(col2) from $testTable2 where col1 in" +
        s" (select col1 from $testTable)")

    val s = conn.createStatement()
    s.execute(s"drop table $testTable")
    s.execute(s"drop table $testTable2")

    s.close()
    conn.close()

    stopNetServer
  }

  def queryHintWithException(snc: SnappyContext): Unit = {
    val testTable = "testTable1"

    createRowTableAndInsertData(snc, testTable, Map("PARTITION_BY" -> "COL1"))
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    runAndValidateQuery(conn, false,
      s"select * from $testTable limit 1")

    // Execute  a query on Store side with wrong syntax that works on spark engine
    try {
      runAndValidateQuery(conn, false,
        s"select * from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Store\n limit 1")
      DistributedTestBase.fail("Expected syntax error as query was supposed to be executed on store with limit clause",
        new TestException("Expected Exception"))
    }
    catch {
      case sqe: SQLException =>
        if ("42X01" != sqe.getSQLState) {
          throw sqe
        }
    }
  }

  def queryGetAllConvertibleEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable1"

    val sc = snc.sparkContext
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt(10))
    }
    val rdd = sc.parallelize(data, data.length).map(s =>
      IndexData(s.head, s(1), Decimal(s(1).toString + '.' + s(2))))

    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(testTable, "row", dataDF.schema, Map("PARTITION_BY" -> "COL1"))
    snc.sql(s"create index col2index on $testTable(col2)")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    val query: String = s"select col1, col3 from $testTable where col2 IN (2,8,6)"
    runAndValidateQuery(conn, false, query)
  }

  def queryPrimaryWithIndex(snc: SnappyContext)  = {
    val sc = snc.sparkContext

    snc.sql("create table tabOne(id1 int not null primary key, id2 int not null, name String, address String) USING row OPTIONS(partition_by 'id1')")
    snc.sql("insert into tabOne values(111, 123, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(222, 234, 'bbb', 'halo')")

    snc.sql("insert into tabOne values(333, 123, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(444, 234, 'bbb', 'halo')")
    snc.sql("insert into tabOne values(555, 234, 'ccc', 'halo')")
    snc.sql("insert into tabOne values(666, 234, 'ccc', 'halo')")

    snc.sql("create index indexOne on tabOne (id1)")
    snc.sql("create index indexTwo on tabOne (id2)")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    var query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexOne\n where id1 = 111"
    runAndValidateQuery(conn, false, query)

    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexTwo\n where id2 = 111"
    runAndValidateQuery(conn, false, query)

    query = "select * from tabOne where id2 = 111"
    runAndValidateQuery(conn, false, query)

    query = "select * from tabOne where id1 = 111"
    runAndValidateQuery(conn, false, query)


    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexOne\n where id1 = 111"
    runAndValidateQuery(conn, false, query)

    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexTwo\n where id2 = 111"
    runAndValidateQuery(conn, false, query)

  }

  def queryWithMultipleHint(snc: SnappyContext)  = {
    val sc = snc.sparkContext

    snc.sql("create table tabOne(id1 int not null primary key, id2 int not null, name String, address String) USING row OPTIONS(partition_by 'id1')")
    snc.sql("insert into tabOne values(111, 123, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(222, 234, 'bbb', 'halo')")

    snc.sql("insert into tabOne values(333, 123, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(444, 234, 'bbb', 'halo')")
    snc.sql("insert into tabOne values(555, 234, 'ccc', 'halo')")
    snc.sql("insert into tabOne values(666, 234, 'ccc', 'halo')")

    snc.sql("create index indexOne on tabOne (id1)")
    snc.sql("create index indexTwo on tabOne (id2)")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    var query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexOne\n where id1 = 111"
    runAndValidateQuery(conn, false, query)

    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store,index=indexTwo\n where id2 = 111"
    runAndValidateQuery(conn, false, query)
    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Spark,index=indexTwo\n where id2 = 111"
    runAndValidateQuery(conn, true, query)

//    //TODO: We may throw exception in future.
    query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Spark,index=indexThree\n where id2 = 111"
    runAndValidateQuery(conn, true, query)

    query = "select * from tabOne --GEMFIREXD-PROPERTIES index=indexTwo\n where id2 = 111"
    runAndValidateQuery(conn, false, query)

    try {
      query = "select * from tabOne --GEMFIREXD-PROPERTIES index=indexThree\n where id2 = 111"
      runAndValidateQuery(conn, true, query)

      //store query hint
      query = "select * from tabOne --GEMFIREXD-PROPERTIES executionEngine=Store, index=indexThree\n where id2 = 111"
      // this should not route but throw exception
      runAndValidateQuery(conn, true, query)
      DistributedTestBase.fail("Expected syntax error as query has wrong index hint",
      new TestException("Expected Exception"))
    }
    catch {
      case sqe: SQLException => if(sqe.getSQLState != SQLState.LANG_INVALID_FORCED_INDEX1) throw sqe
    }
  }

  def queryIndexEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable1"

    val sc = snc.sparkContext
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))

    val rdd = sc.parallelize(data, data.length).map(s =>
      IndexData(s.head, s(1), Decimal(s(1).toString + '.' + s(2))))

    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(testTable, "row", dataDF.schema, Map("PARTITION_BY" -> "COL1"))
    snc.sql(s"create index col2index on $testTable(col2)")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    var query = s"select col1, col3 from $testTable where col2 > 1"
    runAndValidateQuery(conn, true, query)

    query = s"select col1, col3 from $testTable where col2 = 2"
    runAndValidateQuery(conn, false, query)

    query = s"select col1, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark\n where col2 = 2"
    runAndValidateQuery(conn, true, query)

    query = s"select col1, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Store\n where col2 = 2"
    runAndValidateQuery(conn, false, query)

    query = s"select col1, col3 from $testTable where col2 > 1"
    runAndValidateQuery(conn, true, query)

    query = s"select col1, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Store\n where col2 > 1"
    runAndValidateQuery(conn, false, query)

    query = s"select col1, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark\n where col2 > 1"
    runAndValidateQuery(conn, true, query)

    snc.sql(s"create index col1index on $testTable(col1)")

    query = s"select col2, col3 from $testTable where col1 = 3"
    runAndValidateQuery(conn, false, query)

    query = s"select col2, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark\n where col1 = 3"
    runAndValidateQuery(conn, true, query)

    query = s"select col2, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Store\n where col1 = 3"
    runAndValidateQuery(conn, false, query)

    query = s"select col2, col3 from $testTable where col1 > 1"
    runAndValidateQuery(conn, true, query)

    query = s"select col2, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Store\n where col1 > 1"
    runAndValidateQuery(conn, false, query)

    query = s"select col2, col3 from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark\n where col1 > 1"
    runAndValidateQuery(conn, true, query)

  }

/*  def indexSelectivityEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable1"

    val sc = snc.sparkContext
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt(10))
    }
    val rdd = sc.parallelize(data, data.length).map(s =>
      IndexData(s.head, s(1), Decimal(s(1).toString + '.' + s(2))))

    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(testTable, "row", dataDF.schema, Map("PARTITION_BY" -> "COL1"))
    snc.sql(s"create index col1index on $testTable(col1)")
    snc.sql(s"create index col2index on $testTable(col2)")

    dataDF.write.format("row").mode(SaveMode.Append).saveAsTable(testTable)

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    runAndValidateQueryForCostBasedRouting(conn, false,
      s"select col1, count(*) from $testTable WHERE col2 = 2 group by col1")

    runAndValidateQueryForCostBasedRouting(conn, false,
      s"select col1, col3 from $testTable WHERE col2 > 3")

    1 to 10000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt(10))
    }
    val rdd2 = sc.parallelize(data, data.length).map(s =>
      IndexData(s.head, s(1), Decimal(s(1).toString + '.' + s(2))))
    val dataDF2 = snc.createDataFrame(rdd2)

    dataDF2.write.format("row").mode(SaveMode.Overwrite)
        .saveAsTable(testTable)

    runAndValidateQueryForCostBasedRouting(conn, true,
      s"select col1, count(*) from $testTable WHERE col2 = 2 group by col1")

    runAndValidateQueryForCostBasedRouting(conn, true,
      s"select col1, col3 from $testTable WHERE col2 > 3")

  }*/

  def queryHint(snc: SnappyContext): Unit = {
    val testTable = "testTable1"
    val testTable1 = "testTable2"

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable1, Map("PARTITION_BY" -> "COL1"))
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    // Execute  a replicate table query on Spark engine
    runAndValidateQuery(conn, true,
      s"select * from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark")

    // execute distinct query on partitioned table on store
    runAndValidateQuery(conn, false,
      s"select distinct col1  from $testTable1" +
          s" -- GEMFIREXD-PROPERTIES executionEngine=Store")


  }

  def queryMultipleQueryHint(snc: SnappyContext): Unit = {
    val testTable = "testTable1"
    val testTable1 = "testTable2"

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable1, Map("PARTITION_BY" -> "COL1"))
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + startNetServer)

    runAndValidateQuery(conn, true,
      s"select * from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark")


    // Execute  a replicate table query on Spark engine
    runAndValidateQuery(conn, true,
      s"select * from $testTable -- GEMFIREXD-PROPERTIES executionEngine=Spark")

    // execute distinct query on partitioned table on store
    runAndValidateQuery(conn, false,
      s"select distinct col1  from $testTable1" +
          s" -- GEMFIREXD-PROPERTIES executionEngine=Store")

  }


  def replicatedTableExecutionEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable"
    val testTable1 = "testTable1"
    val testTable2 = "testTable2"

    val serverHostPort = startNetServer

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable1)
    createRowTableAndInsertData(snc, testTable2)

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)


    // test for distinct queries
    runAndValidateQuery(conn, false, s"select distinct col1 from $testTable")
    runAndValidateQuery(conn, false, s"select col1 from $testTable where col2 in " +
        s"(select distinct col2 from $testTable2)")
    runAndValidateQuery(conn, false, s"select sum(col1) from $testTable group by col2")

    runAndValidateQuery(conn, false, s"select col1 from  $testTable where col1 in" +
        s" (select avg(col1) from $testTable2 group by col2)")

    // test for union queries
    runAndValidateQuery(conn, false, s"select col1  " +
        s"from $testTable union select col1 from $testTable1")
    runAndValidateQuery(conn, false, s"select *  from $testTable2 where  col1 in " +
        s"( select col1  from $testTable union select col1 from $testTable1)")


    // test intersect queries
    runAndValidateQuery(conn, false, s"select col1  from $testTable " +
        s"intersect select col1 from $testTable1")

    runAndValidateQuery(conn, false, s"select *  from $testTable2 where  col1 in " +
        s"( select col1  from $testTable intersect select col1 from $testTable1)")

    val s = conn.createStatement()
    s.execute(s"drop table $testTable")
    s.execute(s"drop table $testTable1")
    s.execute(s"drop table $testTable2")

  }

  def groupByExecutionEngineRule(snc: SnappyContext): Unit = {
    val testTable = "testTable"
    val testTable1 = "testTable1"
    val testTable2 = "testTable2"

    createRowTableAndInsertData(snc, testTable)
    createRowTableAndInsertData(snc, testTable1, Map("PARTITION_BY" -> "COL2"))
    createRowTableAndInsertData(snc, testTable2, Map("PARTITION_By" -> "COL2"))


    val serverHostPort = startNetServer

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    runAndValidateQuery(conn, true, s"select count(*) from $testTable1 group by col1")


    runAndValidateQuery(conn, true, s"select count(col1)  from $testTable1 " +
        s" where col1 in ( 5, 1, 2, 4, 5, 6,7,8,9,10) group by col3 ")

    runAndValidateQuery(conn, true, s" select sum(t1.col1)  from $testTable1 t1 , $testTable t2 " +
        s"where t1.col1 = t2.col1 group by t1.col2")

    runAndValidateQuery(conn, false, s" select *  from $testTable1 t1 where col1 in  " +
        s"(select avg(col1) from $testTable group by col2)")

    runAndValidateQuery(conn, true, s"create index  testIndex on $testTable1(col1)", true)

    runAndValidateQuery(conn, false, s"select count(col1)  from $testTable1 " +
        s" where col1 in ( 5, 1, 2, 4, 5, 6,7,8,9,10) group by col3 ")

    runAndValidateQuery(conn, false, s" select sum(t1.col1)  from $testTable1 t1 , $testTable t2 " +
        s"where t1.col1 = t2.col1 group by t1.col2")

    runAndValidateQuery(conn, true, s"select sum(col1) from" +
        s" $testTable2 where col2 in (select col1 from $testTable1 " +
        s"where col1 in (1,2,3) group by col1)")

    runAndValidateQuery(conn, false, s"drop table $testTable1")

    runAndValidateQuery(conn, false,
      s" create table $testTable1 (col1 int primary key , col2 int , col3 int ) " +
          s"using row options (" + "PARTITION_BY 'PRIMARY KEY'" + ")", true)

    runAndValidateQuery(conn, true, s"select sum(col1) from" +
        s" $testTable2 where col2 in (select col1 from $testTable1 " +
        s"where col1 in (1,2,3) group by col1)")

    // create index on col2 and then check for query execution again

    val s = conn.createStatement()
    s.execute(s"drop table $testTable")
    s.execute(s"drop table $testTable1")
    s.execute(s"drop table $testTable2")

    s.close()
    conn.close()

    stopNetServer
  }
}