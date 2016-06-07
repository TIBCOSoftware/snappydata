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
package org.apache.spark.sql.store

import java.sql.DriverManager

import scala.util.{Failure, Success, Try}

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import com.pivotal.gemfirexd.internal.impl.sql.compile.ParserImpl
import io.snappydata.SnappyFunSuite
import io.snappydata.core.{Data, TestData, TestData2}
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{AnalysisException, SaveMode}

/**
 * Tests for column tables in GFXD.
 */
class ColumnTableTest
    extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  after {
    snc.dropTable(tableName, ifExists = true)
    snc.dropTable("ROW_TABLE2", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE1", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE2", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE4", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE5", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE6", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE7", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE8", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE9", ifExists = true)
    snc.dropTable("COLUMN_TEST_TABLE10", ifExists = true)
  }

  val tableName: String = "ColumnTable"

  val props = Map.empty[String, String]


  val options = "OPTIONS (PARTITION_BY 'col1')"

  val optionsWithURL = "OPTIONS (PARTITION_BY 'Col1', URL 'jdbc:snappydata:;')"


  test("Test the creation/dropping of column table using Schema") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql("Create Table test.MY_TABLE (a INT, b INT, c INT) using column options()")


    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable("test.MY_TABLE")
    var result = snc.sql("SELECT * FROM test.MY_TABLE" )
    var r = result.collect
    println(r.length)

    snc.sql("drop table test.MY_TABLE" )

    println("Successful")
  }


  test("Test the creation/dropping of table using Snappy API") {
    //shouldn't be able to create without schema
    intercept[AnalysisException] {
      snc.createTable(tableName, "column", props, allowExisting = false)
    }

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)


    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation of table using DataSource API") {

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test table creation using Snappy API and then append/ignore/overwrite DF using DataSource API") {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    var rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    var dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)

    intercept[AnalysisException] {
      dataDF.write.format("column").mode(SaveMode.ErrorIfExists).options(props).saveAsTable(tableName)
    }
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 5)

    // Ignore if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 5)

    // Append if table is present
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 11)

    // Overwrite if table is present
    dataDF.write.format("column").mode(SaveMode.Overwrite).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 6)

    println("Successful")
  }


  test("Test the creation/dropping of table using SQL") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation/dropping of table using SQ with explicit URL") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        optionsWithURL
    )
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation using SQL and insert a DF in append/overwrite/errorifexists mode") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)

    intercept[AnalysisException] {
      dataDF.write.format("column").mode(SaveMode.ErrorIfExists).options(props).saveAsTable(tableName)
    }

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using SQL and SnappyContext ") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    intercept[AnalysisException] {
      snc.createTable(tableName, "column", dataDF.schema, props)
    }

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using CREATE TABLE AS STATEMENT ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val tableName2 = "CoulmnTable2"
    snc.sql("DROP TABLE IF EXISTS CoulmnTable2")
    snc.sql("CREATE TABLE " + tableName2 + " USING column " +
        options + " AS (SELECT * FROM " + tableName + ")"
    )
    var result = snc.sql("SELECT * FROM " + tableName2)
    var r = result.collect
    assert(r.length == 5)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName2)
    result = snc.sql("SELECT * FROM " + tableName2)
    r = result.collect
    assert(r.length == 10)

    snc.dropTable(tableName2)
    println("Successful")
  }

  test("Test the truncate syntax SQL and SnappyContext") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.truncateTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 0)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    snc.sql("TRUNCATE TABLE " + tableName)

    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 0)

    println("Successful")
  }

  test("Test the drop syntax SnappyContext and SQL ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.dropTable(tableName, true)

    intercept[AnalysisException] {
      snc.dropTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    println("Successful")
  }

  test("Test the drop syntax SQL and SnappyContext ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    intercept[AnalysisException] {
      snc.dropTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.dropTable(tableName, true)

    println("Successful")
  }

  test("Test PR with REDUNDANCY") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE1")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE1(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE1", true).asInstanceOf[PartitionedRegion]

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy === 2)
  }

  test("Test PR with buckets") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE2")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE2(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE2", true).asInstanceOf[PartitionedRegion]

    val numPartitions = region.getTotalNumberOfBuckets
    assert(numPartitions === 213)
  }


  test("Test PR with RECOVERDELAY") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE4")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE4(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        "RECOVERYDELAY '2')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE4", true).asInstanceOf[PartitionedRegion]

    val rDelay = region.getPartitionAttributes.getRecoveryDelay
    assert(rDelay === 2)
  }

  test("Test PR with MAXPART") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE5")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE5(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "MAXPARTSIZE '200')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE5", true).asInstanceOf[PartitionedRegion]

    val rMaxMem = region.getPartitionAttributes.getLocalMaxMemory
    assert(rMaxMem === 200)
  }

  test("Test PR with EVICTION BY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE6")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE6(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EVICTION_BY 'LRUMEMSIZE 200')")

    Misc.getRegionForTable("APP.COLUMN_TEST_TABLE6", true).asInstanceOf[PartitionedRegion]
  }

   test("Test PR with Colocation") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql("CREATE TABLE COLUMN_TEST_TABLE20(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EVICTION_BY 'LRUMEMSIZE 200')")

    //snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE21")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE21(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "COLOCATE_WITH 'COLUMN_TEST_TABLE20')")


    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE20", true).asInstanceOf[PartitionedRegion]
    assert(region.colocatedByList.size() == 2)
     snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE21")
     snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE20")
  }

  test("Test PR with PERSISTENT") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE7")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE7(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE7", true).asInstanceOf[PartitionedRegion]
    assert(region.getDiskStore != null)
    assert(!region.getAttributes.isDiskSynchronous)
  }

  test("Test RR with PERSISTENT") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE8")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE8(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PERSISTENT 'ASYNCHRONOUS')")

    val region = Misc.getRegionForTable("APP.COLUMN_TEST_TABLE8", true).asInstanceOf[PartitionedRegion]
    assert(region.getDiskStore != null)
    assert(!region.getAttributes.isDiskSynchronous)
  }

  test("Test PR with multiple columns") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE9")
    snc.sql("CREATE TABLE COLUMN_TEST_TABLE9(OrderId INT ,ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId, ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("COLUMN_TEST_TABLE9")
    val count = snc.sql("select * from COLUMN_TEST_TABLE9").count()
    assert(count === 1000)
  }

  test("Test Non parttitioned tables") {
    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE10")

    snc.sql("CREATE TABLE row_table2(OrderId INT ,ItemId INT)" +
        "USING column options()")


    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("COLUMN_TEST_TABLE10")

    val count = snc.sql("select * from COLUMN_TEST_TABLE10").count()
    assert(count === 1000)
  }

  test("Test PR Incorrect option") {
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE27")

    Try(snc.sql("CREATE TABLE COLUMN_TEST_TABLE7(OrderId INT ,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITIONBY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with incorrect options")
      case Failure(error) => // Do nothing
    }

  }

  test("Test DataSource API  with fully qualified table name") {
    val tableName = "test.table1"
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    assert(snc.sql(s"select * from $tableName").collect().length == 5)
    snc.truncateTable(tableName)
    assert(snc.sql(s"select * from $tableName").collect().length == 0)
    snc.dropTable(tableName)
    println("Successful")
  }

  test("Test SQL API with fully qualified table name") {
    val tableName = "test.table1"
    snc.sql(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 INT) USING column ")
    assert(snc.sql("SELECT * FROM " + tableName).collect().length == 0)
    snc.sql(s" insert into $tableName values ( 1, 2, 3)")
    snc.sql(s" insert into $tableName values ( 2, 2, 3)")
    snc.sql(s" insert into $tableName values ( 3, 2, 3)")
    assert(snc.sql("SELECT * FROM " + tableName).collect().length == 3)
    snc.sql(s"  truncate table $tableName")
    assert(snc.sql("SELECT * FROM " + tableName).collect().length == 0)
    snc.sql(s"DROP TABLE $tableName")
  }

  test ("Test Row buffer eviction with fully qualified table name") {
     testRowBufferEviction("test.testTableWithSchema")
  }

  test ("Test Row buffer eviction with table name without schema") {
    testRowBufferEviction("testTableWithoutSchema")
  }


  private def testRowBufferEviction(tableName:String): Unit = {
    val props = Map(("BUCKETS" -> "1"))
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    assert(snc.sql(s"select * from $tableName").collect().length == 5)

    val conn = DriverManager.getConnection("jdbc:snappydata:;query-routing=false")
    val rs = conn.createStatement().executeQuery("select count (*) from " + tableName)
    if (rs.next()) {
      //The row buffer should not have more than 2 rows as the batch size is 3
      assert(rs.getInt(1) <= 2)
    }

    rs.close()
    conn.close()

  }

  test("Test PR with EXPIRY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS COLUMN_TEST_TABLE27")
    Try(snc.sql("CREATE TABLE COLUMN_TEST_TABLE27(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EXPIRE '200')")) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with incorrect options")
      case Failure(error) => // Do nothing
    }

  }

  test("compare parser performance") {
    val snc = this.snc
    val sqlText = " select" +
        "         SUPP_NATION," +
        "         CUST_NATION," +
        "         L_YEAR, " +
        "         sum(VOLUME) as REVENUE" +
        " from (" +
        "         select" +
        "                 N1.N_NAME as SUPP_NATION," +
        "                 N2.N_NAME as CUST_NATION," +
        //        "                 extract m(year from l_shipdate) as l_year," +
        "                 year(L_SHIPDATE) as L_YEAR," +
        "                 L_EXTENDEDPRICE * (1 - L_DISCOUNT) as VOLUME" +
        "         from" +
        "                 SUPPLIER," +
        "                 LINEITEM," +
        "                 ORDERS," +
        "                 CUSTOMER," +
        "                 NATION N1," +
        "                 NATION N2" +
        "         where" +
        "                 S_SUPPKEY = L_SUPPKEY" +
        "                 and O_ORDERKEY = L_ORDERKEY" +
        "                 and C_CUSTKEY = O_CUSTKEY" +
        "                 and S_NATIONKEY = N1.N_NATIONKEY" +
        "                 and C_NATIONKEY = N2.N_NATIONKEY" +
        "                 and (" +
        "                         (trim(upper(N1.N_NAME)) = 'FRANCE' and " +
        "                          trim(upper(N2.N_NAME)) = 'GERMANY')" +
        "                      or (trim(upper(N1.N_NAME)) = 'GERMANY' and " +
        "                          trim(upper(N2.N_NAME)) = 'FRANCE')" +
        "                 )" +
        "                 and L_SHIPDATE between '1995-01-01' and '1996-12-31'" +
        "         ) as SHIPPING" +
        " group by" +
        "         SUPP_NATION," +
        "         CUST_NATION," +
        "         L_YEAR" +
        " order by" +
        "         SUPP_NATION," +
        "         CUST_NATION," +
        "         L_YEAR"

    // warmup runs
    var plan1: LogicalPlan = null
    var plan2: LogicalPlan = null
    val conn = DriverManager.getConnection("jdbc:snappydata:")
        .asInstanceOf[EmbedConnection]
    conn.setupContextStack(true)
    val cc = conn.getLanguageConnection.pushCompilerContext()
    try {

      val pi = new ParserImpl(cc)
      val pd = new ParseDriver

      // timed runs for the parsers
      var start: Double = 0.0
      var end: Double = 0.0
      var elapsed: Double = 0.0
      val warmupRuns = 2000
      val timedRuns = 5000

      println(s"Warmup runs for SparkSQL parser ...")
      for (i <- 0 until 20) {
        plan1 = SqlParser.parse(sqlText)
      }
      println(s"Done with warmup runs")
      start = System.nanoTime()
      for (i <- 0 until 30) {
        plan1 = SqlParser.parse(sqlText)
      }
      end = System.nanoTime()
      elapsed = (end - start) / 1000000.0
      println(s"Time taken by SparkSQL parser for 30 runs = ${elapsed}ms " +
          s"average=${elapsed / 30}ms")
      println()

      println(s"===============  Comparing $timedRuns runs  ===============")
      println()

      println(s"Warmup runs for Snappy parser ...")
      for (i <- 0 until warmupRuns) {
        plan2 = snc.getSQLDialect().parse(sqlText)
      }
      println(s"Done with warmup runs")
      start = System.nanoTime()
      for (i <- 0 until timedRuns) {
        plan2 = snc.getSQLDialect().parse(sqlText)
      }
      end = System.nanoTime()
      elapsed = (end - start) / 1000000.0
      println(s"Time taken by Snappy parser = ${elapsed}ms " +
          s"average=${elapsed / timedRuns}ms")
      println()

      println(s"Warmup runs for GemXD parser ...")
      for (i <- 0 until warmupRuns) {
        pi.parseStatement(sqlText)
      }
      println(s"Done with warmup runs")
      start = System.nanoTime()
      for (i <- 0 until timedRuns) {
        pi.parseStatement(sqlText)
      }
      end = System.nanoTime()
      elapsed = (end - start) / 1000000.0
      println(s"Time taken by GemXD parser = ${elapsed}ms " +
          s"average=${elapsed / timedRuns}ms")
      println()

      println(s"Warmup runs for Hive parser ...")
      for (i <- 0 until warmupRuns) {
        pd.parse(sqlText)
      }
      println(s"Done with warmup runs")
      start = System.nanoTime()
      for (i <- 0 until timedRuns) {
        pd.parse(sqlText)
      }
      end = System.nanoTime()
      elapsed = (end - start) / 1000000.0
      println(s"Time taken by Hive parser = ${elapsed}ms " +
          s"average=${elapsed / timedRuns}ms")

    } finally {
      conn.getLanguageConnection.popCompilerContext(cc)
      conn.restoreContextStack()
      conn.close()
    }
  }
}
