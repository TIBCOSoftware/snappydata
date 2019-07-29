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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ListBuffer

import io.snappydata.app.{Data1, Data2, Data3}
import io.snappydata.{QueryHint, SnappyFunSuite}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.execution.PartitionedPhysicalScan
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.HashJoinExec
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode, SnappyContext}

class CreateIndexTest extends SnappyFunSuite with BeforeAndAfterEach {
  self =>

  val indexesToDrop = new ListBuffer[String]
  val tablesToDrop = new ListBuffer[String]
  val context = new AtomicReference[SnappyContext]

  override def beforeAll(): Unit = {
    // System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.testing", "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.testing")
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    try {
      val snContext = SnappyContext(sc)
      io.snappydata.Property.EnableExperimentalFeatures.set(snContext.conf, true)
      snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
      context.set(snContext)
    } finally {
      super.beforeEach()
    }
  }

  override def afterEach(): Unit = {
    try {
      val snContext = context.getAndSet(null)
      if (snContext != null) {
        io.snappydata.Property.EnableExperimentalFeatures.remove(snContext.conf)
        snContext.conf.unsetConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
        indexesToDrop.reverse.foreach(i => snContext.sql(s"DROP INDEX $i "))
        tablesToDrop.reverse.foreach(t => snContext.sql(s"DROP TABLE $t "))
        indexesToDrop.clear()
        tablesToDrop.clear()
      }
    } finally {
      super.afterEach()
    }
  }

  test("Test choice of index according to where predicate") {
    val tableName = "tabOne"
    val snContext = context.get

    def createBaseTable(): Unit = {
      val props = Map(
        "PARTITION_BY" -> "col1")
      snContext.sql("drop table if exists " + tableName)

      val data = Seq(Seq(111, "aaa", "hello"),
        Seq(222, "bbb", "halo"),
        Seq(333, "aaa", "hello"),
        Seq(444, "bbb", "halo"),
        Seq(555, "ccc", "halo"),
        Seq(666, "ccc", "halo")
      )

      val rdd = sc.parallelize(data, data.length).map(s =>
        Data2(s.head.asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
      val dataDF = snContext.createDataFrame(rdd)

      dataDF.write.format("column").options(props).saveAsTable(tableName)
      tablesToDrop += tableName
    }

    createBaseTable()

    doPrint("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
    snContext.sql(s"create index $indexOne on $tableName (COL1)")
    indexesToDrop += indexOne
    snContext.sql(s"create index $indexTwo on $tableName (COL2, COL3)")
    indexesToDrop += indexTwo
    snContext.sql(s"create index $indexThree on $tableName (COL1, COL3)")
    indexesToDrop += indexThree

    val executeQ = CreateIndexTest.QueryExecutor(snContext)

    executeQ(s"select * from $tableName where col1 = 111") {
      CreateIndexTest.validateIndex(Seq(indexOne))(_)
    }

    executeQ(s"select * from $tableName where col2 = 'aaa' ") {
      CreateIndexTest.validateIndex(Nil, tableName)(_)
    }

    executeQ(s"select * from $tableName where col2 = 'bbb' and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexTwo))(_)
    }

    executeQ(s"select * from $tableName where col1 = 111 and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexThree))(_)
    }
  }

  test("Test create Index on Column Table using Snappy API") {
    val tableName: String = "tcol1"
    val snContext = context.get

    val props = Map(
      "PARTITION_BY" -> "col1")
    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111, "aaa", "hello"),
      Seq(222, "bbb", "halo"),
      Seq(333, "aaa", "hello"),
      Seq(444, "bbb", "halo"),
      Seq(555, "ccc", "halo"),
      Seq(666, "ccc", "halo")
    )

    val table2 = "tableCol2"
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createTable(s"$table2", "column", dataDF.schema, props)
    tablesToDrop += table2

    dataDF.write.format("column").options(props).saveAsTable(tableName)
    tablesToDrop += tableName

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    indexesToDrop += "test1"
    snContext.sql("create index test2 on " + tableName +
        s" (COL1) Options (colocate_with  '$tableName')")
    indexesToDrop += "test2"

    try {
      snContext.sql(s"drop table $tableName")
      fail("This should fail as there are indexes associated with this table")
    } catch {
      case e: Throwable =>
    }
    snContext.sql("drop index test1")
    snContext.sql("drop index test2")
    indexesToDrop.clear()
    try {
      snContext.sql("create index a1.test1 on " + tableName + " (COL1)")
      fail("This should fail as the index should have same database as the table")
    } catch {
      case e: Throwable =>
    }

    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")

    snContext.createIndex("test1", tableName,
      Seq("col1" -> None), Map("colocate_with" -> tableName))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Seq("col1" -> None),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName, Seq("col1" -> Some(Ascending)),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    // drop non-existent indexes with if exist clause
    snContext.dropIndex("test1", true)
    snContext.sql("drop index if exists test1")
  }

  test("Test case-sensitivity of index column names (GITHUB-900)") {
    val tableName = "SCHEMA_MIGRATIONS_2"
    val indexName = "SCHEMA_MIGRATIONS_VERSION_IDX"
    val snContext = context.get
    val data = Seq(Row(111L, "aaa"), Row(222L, "bbb"), Row(333L, "aaa"),
      Row(444L, "bbb"), Row(555L, "ccc"), Row(666L, "ccc"))

    import snContext.implicits._

    // test using SQL first
    snContext.sql("drop table if exists " + tableName)
    snContext.sql(
      s"""create table $tableName("version" BIGINT NOT NULL,
        "value" VARCHAR(20), PRIMARY KEY ("version"))""")
    // index creation should succeed with unquoted or quoted name (case-insensitive)
    snContext.sql(s"CREATE UNIQUE INDEX $indexName ON $tableName (version)")
    snContext.sql(s"DROP INDEX $indexName")
    snContext.sql(s"""CREATE UNIQUE INDEX $indexName ON $tableName ("version")""")
    val schema = snContext.table(tableName).schema

    implicit val encoder = RowEncoder(schema)
    val ds = snContext.createDataset(data)
    ds.write.insertInto(tableName)

    checkAnswer(snContext.sql(s"select * from $tableName"), data)
    checkAnswer(snContext.sql(
      s"""select "value" from $tableName where `version`=111"""), Seq(Row("aaa")))
    checkAnswer(snContext.sql(s"""select * from $tableName where "version" >= 555"""),
      Seq(Row(555, "ccc"), Row(666, "ccc")))

    snContext.sql("drop table " + tableName)

    // test using API
    snContext.setConf(SQLConf.CASE_SENSITIVE, true)
    snContext.createTable(tableName, "row",
      """("version" BIGINT NOT NULL,
        "value" VARCHAR(20), PRIMARY KEY ("version"))""",
      Map.empty[String, String], allowExisting = false)
    // index creation should succeed with unquoted or quoted name (case-insensitive)
    snContext.createIndex(indexName, tableName, Seq("version" -> None),
      Map("INDEX_TYPE" -> "UNIQUE"))
    snContext.dropIndex(indexName, ifExists = false)
    snContext.createIndex(indexName, tableName, Seq("Version" -> None),
      Map("INDEX_TYPE" -> "UNIQUE"))

    ds.write.insertInto(tableName)

    checkAnswer(snContext.table(tableName), data)
    checkAnswer(snContext.table(tableName).filter($"version" === 111)
        .select($"value"), Seq(Row("aaa")))
    checkAnswer(snContext.table(tableName).filter($"version" >= 555),
      Seq(Row(555, "ccc"), Row(666, "ccc")))

    snContext.setConf(SQLConf.CASE_SENSITIVE, false)

    snContext.dropIndex(indexName, ifExists = false)
    snContext.dropTable(tableName)
  }

  private def createBase3Tables(snContext: SnappyContext,
      table1: String,
      table2: String,
      table3: String): Unit = {

    val props = Map(
      "PARTITION_BY" -> "col1")
    snContext.sql(s"drop table if exists $table1")

    val data = Seq(Seq(111, "aaa", "hello"),
      Seq(222, "bbb", "halo"),
      Seq(333, "aaa", "hello"),
      Seq(444, "bbb", "halo"),
      Seq(555, "ccc", "halo"),
      Seq(666, "ccc", "halo")
    )

    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    snContext.createTable(s"$table2", "column", dataDF.schema,
      props + ("PARTITION_BY" -> "col2,col3"))
    tablesToDrop += table2

    snContext.createTable(s"$table3", "column", dataDF.schema, props)
    tablesToDrop += table3

    dataDF.write.format("column").options(props).saveAsTable(table1)
    tablesToDrop += table1

    dataDF.write.insertInto(table2)
    dataDF.write.insertInto(table3)
  }

  test("Test two table joins") {
    val table1 = "tabOne"
    val table2 = "tabTwo"
    val table3 = "tabThree"

    val index1 = s"${table1}_IdxOne"
    val index2 = s"${table1}_IdxTwo"
    val index3 = s"${table1}_IdxThree"
    val index4 = s"${table1}_IdxFour"

    val index31 = s"${table3}_IdxOne"

    val leftIdx = Seq(1, 2)
    val rightIdx = Seq(3, 4, 5, 6)

    val snContext = context.get

    createBase3Tables(snContext, table1, table2, table3)

    doPrint("Verify index create and drop for various index types")
    snContext.sql(s"create index $index1 on $table1 (COL1)")
    indexesToDrop += index1
    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL1) Options (colocate_with  '$table3')")
    indexesToDrop += index2

    snContext.sql(s"create index $index3 on $table1 (COL2, COL3)")
    indexesToDrop += index3
    snContext.sql(s"create index $index4 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")
    indexesToDrop += index4

    snContext.sql(s"create index $index31 on $table3 " +
        s" (COL2, COL3) Options (colocate_with  '$index4')")
    indexesToDrop += index31

    val executeQ = CreateIndexTest.QueryExecutor(snContext)

    executeQ(s"select * from $table1 tab1 join $table3 tab2 on tab1.col1 = tab2.col1") {
      CreateIndexTest.validateIndex(Seq(index2), table3)(_)
    }

    executeQ(s"select * from $table1 t1, $table3 t2 where t1.col1 = t2.col1  ") {
      CreateIndexTest.validateIndex(Seq(index2), table3)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") {
      CreateIndexTest.validateIndex(Seq(index4), table2)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2 where t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") {
      CreateIndexTest.validateIndex(Seq(index4), table2)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2 where t1.col2 = t2.col3 " +
        s"and t1.col3 = t2.col2 ") {
      CreateIndexTest.validateIndex(Nil, table1, table2)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table2 t1 join $table3 t2 on t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") { df => // tab2 vs index31
      CreateIndexTest.validateIndex(Seq(index31), table2)(df)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table3 t2 on t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") { df => // index4 vs index31
      CreateIndexTest.validateIndex(Seq(index31, index4))(df)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table3 t2 on t1.col2" +
        s" = t2.col2 and t1.col3 = t2.col3 ") {
      CreateIndexTest.validateIndex(Seq(index31, index4))(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 /*+ index( ) */ join $table3 t2 on t1.col2" +
        s" = t2.col2 and t1.col3 = t2.col3 ") {
      CreateIndexTest.validateIndex(Nil, table1, table3)(_)
    }

    executeQ(s"select * from $table1 /*+ ${QueryHint.Index}($index1) */, $table3 " +
        s"where $table1.col1 = $table3.col1") {
      CreateIndexTest.validateIndex(Seq(index1), table3)(_)
    }

    executeQ(s"select * from $table1 t1 /*+ ${QueryHint.Index}($index1) */, $table3 t2 " +
        s"where t1.col1 = t2.col1") {
      CreateIndexTest.validateIndex(Seq(index1), table3)(_)
    }

    executeQ(s"select * from $table1 /*+ ${QueryHint.Index}($index1) */ as t1, $table3 t2 " +
        s"where t1.col1 = t2.col1") {
      CreateIndexTest.validateIndex(Seq(index1), table3)(_)
    }

    executeQ(s"select * from $table1 tab1 join $table2 tab2 on tab1.col2 = tab2.col2") {
      CreateIndexTest.validateIndex(Nil, table1, table2)(_)
    }

    try {
      executeQ(s"select * from (select * from $table1 ) t1 /*+ ${QueryHint.Index}($index1) */" +
          s", $table3 t2 where t1.col1 = t2.col1")
      fail(s"exepected AnalysisException as ${QueryHint.Index}" +
          s" hint cannot be specified on derived tables")
    } catch {
      case ae: AnalysisException if ae.message.contains(s"${QueryHint.Index}") =>
      // okay correct message.
      case e: Throwable => alert(e.getMessage)
        throw e
    }
  }

  test("Test two table joins corner cases") {

    val table1 = "tabOne"
    val table2 = "tabTwo"
    val table3 = "tabThree"

    val index1 = s"${table1}_IdxOne"
    val index2 = s"${table1}_IdxTwo"

    val index31 = s"${table3}_IdxOne"

    val snContext = context.get

    createBase3Tables(snContext, table1, table2, table3)

    doPrint("Verify index create and drop for various index types")
    snContext.sql(s"create index $index1 on $table1 " +
        s" (COL1) Options (colocate_with  '$table3')")
    indexesToDrop += index1

    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")
    indexesToDrop += index2

    snContext.sql(s"create index $index31 on $table3 " +
        s" (COL2, COL3) Options (colocate_with  '$index2')")
    indexesToDrop += index31

    val executeQ = CreateIndexTest.QueryExecutor(snContext)

    executeQ(s"select * from $table1 tab1 join $table3 tab2 on tab1.col1 = tab2.col1 " +
        s"where tab1.col1 = 111 ") {
      CreateIndexTest.validateIndex(Seq(index1), table3)(_)
    }

    executeQ(s"select $table1.col2, $table3.col3 from $table1, $table3 " +
        s"where $table1.col1 = $table3.col1 and $table3.col1 = 111 ") {
      CreateIndexTest.validateIndex(Seq(index1), table3)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"where t1.col3 = t2.col3 ", true) {
      CreateIndexTest.validateIndex(Seq(index2), table2)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ") { df =>
      // CreateIndexTest.validateIndex(Seq(index2", table2"))(_)
      val msg = "TODO:SB: Fix this "
      logInfo(msg)
      info(msg)
    }
  }

  test("Test choice of index for 3 or more table joins") {
    val (table1, table2, table3, table4, rtable5, rtable6) = ("T_one", "T_two",
        "T_three", "T_four", "R_one", "R_two")

    val snContext = context.get

    def createBaseTables(): Unit = {
      snContext.sql(s"drop table if exists $table1")

      val data = Seq(Seq(1, "aaa", "v1", "p1"),
        Seq(2, "bbb", "v2", "p2"),
        Seq(3, "aaa", "v1", "p3"),
        Seq(4, "bbb", "v2", "p1"),
        Seq(5, "ccc", "v3", "p2"),
        Seq(6, "ccc", "v3", "p3")
      )

      val rdd = sc.parallelize(data, data.length).map(s =>
        new Data3(s(0).asInstanceOf[Int], s(1).asInstanceOf[String],
          s(2).asInstanceOf[String], s(3).asInstanceOf[String]))
      val dataDF = snContext.createDataFrame(rdd)

      val props = Map(
        "PARTITION_BY" -> "col1")

      snContext.createTable(s"$table2", "column", dataDF.schema,
        props + ("PARTITION_BY" -> "col2, col3"))
      tablesToDrop += table2

      snContext.createTable(s"$table3", "column", dataDF.schema,
        (props + ("PARTITION_BY" -> "col1, col3")))
      tablesToDrop += table3

      snContext.createTable(s"$table4", "column", dataDF.schema,
        (props + ("PARTITION_BY" -> "col4")))
      tablesToDrop += table4

      snContext.createTable(s"$rtable5", "row", dataDF.schema,
        (props -- Seq("PARTITION_BY") + ("REPLICATE" -> "true")))
      tablesToDrop += rtable5

      snContext.createTable(s"$rtable6", "row", dataDF.schema,
        (props -- Seq("PARTITION_BY") + ("REPLICATE" -> "true")))
      tablesToDrop += rtable6

      dataDF.write.format("column").options(props).saveAsTable(table1)
      tablesToDrop += table1

      dataDF.write.insertInto(table2)
      dataDF.write.insertInto(table3)
      dataDF.write.insertInto(table4)
      dataDF.write.insertInto(rtable5)
      dataDF.write.insertInto(rtable6)
    }


    createBaseTables()

    val index1 = s"${table1}_Idx_One"
    val index2 = s"${table1}_Idx_Two"
    val index3 = s"${table1}_Idx_Three"

    doPrint("Verify index create and drop for various index types")

    snContext.sql(s"create index $index1 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")
    indexesToDrop += index1

    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL1, COL3) Options (colocate_with  '$table3')")
    indexesToDrop += index2

    snContext.sql(s"create index $index3 on $table1 " +
        s" (COL4) Options (colocate_with  '$table4')")
    indexesToDrop += index3

    val executeQ = CreateIndexTest.QueryExecutor(snContext, false, false)
    /*
    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2, $table4 t4 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 " +
        s"and t1.col4 = t4.col4 and t1.col1 = t4.col2 ")
*/

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ") {
      CreateIndexTest.validateIndex(Seq(index1), table2)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2, $table3 t3 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 " +
        s"and t1.col1 = t3.col1 and t1.col3 = t3.col3") {
      // t1 -> t2, t1 -> t3
      CreateIndexTest.validateIndex(Seq(index1), table2, table3)(_)
    }

    executeQ(s"select t1.col2, t4.col3 from $table1 t1, $table4 t4 " +
        s"where t1.col4 = t4.col4 and t1.col1 = t4.col2 ") {
      // still results into shuffle.
      CreateIndexTest.validateIndex(Seq(index3), table4)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2, $table4 t4 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 " +
        s"and t1.col4 = t4.col4 ") {
      // t1 -> t2, t1 -> t4
      CreateIndexTest.validateIndex(Seq(index1), table2, table4)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table4 t4, $table2 t2 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 " +
        s"and t1.col4 = t4.col4 ") {
      // t1 -> t4, t1 -> t2
      CreateIndexTest.validateIndex(Seq(index1), table2, table4)(_)
    }

    executeQ(s"select t1.col4, xx.col5 from $table1 t1, " +
        s"(select t4.col1 as col5 from $table4 t4) xx, " +
        s"$table2 t2 where t1.col2 = t2.col2 and t1.col3 = t2.col3 " +
        s"and t1.col4 = xx.col5 ") {
      // t1 -> t4, t1 -> t2
      CreateIndexTest.validateIndex(Seq(index1), table2, table4)(_)
    }

    executeQ(s"select t1.col4, xx.col5 from $table1 t1, " +
        s"(select t4.col1 as col5, col2, col3 from $table4 t4) xx, " +
        s"$table2 t2 where xx.col2 = t2.col2 and xx.col3 = t2.col3 " +
        s"and t1.col4 = xx.col5 ") {
      // t1 -> t4, t2 -> t4
      CreateIndexTest.validateIndex(Nil, table1, table2, table4)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table4 t4, $rtable5 t5, $table2 t2 " +
        s"where t1.col4 = t4.col4 " +
        s"and t1.col2 = t5.col2 and t1.col3 = t5.col3 " +
        s"and t5.col2 = t2.col2 and t5.col3 = t2.col3 ") {
      // t1 -> t4, t1 -> t5 -> t2
      CreateIndexTest.validateIndex(Seq(index3), rtable5, table2, table4)(_)
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $rtable5 t5, $table2 t2 " +
        s"where t1.col2 = t5.col2 and t1.col3 = t5.col3 " +
        s"and t5.col2 = t2.col2 and t5.col3 = t2.col3 ") {
      // t1 -> t4, t1 -> t5 -> t2
      CreateIndexTest.validateIndex(Seq(index1), rtable5, table2)(_)
    }

    // ReplicateWithFilters
    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $rtable5 t5, $table2 t2 " +
        s"where t1.col2 = t5.col2 and t1.col3 = t5.col3 " +
        s"and t5.col2 = t2.col2 and t5.col3 = t2.col3 " +
        s"and t5.col4 = 'p1' ") { df => // t1 -> t4, t1 -> t5 -> t2
      CreateIndexTest.validateIndex(Seq(index1), rtable5, table2)(df)
      val leaf = df.queryExecution.sparkPlan.collectFirst({ case l: HashJoinExec => l }).
          getOrElse(fail("HashJoin not found"))
      leaf.find({
        case p: PartitionedPhysicalScan => p.relation match {
          case r: RowFormatRelation if r.table.indexOf(rtable5.toUpperCase) > 0 => true
          case _ => false
        }
        case _ => false
      }).getOrElse(fail(s"$rtable5 not found to be applied first."))
    }

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $rtable6 t6, $rtable5 t5, $table2 t2 " +
        s"where t1.col2 = t5.col2 and t1.col3 = t5.col3 " +
        s"and t5.col4 = t6.col4 " +
        s"and t6.col2 = t2.col2 and t6.col3 = t2.col3 " +
        s"and t5.col4 = 'p1' ", true) { df => // t1 -> t4, t1 -> t5 -> t2
      CreateIndexTest.validateIndex(Seq(index1), rtable5, rtable6, table2)(df)
      val leaf = df.queryExecution.sparkPlan.collectFirst({ case l: HashJoinExec => l }).
          getOrElse(fail("HashJoin not found"))
      leaf.find({
        case p: PartitionedPhysicalScan => p.relation match {
          case r: RowFormatRelation if r.table.indexOf(rtable5.toUpperCase) > 0 => true
          case _ => false
        }
        case _ => false
      }).getOrElse(fail(s"$rtable5 not found to be applied first."))
    }
  }


  test("Test append table joins") {
    val table1 = "tabOne"
    val table2 = "tabTwo"
    val table3 = "tabThree"

    val index1 = s"${table1}_IdxOne"
    val index2 = s"${table1}_IdxTwo"

    val index31 = s"${table3}_IdxOne"

    val snContext = context.get
    // Property.ColumnBatchSize.set(snContext.conf, 30)

    createBase3Tables(snContext, table1, table2, table3)

    doPrint("Verify index create and drop for various index types")
    snContext.sql(s"create index $index1 on $table1 " +
        s" (COL1) Options (colocate_with  '$table3')")
    indexesToDrop += index1

    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")
    indexesToDrop += index2

    snContext.sql(s"create index $index31 on $table3 " +
        s" (COL2, COL3) Options (colocate_with  '$index2')")
    indexesToDrop += index31

    val data = Seq(Seq(777, "aaa", "hello"),
      Seq(888, "bbb", "halo"),
      Seq(999, "aaa", "hello"),
      Seq(101010, "bbb", "halo"),
      Seq(111111, "ccc", "halo"),
      Seq(121212, "ccc", "halo")
    )

    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(table1)

    val executeQ = CreateIndexTest.QueryExecutor(snContext, false, false)

    val selDF = executeQ(s"select * from $table1") {
      CreateIndexTest.validateIndex(Nil, s"$table1")(_)
    }

    val baseRows = selDF.collect().toSet
    executeQ(s"select * from $table1 --+${QueryHint.Index}($index1)") { df =>
      CreateIndexTest.validateIndex(Seq(index1))(df)
      assert(df.collect().toSet.equals(baseRows))
    }
  }

  test("Test create Index on Row Table using Snappy API") {
    val snContext = context.get
    val tableName: String = "trow1"
    val props = Map("PARTITION_BY" -> "col2")

    val data = Seq(Seq(111, "aaaaa"), Seq(222, ""))
    val rdd = sc.parallelize(data, data.length).
        map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snContext.sql("create unique index uidx on " + tableName + " (COL1)")
    // drop it
    snContext.sql("drop index uidx")
    // first try and create a unique index on col1 -- SNAP-1385
    val tableNameRowReplicated = s"${tableName}_rep"
    snContext.createTable(tableNameRowReplicated,
      "row", dataDF.schema, Map.empty[String, String])
    snContext.sql(s"create unique index uidx_rep on ${tableNameRowReplicated} (COL1)")
    snContext.sql("drop index uidx_rep")

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create unique index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create global hash index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")


    snContext.createIndex("test1", tableName, Seq("col1" -> None), Map("index_type" -> "unique"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName,
      Seq("col1" -> None), Map("index_type" -> "global hash"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Seq("col1" -> None),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName,
      Seq("col1" -> Some(Descending)), Map("index_type" -> "unique"))
    snContext.dropIndex("test1", false)

    // drop non-existent indexes with if exist clause
    snContext.dropIndex("test1", true)
    snContext.sql("drop index if exists test1")


    doPrint("Create Index - Start")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    doPrint("Create Index - Done")

    // TODO fails if column name not in caps
    val result = snContext.sql("select COL1 from " +
        tableName +
        " where COL2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect.foreach(doPrint)
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")

    doPrint("Drop Index - Start")
    snContext.sql("drop index test1")
    doPrint("Drop Index - Done")
  }

  def verifyRows(r: Row): Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  def doPrint(s: Any): Unit = {
    // println(s)
  }
}

object CreateIndexTest extends SnappyFunSuite {

  case class QueryExecutor(snContext: SnappyContext,
      showResults: Boolean = false,
      withExplain: Boolean = false) {

    def apply(sqlText: String, explainQ: Boolean = false)
        (implicit validate: DataFrame => Unit = _ => ()): DataFrame = {
      val msg = s"Executing $sqlText"
      logInfo(msg) // log it
      info(msg) // inform it

      val selectRes = snContext.sql(sqlText)

      if (withExplain || explainQ) {
        // selectRes.explain(true)
      }

      validate(selectRes)

      if (showResults) {
        logInfo(selectRes.collect().take(20).mkString("\n"))
      } else {
        logInfo(selectRes.collect().take(10).mkString("\n"))
      }

      selectRes
    }

  }

  def validateIndex(index: Seq[String], tables: String*)(df: DataFrame): Unit = {
    val (indexesMatched, indexesUnMatched) = df.queryExecution.optimizedPlan.collect {
      case l@LogicalRelation(idx: IndexColumnFormatRelation, _, _) => idx
    }.partition(rel => index.exists(i => rel.table.indexOf(i.toUpperCase) > 0))

    if (indexesMatched.size != index.size) {
      df.explain(true)
      fail(s"Expected index selection ${index.mkString(",")}, but found" +
          s" ${indexesUnMatched.mkString(",")}")
    }

    val tablesAppeared = df.queryExecution.optimizedPlan.collect {
      case l@LogicalRelation(columnTable: ColumnFormatRelation, _, _) => columnTable.table
      case l@LogicalRelation(rowTable: RowFormatRelation, _, _) => rowTable.table
    }

    val (tablesFound, tablesNotFound) = tables.partition(tab =>
      tablesAppeared.exists(t => t.indexOf(tab.toUpperCase) > 0))

    val unexpected = tablesAppeared.filterNot(tab =>
      tables.exists(t => tab.indexOf(t.toUpperCase) > 0))

    if (tablesFound.size != tables.size) {
      df.explain(true)
      fail(s"Expected tables ${tables.mkString(",")} but not found " +
          s"${tablesNotFound.mkString(",")} with additional tables ${unexpected.mkString(",")}")
    }
  }

}
