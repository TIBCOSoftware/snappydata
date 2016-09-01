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
package io.snappydata.app

import io.snappydata.{Constant, SnappyFunSuite}

import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SnappyContext}

class CreateIndexTest extends SnappyFunSuite {

  test("Test create Index on Column Table using Snappy API") {
    val tableName: String = "tcol1"
    val snContext = SnappyContext(sc)

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

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("create index test2 on " + tableName +
        s" (COL1) Options (colocate_with  '$tableName')")

    try {
      snContext.sql(s"drop table $tableName")
      fail("This should fail as there are indexes associated with this table")
    } catch {
      case e: Throwable =>
    }
    snContext.sql("drop index test1")
    snContext.sql("drop index test2")
    try {
      snContext.sql("create index a1.test1 on " + tableName + " (COL1)")
      fail("This should fail as the index should have same database as the table")
    } catch {
      case e: Throwable =>
    }

    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")

    snContext.createIndex("test1", tableName,
      Map(("col1" -> None)), Map("colocate_with" -> tableName))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Map(("col1" -> None)),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName, Map(("col1" -> Some(Ascending))),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    // drop non-existent indexes with if exist clause
    snContext.dropIndex("test1", true)
    snContext.sql("drop index if exists test1")

    snContext.sql(s"drop table $tableName")
    snContext.sql(s"drop table $table2")
  }


  private def executeQuery(snContext: SnappyContext, withExplain: Boolean = false)
      (sqlText: String)(implicit validate: DataFrame => Unit = _ => ()) = {
    val selectRes = snContext.sql(sqlText)
    if (withExplain) {
      selectRes.explain(true)
    }
    selectRes.show
    if (validate != null) {
      validate(selectRes)
    }
  }

  test("Test choice of index according to where predicate") {
    val tableName = "tabOne"
    val snContext = SnappyContext(sc)

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
        new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
      val dataDF = snContext.createDataFrame(rdd)

      dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    }

    createBaseTable()

    doPrint("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
    snContext.sql(s"create index $indexOne on $tableName (COL1)")
    snContext.sql(s"create index $indexTwo on $tableName (COL2, COL3)")
    snContext.sql(s"create index $indexThree on $tableName (COL1, COL3)")

    val executeQ = executeQuery(snContext) _

    executeQ(s"select * from $tableName where col1 = 111")

    executeQ(s"select * from $tableName where col2 = 'aaa' ")

    executeQ(s"select * from $tableName where col2 = 'bbb' and col3 = 'halo' ")

    executeQ(s"select * from $tableName where col1 = 111 and col3 = 'halo' ")

    snContext.sql(s"drop index $indexOne")
    snContext.sql(s"drop index $indexTwo")
    snContext.sql(s"drop index $indexThree")
    snContext.sql(s"drop table $tableName")
  }

  test("Test two table joins") {
    val table1 = "tabOne"
    val table2 = "tabTwo"
    val table3 = "tabThree"

    val snContext = SnappyContext(sc)
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")


    def createBaseTables() : Unit = {
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

      snContext.createTable(s"$table3", "column", dataDF.schema, props)

      dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(table1)
      dataDF.write.insertInto(table2)
      dataDF.write.insertInto(table3)
    }


    createBaseTables()

    val index1 = s"${table1}_IdxOne"
    val index2 = s"${table1}_IdxTwo"
    val index3 = s"${table1}_IdxThree"
    val index4 = s"${table1}_IdxFour"

    val index31 = s"${table3}_IdxOne"

    doPrint("Verify index create and drop for various index types")
    snContext.sql(s"create index $index1 on $table1 (COL1)")
    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL1) Options (colocate_with  '$table3')")

    snContext.sql(s"create index $index3 on $table1 (COL2, COL3)")
    snContext.sql(s"create index $index4 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")

    snContext.sql(s"create index $index31 on $table3 " +
        s" (COL2, COL3) Options (colocate_with  '$index4')")

    val executeQ = executeQuery(snContext, true) _

/*
    executeQ(s"select * from $table1 tab1 " +
        s"join $table3 tab2 on tab1.col1 = tab2.col1")

    executeQ(s"select * from $table1 tab1 " +
        s"join $table3 tab2 on tab1.col1 = tab2.col1 where tab1.col1 = 111 ")


    executeQ(s"select * from $table1, $table3 " +
        s" where $table1.col1 = $table3.col1")

    executeQ(s"select $table1.col2, $table3.col3 from $table1, $table3 " +
        s"where $table1.col1 = $table3.col1 and $table3.col1 = 111 ")

    executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2 " +
    s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ")

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
    s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ")

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"where t1.col3 = t2.col3 ")
*/

    executeQ(s"select t1.col2, t2.col3 from $table2 t1 join $table3 t2 on t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") // tab2 vs index31

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table3 t2 on t1.col2 = t2.col2 " +
        s"and t1.col3 = t2.col3 ") // index4 vs index31

    snContext.sql(s"drop index $index1")
    snContext.sql(s"drop index $index2")
    snContext.sql(s"drop index $index3")
    snContext.sql(s"drop index $index31")
    snContext.sql(s"drop index $index4")

    snContext.sql(s"drop table $table1")
    snContext.sql(s"drop table $table2")
    snContext.sql(s"drop table $table3")
  }


  test("Test choice of index for 3 or more table joins") {
    val table1 = "tabOne"
    val table2 = "tabTwo"
    val table3 = "tabThree"

    val snContext = SnappyContext(sc)
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")


    def createBaseTables() : Unit = {
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
        props + ("PARTITION_BY" -> "col2, col3"))

      snContext.createTable(s"$table3", "column", dataDF.schema,
        (props + ("PARTITION_BY" -> "col1, col3")))

      dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(table1)
      dataDF.write.insertInto(table2)
      dataDF.write.insertInto(table2)
      dataDF.write.insertInto(table3)
    }


    createBaseTables()

    val index1 = s"${table1}_IdxOne"
    val index2 = s"${table1}_IdxTwo"
    val index3 = s"${table1}_IdxThree"
    val index4 = s"${table1}_IdxFour"

    doPrint("Verify index create and drop for various index types")
    snContext.sql(s"create index $index1 on $table1 " +
        s" (COL1, COL3) Options (colocate_with  '$table3')")

    snContext.sql(s"create index $index2 on $table1 " +
        s" (COL2, COL3) Options (colocate_with  '$table2')")

    val executeQ = executeQuery(snContext, true) _

    /*
        executeQ(s"select * from $table1 tab1 " +
            s"join $table3 tab2 on tab1.col1 = tab2.col1")

        executeQ(s"select * from $table1 tab1 " +
            s"join $table3 tab2 on tab1.col1 = tab2.col1 where tab1.col1 = 111 ")


        executeQ(s"select * from $table1, $table3 " +
            s" where $table1.col1 = $table3.col1")

        executeQ(s"select $table1.col2, $table3.col3 from $table1, $table3 " +
            s"where $table1.col1 = $table3.col1 and $table3.col1 = 111 ")

        executeQ(s"select t1.col2, t2.col3 from $table1 t1, $table2 t2 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ")

        executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"where t1.col2 = t2.col2 and t1.col3 = t2.col3 ")
    */

    executeQ(s"select t1.col2, t2.col3 from $table1 t1 join $table2 t2 on t1.col2 = t2.col2 " +
        s"where t1.col3 = t2.col3 ")

    snContext.sql(s"drop index $index1")
    snContext.sql(s"drop index $index2")
    snContext.sql(s"drop index $index3")
    snContext.sql(s"drop index $index4")

    snContext.sql(s"drop table $table1")
    snContext.sql(s"drop table $table2")
    snContext.sql(s"drop table $table3")
  }

  test("Test create Index on Row Table using Snappy API") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    val tableName: String = "trow1"
    val props = Map("PARTITION_BY" -> "col2")

    val data = Seq(Seq(111, "aaaaa"), Seq(222, ""))
    val rdd = sc.parallelize(data, data.length).
        map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create unique index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create global hash index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")


    snContext.createIndex("test1", tableName, Map(("col1" -> None)), Map("index_type" -> "unique"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName,
      Map(("col1" -> None)), Map("index_type" -> "global hash"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Map(("col1" -> None)),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName,
      Map(("col1" -> Some(Descending))), Map("index_type" -> "unique"))
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
    //println(s)
  }
}
