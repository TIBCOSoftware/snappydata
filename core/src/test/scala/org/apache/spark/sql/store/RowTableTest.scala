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

import java.sql.SQLException

import scala.util.{Failure, Success, Try}

import io.snappydata.SnappyFunSuite
import io.snappydata.core.{Data, TRIPDATA}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql._
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types.{IntegerType, StructField}

/**
 * Tests for ROW tables.
 */
class RowTableTest
    extends SnappyFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val tableName: String = "RowTable"

  val props = Map.empty[String, String]

  after {
   snc.dropTable(tableName, ifExists = true)
    snc.dropTable("RowTable2", ifExists = true)
  }

  test("Test the creation/dropping of row table using Schema") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql("create schema my_schema")
    dataDF.write.format("row").saveAsTable("MY_SCHEMA.MY_TABLE")
    var result = snc.sql("SELECT * FROM MY_SCHEMA.MY_TABLE" )
    var r = result.collect
    logInfo(r.length.toString)

    snc.sql("drop table MY_SCHEMA.MY_TABLE" )
    snc.sql("drop schema MY_SCHEMA")

    logInfo("Successful")
  }


  test("Test the creation/dropping of row table using Snappy API") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    logInfo("Successful")
  }

  test("Test the fetch first n row only test. with and without n parameter") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)
    var result = snc.sql("SELECT * FROM " + tableName + " fetch first 4 row only ")
    var r = result.collect
    assert(r.length == 0)

    result = snc.sql("SELECT * FROM " + tableName + " fetch first row only")
    r = result.collect
    assert(r.length == 0)
    logInfo("Successful")
  }

  test("Test the creation of table using DataSource API") {

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("row").options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    logInfo("Successful")
  }

  test("Test the creation of table using DataSource API(PUT)") {

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    intercept[AnalysisException] {
      dataDF.write.putInto(tableName)
    }
    dataDF.write.format("row").options(props).saveAsTable(tableName)

    // Again do putInto, as there is no primary key, all will be appended
    dataDF.write.format("row").mode(SaveMode.Overwrite).options(props).putInto(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    // no primary key
    assert(r.length == 10)
    logInfo("Successful")
  }


  test("Test the creation of table using Snappy API and then append/ignore/overwrite/upsert" +
      " DF using DataSource API") {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    var rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    var dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)

    intercept[AnalysisException] {
      dataDF.write.format("row").mode(SaveMode.ErrorIfExists).
      options(props).saveAsTable(tableName)
    }
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 5)

    // Ignore if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300),
      Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("row").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 5)

    // Append if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300),
      Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 11)

    // Overwrite if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300),
      Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("row").mode(SaveMode.Overwrite).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 6)

    logInfo("Successful")
  }

  val options = "OPTIONS (PARTITION_BY 'Col1')"
  val optionsWithURL = "OPTIONS (PARTITION_BY 'Col1', URL 'jdbc:snappydata:;')"

  test("Test the creation/dropping of table using SQL") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    logInfo("Successful")
  }

  test("Test the creation/dropping of table using SQ with explicit URL") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        optionsWithURL
    )
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    logInfo("Successful")
  }

  test("Test the creation using SQL and insert a DF in append/overwrite/errorifexists mode") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    intercept[AnalysisException] {
      dataDF.write.format("row").mode(SaveMode.ErrorIfExists).options(props).saveAsTable(tableName)
    }

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    logInfo("Successful")
  }

  test("Test the creation using SQL and put a DF in append/overwrite/errorifexists mode") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("row").mode(SaveMode.Ignore).options(props).putInto(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    logInfo("Successful")
  }

  test("Test the creation using SQL and put a seq of rows in append/overwrite/errorifexists mode") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7), Seq(1,100,200))
    data.map { r =>
      snc.put(tableName, Row.fromSeq(r))
    }
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    logInfo("Successful")
  }

  // should throw exception if primary key is getting updated?
  test("Test Creation using SQL with Primary Key and PUT INTO") {
    snc.sql("CREATE TABLE " + tableName + " (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7),Seq(1, 200, 300))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.putInto(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)

    // check if the row against primary key 1 is 1, 200, 300

    val row1 = snc.sql(s"SELECT * FROM $tableName WHERE Col1='1'").collect()
    assert(row1.length == 1)

    logInfo(row1.mkString("\n"))

    logInfo("Successful")
  }

  test("Test Creation using SQL with Primary Key and PUT INTO SELECT AS ") {
    snc.sql("CREATE TABLE tempTable  (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val data1 = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7),Seq(1, 200, 300))

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("tempTable")
    val result1 = snc.sql("SELECT * FROM tempTable")
    val r1 = result1.collect
    assert(r1.length == 6)

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) " + " USING row " +
        options)

    val rdd1 = sc.parallelize(data1, data1.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF1 = snc.createDataFrame(rdd1)

    dataDF1.write.format("row").mode(SaveMode.Overwrite).options(props).saveAsTable(tableName)

    snc.sql("PUT INTO TABLE " + tableName + " SELECT * FROM tempTable")


    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)

    // check if the row against primary key 1 is 1, 200, 300

    val row1 = snc.sql(s"SELECT * FROM $tableName WHERE Col1='1'").collect()
    assert(row1.length == 1)

    logInfo(row1.mkString("\n"))
    snc.dropTable("tempTable")

    logInfo("Successful")
  }

  test("PUT INTO TABLE USING SQL"){
    snc.sql("CREATE TABLE " + tableName + " (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) " + " USING row " +
        options)
    snc.sql("PUT INTO " + tableName + " VALUES(1,11, 111)")
    snc.sql("PUT INTO " + tableName +  " VALUES(2,11, 111)")
    snc.sql("PUT INTO " + tableName + " VALUES(3,11, 111)")



    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    // just update a row
    snc.sql("PUT INTO " + tableName + " VALUES(3,111, 1111)")
    assert(snc.sql("SELECT * FROM " + tableName).collect.length == 3)
  }

  test("PUT INTO TABLE USING SQL with COLUMN NAME"){
    snc.sql("CREATE TABLE " + tableName + " (Col1 INT NOT NULL PRIMARY KEY, Col2 INT, Col3 INT) " + " USING row " +
        options)
    snc.sql("PUT INTO " + tableName + " (Col1, Col2, Col3) VALUES(1,11, 111)")
    snc.sql("PUT INTO " + tableName +  " (Col1, Col2, Col3)  VALUES(2,11, 111)")
    snc.sql("PUT INTO " + tableName + " (Col1, Col2, Col3)  VALUES(3,11, 111)")


    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    // just update a row
    snc.sql("PUT INTO " + tableName + " (Col1, Col2, Col3) VALUES(3,111, 1111)")
    assert(snc.sql("SELECT * FROM " + tableName).collect.length == 3)
  }

  test("Test the creation of table using SQL and SnappyContext ") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    intercept[AnalysisException] {
      snc.createTable(tableName, "row", dataDF.schema, props)
    }

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    logInfo("Successful")
  }

  test("Test the creation of table using CREATE TABLE AS STATEMENT ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val tableName2 = "RowTable2"
    snc.sql("DROP TABLE IF EXISTS RowTable2")
    snc.sql("CREATE TABLE " + tableName2 + " USING row " +
        options + " AS (SELECT * FROM " + tableName + ")"
    )
    var result = snc.sql("SELECT * FROM " + tableName2)
    var r = result.collect
    assert(r.length == 5)

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName2)
    result = snc.sql("SELECT * FROM " + tableName2)
    r = result.collect
    assert(r.length == 10)

    snc.dropTable(tableName2)
    logInfo("Successful")
  }

  test("Test alter table SQL syntax") {
    snc.sql("drop table if exists employee")
    snc.sql("create table employee(name string, surname string)")
    assert (snc.sql("select * from employee").schema.fields.length == 2)
    snc.sql("alter table employee add column age int")
    assert (snc.sql("select * from employee").schema.fields.length == 3)
    snc.sql("alter table employee drop column surname")
    assert (snc.sql("select * from employee").schema.fields.length == 2)
    snc.sql("insert into employee values ('a' , 1)")
    assert (snc.sql("select * from employee").count == 1)
    intercept[TableNotFoundException] {
      snc.sql("alter table non_employee add column age int")
    }
    intercept[SQLException] {  // existing column 'age'
      snc.sql("alter table employee add column age int")
    }
    intercept[SQLException] { // non-existing column
      snc.sql("alter table employee drop column surname")
    }
    snc.sql("alter table employee add column dateCol date")
    snc.sql("alter table employee add column timeCol timestamp")
    assert (snc.sql("select * from employee").schema.fields.length == 4)
    snc.sql("alter table employee drop column dateCol")
    snc.sql("alter table employee drop column timeCol")
    assert (snc.sql("select * from employee").schema.fields.length == 2)

    snc.sql("create index emp_age on employee (age)")
    intercept[SQLException] {  // because INDEX 'EMP_AGE' is dependent on that object
      snc.sql("alter table employee drop column age")
    }
    assert (snc.sql("select * from employee").schema.fields.length == 2)
    snc.sql("insert into employee values ('b', 2)")
    assert (snc.sql("select * from employee").count == 2)
    //covers SNAP-2269
    snc.sql("alter table employee add column eID int not null default -9999")
    assert (snc.sql("select * from employee").count == 2)
    assert (snc.sql("select * from employee").schema.fields.length == 3)
    snc.sql("insert into employee values ('c', 2, 111)")
    assert (snc.sql("select * from employee").count == 3)
    assert (snc.sql("select * from employee").schema.fields.length == 3)
    snc.sql("alter table employee add column deptID int default -999")
    assert (snc.sql("select * from employee").count == 3)
    assert (snc.sql("select * from employee").schema.fields.length == 4)
  }

  test("Test alter table API SnappySession") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.alterTable(tableName, true, StructField("col4", IntegerType, true))
    assert(snc.sql("SELECT * FROM " + tableName).schema.fields.length == 4)
    snc.alterTable(tableName, false, StructField("col3", IntegerType, true))
    assert(snc.sql("SELECT * FROM " + tableName).schema.fields.length == 3)
  }

  test("SNAP-1825") {
    snc.sql("create table tabOne(id int, name String, address String)" +
      " USING row OPTIONS(partition_by 'id')")
    snc.sql("insert into tabOne values(111, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(222, 'bbb', 'halo')")
    snc.sql("insert into tabOne values(333, 'aaa', 'hello')")
    snc.sql("insert into tabOne values(444, 'bbb', 'halo')")
    snc.sql("insert into tabOne values(555, 'ccc', 'halo')")
    snc.sql("insert into tabOne values(666, 'ccc', 'halo')")
    assert(snc.sql("select * from tabOne").collect().length == 6)
    snc.sql("ALTER TABLE tabOne ADD city String")
    snc.sql("insert into tabOne values(777, 'ddd', 'halo', 'Pune')")
    assert (snc.sql("select id, name from tabOne where city='Pune'").collect().length == 1)
  }

  test("Test the truncate syntax SQL and SnappyContext") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.truncateTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 0)

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    snc.sql("TRUNCATE TABLE " + tableName)

    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 0)

    logInfo("Successful")
  }

  test("Test the drop syntax SnappyContext and SQL ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.dropTable(tableName, true)

    intercept[AnalysisException] {
      snc.dropTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    logInfo("Successful")
  }

  test("Test the drop syntax SQL and SnappyContext ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    intercept[AnalysisException] {
      snc.dropTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.dropTable(tableName, true)

    logInfo("Successful")
  }

  test("Test the update table ") {
    snc.sql("CREATE TABLE RowTableUpdate(CODE INT,DESCRIPTION varchar(100))" +
        "USING row " +
        "options()")

    snc.sql("insert into RowTableUpdate values (5,'test')")
    snc.sql("insert into RowTableUpdate values (6,'test1')")

    val df1 = snc.sql("select DESCRIPTION from RowTableUpdate where DESCRIPTION='test'")
    assert(df1.count() == 1)

    val d1 = snc.sql("select * from  RowTableUpdate")

    snc.sql("CREATE TABLE RowTableUpdate2 " +
        "USING row " +
        "options() AS (select * from  RowTableUpdate)")

    val d2 = snc.sql("select * from  RowTableUpdate2")
    assert(d2.count() == 2)

    snc.sql("update RowTableUpdate2 set DESCRIPTION ='No#complaints' where CODE = 5")

    val df2 = snc.sql("select DESCRIPTION from RowTableUpdate2 where DESCRIPTION = 'No#complaints' ")
    assert(df2.count() == 1)

    val df3 = snc.sql("select DESCRIPTION from RowTableUpdate2 where DESCRIPTION  in ('No#complaints', 'test1') ")
    assert(df3.count() == 2)

    snc.dropTable("RowTableUpdate")
    snc.dropTable("RowTableUpdate2")
    logInfo("Successful")
  }


  test("Test row Incorrect option") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE27")

    Try(snc.sql("CREATE TABLE ROW_TEST_TABLE27(OrderId INT ,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITIONBY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")) match {
      case Success(df) => throw new AssertionError(" Should not have succedded with incorrect options")
      case Failure(error) => // Do nothing
    }

  }

  test("Test Null varchar value for row table inserts") {
    snc.sql(
      s"""CREATE TABLE NYCTAXI (MEDALLION VARCHAR(100) NOT NULL PRIMARY KEY,
            HACK_LICENSE VARCHAR(100),
            VENDOR_ID VARCHAR(100),
            RATE_CODE INTEGER,
            STORE_AND_FWD_FLAG VARCHAR(100),
            PICKUP_DATETIME VARCHAR(100),
            DROPOFF_DATETIME VARCHAR(100),
            PASSENGER_COUNT INTEGER,
            TRIP_TIME_IN_SECS INTEGER,
            TRIP_DISTANCE DOUBLE PRECISION,
            PICKUP_LONGITUDE DOUBLE PRECISION,
            PICKUP_LATITUDE DOUBLE PRECISION,
            DROPOFF_LONGITUDE DOUBLE PRECISION,
            DROPOFF_LATITUDE DOUBLE PRECISION
            ) USING ROW OPTIONS (PARTITION_BY 'MEDALLION', BUCKETS '8')
        """)

    val rdd = sc.parallelize(
      (1 to 2000), 5).map(i => TRIPDATA(
      "23A89BC906FBB8BD110677FBB0B0A6C5" + i,
      if (i % 100 == 0) "HACK_LICENSE_" + i else null,
      if (i % 200 == 0) "VENDOR_ID" + i else "",
      156,
      "STORE_AND_FWD_FLAG" + i,
      "PICKUP_DATETIME" + i,
      "2003-12-01 23:11:12",
      10,
      2000,
      -20.000879,
      20.0,
      -20.00045,
      12.0,
      12.0
    )
    )

    val csvDF = snc.createDataFrame(rdd)

    csvDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("NYCTAXI")

    val cnts = snc.sql("select * from NYCTAXI").count()

    assert(cnts === 2000)

  }

  test("Test insert into select from  ") {

    snc.sql("DROP table if exists row_tab1");
    snc.sql("DROP table if exists row_tab2");
    snc.sql("DROP table if exists col_tab1");
    snc.sql("DROP table if exists col_tab2");

    snc.sql("create table row_tab1(col1 Integer,col2 Integer)");
    snc.sql("insert into row_tab1 values(1,2)");
    snc.sql("create table row_tab2 (col1 Integer,col2 Integer) ");
    snc.sql("create table col_tab1 (col1 Integer,col2 Integer) USING COLUMN options(buckets '8')");
    snc.sql("insert into col_tab1 values(1,2)");
    snc.sql("create table col_tab2 (col1 Integer,col2 Integer) USING COLUMN options(buckets '8')");

    //inserting the data to row table from row table
    snc.sql("insert into row_tab2 select * from row_tab1")
    val df1 = snc.sql("select * from row_tab2")
    assert(df1.count() === 1)


    //inserting into column table from row_table
    snc.sql("insert into col_tab1 select * from row_tab1")
    val df2 = snc.sql("select * from col_tab1")
    //Row count will be 2 as we have already inserted a row after creating the table
    assert(df2.count() === 2)


    //inserting the data to row table from row table
    snc.sql("insert into row_tab2 select * from col_tab1")
    val df3 = snc.sql("select * from row_tab2")
    assert(df3.count() === 3)

    //inserting into column table from column table
    snc.sql("insert into col_tab2 select * from col_tab1")
    val df4 = snc.sql("select * from col_tab2")
    assert(df4.count() === 2)

  }


  test("Test the creation of table using CREATE TABLE AS STATEMENT without specifying USING..OPTIONS") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val tableName2 = "RowTable2"
    snc.sql("DROP TABLE IF EXISTS RowTable2")
    snc.sql("CREATE TABLE " + tableName2 + " AS (SELECT * FROM " + tableName + ")"
    )
    var result = snc.sql("SELECT * FROM " + tableName2)
    var r = result.collect
    assert(r.length == 5)

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName2)
    result = snc.sql("SELECT * FROM " + tableName2)
    r = result.collect
    assert(r.length == 10)

    snc.dropTable(tableName2)
    logInfo("Successful")
  }

  test("Test create table from CSV without header- SNAP-1442") {
    snc.sql(s"create external table t1 using csv options(path '${(getClass.getResource("/northwind/regions"+
      ".csv").getPath)}', header 'true', inferschema 'true')")
    snc.sql("CREATE TABLE t2 (RegionID int, RegionDescription string) USING row OPTIONS(PERSISTENT 'async') AS " +
      "(SELECT RegionID, RegionDescription FROM t1)")

    val df2 = snc.sql("select * from t1")
    assert(df2.count()==4)
    snc.sql("DROP table t2")
  }

  test("Test Long Datatype for Row table - SNAP-1722") {
    // Also test long varchar to see if its not breaking the previous implementation
    snc.sql("create table table1 (col1  long, col2 Long,col3 short,col4  TINYINT, col5 " +
      "BYTE, col6 SMALLINT, primary key (col1, col2))" +
      " using row options( partition_by 'col1,col2', buckets '8')")
    for (i <- 1 to 100) {
      snc.sql(s"insert into table1 values($i,${i + 1},1,1,1,1)")
    }
    val cnt = snc.sql("select * from table1").count
    snc.sql("drop table table1")
    assert(cnt == 100, s"Expected count is 100 but actual count is $cnt")
  }

  test("create table without explicit schema (SNAP-2047)") {
    val hfile = getClass.getResource("/2015.parquet").getPath
    val session = this.snc

    session.createExternalTable("staging_airline", "parquet", Map("path" -> hfile))
    session.sql("create table airline using row options(partition_by 'FlightNum') " +
        "AS (SELECT * FROM staging_airline limit 20000)")
    assert(session.table("airline").count() === 20000)

    session.sql("drop table airline")
  }

    test("Test method for getting primary keys of row tables") {
        var session = new SnappySession(snc.sparkContext)
        session.sql("drop table if exists temp1")
        session.sql("drop table if exists temp2")
        session.sql("drop table if exists temp3")

        session.sql("create table temp1(id1 bigint not null primary key , name1 varchar(10)) ")
        session.sql("create table temp2(id1 bigint not null , name1 varchar(10)) ")
        session.sql("create table temp3(id1 bigint not null , name1 varchar(10), " +
            "id3 bigint not null, id2 bigint not null, constraint netw_pk primary key (id2, id1)) ")

        val res1 = session.sessionCatalog.getKeyColumns("temp1")
        assert(res1.size == 1)

        val res2 = session.sessionCatalog.getKeyColumns("temp2")
        assert(res2.size == 0)

        val res3 = session.sessionCatalog.getKeyColumns("temp3")
        assert(res3.size == 2)

        Try(session.sessionCatalog.getKeyColumns("temp5")) match {
            case Success(_) => throw new AssertionError(
                "Should not have succedded with incorrect options")
            case Failure(_) => // Do nothing
        }
    }
}
