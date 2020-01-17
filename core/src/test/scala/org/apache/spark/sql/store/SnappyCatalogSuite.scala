/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter}

import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.{SnappySession, AnalysisException}
import org.apache.spark.sql.catalog.{Column, Function, Table, Database}
import org.apache.spark.sql.catalyst.{ScalaReflection, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.util.Utils

/**
 * Most of the code is copied from CatalogSuite of Spark. Necessary modification for Snappy
 */

class SnappyCatalogSuite extends SnappyFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll {

  var snappySession: SnappySession = _

  private var sessionCatalog: SessionCatalog = _

  before {
    try {
      if (sessionCatalog != null) {
        sessionCatalog.reset()
      }
      snappySession = new SnappySession(snc.sparkContext)
      sessionCatalog = snappySession.sessionState.catalog
    } finally {
      // super.afterEach()
    }
  }

  private val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    override val tableOutputFormat: String = "org.apache.hadoop.mapred.SequenceFileOutputFormat"

    override def newEmptyCatalog(): ExternalCatalog = snc.sharedState.externalCatalog
  }

  private def createDatabase(name: String): Unit = {
    sessionCatalog.createDatabase(utils.newDb(name), ignoreIfExists = false)
  }

  private def dropDatabase(name: String): Unit = {
    sessionCatalog.dropDatabase(name, ignoreIfNotExists = false, cascade = true)
  }

  private def createTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.createTable(utils.newTable(name, db), ignoreIfExists = false)
  }

  private def createTempTable(name: String): Unit = {
    sessionCatalog.createTempView(name, Range(1, 2, 3, 4), overrideIfExists = true)
  }

  private def dropTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.dropTable(TableIdentifier(name, db), ignoreIfNotExists = false, purge = false)
  }

  private def createFunction(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.createFunction(utils.newFunc(name, db), ignoreIfExists = false)
  }

  private def createTempFunction(name: String): Unit = {
    val info = new ExpressionInfo("className", name)
    val tempFunc = (e: Seq[Expression]) => e.head
    sessionCatalog.createTempFunction(name, info, tempFunc, ignoreIfExists = false)
  }

  private def dropFunction(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.dropFunction(FunctionIdentifier(name, db), ignoreIfNotExists = false)
  }

  private def dropTempFunction(name: String): Unit = {
    sessionCatalog.dropTempFunction(name, ignoreIfNotExists = false)
  }

  private def testListColumns(tableName: String, dbName: Option[String]): Unit = {
    val tableMetadata = sessionCatalog.getTableMetadata(TableIdentifier(tableName, dbName))
    val columns = dbName
        .map { db => snappySession.catalog.listColumns(db, tableName) }
        .getOrElse {
          snappySession.catalog.listColumns(tableName)
        }
    assume(tableMetadata.schema.nonEmpty, "bad test")
    // assume(tableMetadata.partitionColumnNames.nonEmpty, "bad test")
    // assume(tableMetadata.bucketSpec.nonEmpty, "bad test")
    assert(columns.collect().map(_.name).toSet == tableMetadata.schema.map(_.name).toSet)
    /*
    columns.collect().foreach { col =>
      assert(col.isPartition == tableMetadata.partitionColumnNames.contains(col.name))
      assert(col.isBucket == tableMetadata.bucketSpec.contains(col.name))
    }
    */
  }


  test("current database") {
    assert(snappySession.catalog.currentDatabase == "app")
    assert(sessionCatalog.getCurrentDatabase == "app")
    createDatabase("my_db")
    snappySession.catalog.setCurrentDatabase("my_db")
    assert(snappySession.catalog.currentDatabase == "my_db")
    assert(sessionCatalog.getCurrentDatabase == "my_db")
    val e = intercept[AnalysisException] {
      snappySession.catalog.setCurrentDatabase("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list databases") {
    assert(snappySession.catalog.listDatabases().collect()
        .map(_.name).toSet == Set("app", "default", "sys"))
    createDatabase("my_db1")
    createDatabase("my_db2")
    assert(snappySession.catalog.listDatabases().collect().map(_.name).toSet ==
        Set("app", "my_db1", "my_db2", "default", "sys"))
    dropDatabase("my_db1")
    assert(snappySession.catalog.listDatabases().collect().map(_.name).toSet ==
        Set("app", "my_db2", "default", "sys"))
  }

  test("list tables") {
    assert(snappySession.catalog.listTables().collect().isEmpty)
    createTable("my_table1")
    createTable("my_table2")
    createTempTable("my_temp_table")
    assert(snappySession.catalog.listTables().collect().map(_.name.toLowerCase).toSet ==
        Set("my_table1", "my_table2", "my_temp_table"))
    dropTable("my_table1", Option("app"))
    assert(snappySession.catalog.listTables().collect().map(_.name.toLowerCase).toSet ==
        Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(snappySession.catalog.listTables().collect()
        .map(_.name.toLowerCase).toSet == Set("my_table2"))
  }

  test("list tables with database") {
    assert(snappySession.catalog.listTables("default").collect().isEmpty)
    createDatabase("my_db1")
    createDatabase("my_db2")
    createTable("my_table1", Some("my_db1"))
    createTable("my_table2", Some("my_db2"))
    createTempTable("my_temp_table")
    assert(snappySession.catalog.listTables("default").collect().map(_.name.toLowerCase).toSet ==
        Set("my_temp_table"))
    assert(snappySession.catalog.listTables("my_db1").collect().map(_.name.toLowerCase).toSet ==
        Set("my_table1", "my_temp_table"))
    assert(snappySession.catalog.listTables("my_db2").collect().map(_.name.toLowerCase).toSet ==
        Set("my_table2", "my_temp_table"))
    dropTable("my_table1", Some("my_db1"))
    assert(snappySession.catalog.listTables("my_db1").collect().map(_.name.toLowerCase).toSet ==
        Set("my_temp_table"))
    assert(snappySession.catalog.listTables("my_db2").collect().map(_.name.toLowerCase).toSet ==
        Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(snappySession.catalog.listTables("default").collect().map(_.name.toLowerCase).isEmpty)
    assert(snappySession.catalog.listTables("my_db1").collect().map(_.name.toLowerCase).isEmpty)
    assert(snappySession.catalog.listTables("my_db2").collect().map(_.name.toLowerCase).toSet ==
        Set("my_table2"))
    val e = intercept[AnalysisException] {
      snappySession.catalog.listTables("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list functions") {
    assert(Set("+", "current_database", "window").subsetOf(
      snappySession.catalog.listFunctions().collect().map(_.name).toSet))
    createFunction("my_func1")
    createFunction("my_func2")
    createTempFunction("my_temp_func")
    val funcNames1 = snappySession.catalog.listFunctions().collect().map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    dropFunction("my_func1")
    dropTempFunction("my_temp_func")
    val funcNames2 = snappySession.catalog.listFunctions().collect().map(_.name).toSet
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(!funcNames2.contains("my_temp_func"))
  }

  test("list functions with database") {
    assert(Set("+", "current_database", "window").subsetOf(
      snappySession.catalog.listFunctions().collect().map(_.name).toSet))
    createDatabase("my_db1")
    createDatabase("my_db2")
    createFunction("my_func1", Some("my_db1"))
    createFunction("my_func2", Some("my_db2"))
    createTempFunction("my_temp_func")
    val funcNames1 = snappySession.catalog.listFunctions("my_db1").collect().map(_.name).toSet
    val funcNames2 = snappySession.catalog.listFunctions("my_db2").collect().map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(!funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(funcNames2.contains("my_temp_func"))

    // Make sure database is set properly.
    assert(
      snappySession.catalog.listFunctions("my_db1").collect()
          .map(_.database).toSet == Set("my_db1", null))
    assert(
      snappySession.catalog.listFunctions("my_db2").collect()
          .map(_.database).toSet == Set("my_db2", null))

    // Remove the function and make sure they no longer appear.
    dropFunction("my_func1", Some("my_db1"))
    dropTempFunction("my_temp_func")
    val funcNames1b = snappySession.catalog.listFunctions("my_db1").collect().map(_.name).toSet
    val funcNames2b = snappySession.catalog.listFunctions("my_db2").collect().map(_.name).toSet
    assert(!funcNames1b.contains("my_func1"))
    assert(!funcNames1b.contains("my_temp_func"))
    assert(funcNames2b.contains("my_func2"))
    assert(!funcNames2b.contains("my_temp_func"))
    val e = intercept[AnalysisException] {
      snappySession.catalog.listFunctions("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list columns") {
    createTable("tab1")
    testListColumns("tab1", dbName = None)
  }

  test("list columns in temporary table") {
    createTempTable("temp1")
    snappySession.catalog.listColumns("temp1")
  }

  test("list columns in database") {
    createDatabase("db1")
    createTable("tab1", Some("db1"))
    testListColumns("tab1", dbName = Some("db1"))
  }

  test("Database.toString") {
    assert(new Database("cool_db", "cool_desc", "cool_path").toString ==
        "Database[name='cool_db', description='cool_desc', path='cool_path']")
    assert(new Database("cool_db", null, "cool_path").toString ==
        "Database[name='cool_db', path='cool_path']")
  }

  test("Table.toString") {
    assert(new Table("volley", "databasa", "one", "world", isTemporary = true).toString ==
        "Table[name='volley', database='databasa', description='one', " +
            "tableType='world', isTemporary='true']")
    assert(new Table("volley", null, null, "world", isTemporary = true).toString ==
        "Table[name='volley', tableType='world', isTemporary='true']")
  }

  test("Function.toString") {
    assert(
      new Function("nama", "databasa", "commenta", "classNameAh", isTemporary = true).toString ==
          "Function[name='nama', database='databasa', description='commenta', " +
              "className='classNameAh', isTemporary='true']")
    assert(new Function("nama", null, null, "classNameAh", isTemporary = false).toString ==
        "Function[name='nama', className='classNameAh', isTemporary='false']")
  }

  test("Column.toString") {
    assert(new Column("namama", "descaca", "datatapa",
      nullable = true, isPartition = false, isBucket = true).toString ==
        "Column[name='namama', description='descaca', dataType='datatapa', " +
            "nullable='true', isPartition='false', isBucket='true']")
    assert(new Column("namama", null, "datatapa",
      nullable = false, isPartition = true, isBucket = true).toString ==
        "Column[name='namama', dataType='datatapa', " +
            "nullable='false', isPartition='true', isBucket='true']")
  }

  test("catalog classes format in Dataset.show") {
    val db = new Database("nama", "descripta", "locata")
    val table = new Table("nama", "databasa", "descripta", "typa", isTemporary = false)
    val function = new Function("nama", "databasa", "descripta", "classa", isTemporary = false)
    val column = new Column(
      "nama", "descripta", "typa", nullable = false, isPartition = true, isBucket = true)
    val dbFields = ScalaReflection.getConstructorParameterValues(db)
    val tableFields = ScalaReflection.getConstructorParameterValues(table)
    val functionFields = ScalaReflection.getConstructorParameterValues(function)
    val columnFields = ScalaReflection.getConstructorParameterValues(column)
    assert(dbFields == Seq("nama", "descripta", "locata"))
    assert(tableFields == Seq("nama", "databasa", "descripta", "typa", false))
    assert(functionFields == Seq("nama", "databasa", "descripta", "classa", false))
    assert(columnFields == Seq("nama", "descripta", "typa", false, true, true))
    val dbString = CatalogImpl.makeDataset(Seq(db), snappySession).showString(10)
    val tableString = CatalogImpl.makeDataset(Seq(table), snappySession).showString(10)
    val functionString = CatalogImpl.makeDataset(Seq(function), snappySession).showString(10)
    val columnString = CatalogImpl.makeDataset(Seq(column), snappySession).showString(10)
    dbFields.foreach { f => assert(dbString.contains(f.toString)) }
    tableFields.foreach { f => assert(tableString.contains(f.toString)) }
    functionFields.foreach { f => assert(functionString.contains(f.toString)) }
    columnFields.foreach { f => assert(columnString.contains(f.toString)) }
  }

}

/**
 * A collection of utility fields and methods for tests related to the [[ExternalCatalog]].
 */
abstract class CatalogTestUtils {

  // Unimplemented methods
  val tableInputFormat: String
  val tableOutputFormat: String

  def newEmptyCatalog(): ExternalCatalog

  // These fields must be lazy because they rely on fields that are not implemented yet
  lazy val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = Some(tableInputFormat),
    outputFormat = Some(tableOutputFormat),
    serde = None,
    compressed = false,
    properties = Map.empty)
  lazy val part1 = CatalogTablePartition(Map("a" -> "1", "b" -> "2"), storageFormat)
  lazy val part2 = CatalogTablePartition(Map("a" -> "3", "b" -> "4"), storageFormat)
  lazy val part3 = CatalogTablePartition(Map("a" -> "5", "b" -> "6"), storageFormat)
  lazy val partWithMixedOrder = CatalogTablePartition(Map("b" -> "6", "a" -> "6"), storageFormat)
  lazy val partWithLessColumns = CatalogTablePartition(Map("a" -> "1"), storageFormat)
  lazy val partWithMoreColumns =
    CatalogTablePartition(Map("a" -> "5", "b" -> "6", "c" -> "7"), storageFormat)
  lazy val partWithUnknownColumns =
    CatalogTablePartition(Map("a" -> "5", "unknown" -> "6"), storageFormat)
  lazy val funcClass = "org.apache.spark.myFunc"

  /**
   * Creates a basic catalog, with the following structure:
   *
   * default
   * db1
   * db2
   *   - tbl1
   *   - tbl2
   *     - part1
   *     - part2
   *   - func1
   */
  def newBasicCatalog(): ExternalCatalog = {
    val catalog = newEmptyCatalog()
    // When testing against a real catalog, the default database may already exist
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    catalog.createDatabase(newDb("db1"), ignoreIfExists = false)
    catalog.createDatabase(newDb("db2"), ignoreIfExists = false)
    catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    catalog.createTable(newTable("tbl2", "db2"), ignoreIfExists = false)
    catalog.createPartitions("db2", "tbl2", Seq(part1, part2), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("func1", Some("db2")))
    catalog
  }

  def newFunc(): CatalogFunction = newFunc("funcName")

  def newUriForDatabase(): String = Utils.createTempDir().toURI.toString.stripSuffix("/")

  def newDb(name: String): CatalogDatabase = {
    CatalogDatabase(name, name + " description", newUriForDatabase(), Map.empty)
  }

  def newTable(name: String, db: String): CatalogTable = newTable(name, Some(db))

  def newTable(name: String, database: Option[String] = None): CatalogTable = {
      CatalogTable(
      identifier = TableIdentifier(name, database),
      tableType = CatalogTableType.EXTERNAL,
      storage = storageFormat,
      schema = StructType(Seq(
          StructField("col1", IntegerType),
          StructField("col2", StringType),
          StructField("a", IntegerType),
          StructField("b", StringType))),
      provider = Some("column"),
      partitionColumnNames = Seq("a", "b"),
      bucketSpec = Some(BucketSpec(8, Seq("col1"), Nil)))
  }

  def newFunc(name: String, database: Option[String] = None): CatalogFunction = {
    CatalogFunction(FunctionIdentifier(name, database), funcClass, Nil)
  }

  /**
   * Whether the catalog's table partitions equal the ones given.
   * Note: Hive sets some random serde things, so we just compare the specs here.
   */
  def catalogPartitionsEqual(
      catalog: ExternalCatalog,
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Boolean = {
    catalog.listPartitions(db, table).map(_.spec).toSet == parts.map(_.spec).toSet
  }
}
