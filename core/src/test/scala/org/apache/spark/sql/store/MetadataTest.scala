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

import java.sql.{SQLException, Statement}

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.diag.SysVTIs
import io.snappydata.SnappyFunSuite
import org.scalatest.Assertions

import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.columnar.impl.ColumnPartitionResolver
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SnappySession, execution}

/**
 * Tests for meta-data queries using Spark SQL.
 */
class MetadataTest extends SnappyFunSuite {

  test("SYS tables/VTIs") {
    val session = this.snc.snappySession
    MetadataTest.testSYSTablesAndVTIs(session.sql)
  }

  test("DESCRIBE and SHOW") {
    val session = this.snc.snappySession
    MetadataTest.testDescribeAndShow(session.sql)
  }

  test("DSID joins with SYS tables") {
    val session = this.snc.snappySession
    MetadataTest.testDSIDWithSYSTables(session.sql, Seq(""))
  }
}

object MetadataTest extends Assertions {

  private def getLongVarcharTuple(name: String, nullable: Boolean = true) =
    (name, 32700L, "LONGVARCHAR", nullable)

  private def getMetadata(name: String, size: Long, typeName: String = "VARCHAR",
      scale: Long = 0): Metadata = typeName match {
    case "VARCHAR" | "CHAR" =>
      val builder = new MetadataBuilder
      builder.putString("name", name)
      builder.putLong("size", size)
      builder.putString("base", typeName)
      builder.putLong("scale", scale)
      builder.build()
    case "LONGVARCHAR" | "CLOB" | "BOOLEAN" =>
      val builder = new MetadataBuilder
      builder.putString("name", name)
      builder.putLong("scale", scale)
      builder.build()
    case _ => Metadata.empty
  }

  private def checkExpectedColumns(rs: Array[Row], expected: List[String]): Unit = {
    assert(rs.length === expected.length)
    assert(rs.map(_.getString(0)).sorted === expected.sorted)
  }

  private def checkTableProperties(rs: Array[Row], isRowTable: Boolean): Unit = {
    val (tableType, provider) = if (isRowTable) {
      "ROW" -> classOf[execution.row.DefaultSource].getName
    } else {
      "COLUMN" -> classOf[execution.columnar.impl.DefaultSource].getName
    }
    val rsMap = rs.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(rsMap("EXTERNAL_SNAPPY") === tableType)
    assert(rsMap("spark.sql.sources.provider") === provider)
    assert(rsMap("spark.sql.sources.schema.numParts") === "1")
  }

  private val expectedSYSTables = Array("ASYNCEVENTLISTENERS", "GATEWAYRECEIVERS",
    "GATEWAYSENDERS", "SYSALIASES", "SYSCHECKS", "SYSCOLPERMS", "SYSCOLUMNS", "SYSCONGLOMERATES",
    "SYSCONSTRAINTS", "SYSDEPENDS", "SYSDISKSTORES", "SYSFILES", "SYSFOREIGNKEYS",
    "SYSHDFSSTORES", "SYSKEYS", "SYSROLES", "SYSROUTINEPERMS", "SYSSCHEMAS", "SYSSTATEMENTS",
    "SYSSTATISTICS", "SYSTABLEPERMS", "SYSTABLES", "SYSTRIGGERS", "SYSVIEWS")
  private val expectedVTIs = Array("DISKSTOREIDS", "HIVETABLES", "INDEXES", "JARS", "MEMBERS",
    "SYSPOLICIES", "TABLESTATS", "VTIS")
  private val localVTIs = Array("MEMORYANALYTICS", "QUERYSTATS", "SESSIONS", "STATEMENTPLANS")
  private val sysSchemasColumns = List("SCHEMAID" -> 36, "SCHEMANAME" -> 128,
    "AUTHORIZATIONID" -> 128, "DEFAULTSERVERGROUPS" -> 32672)
  private val sysTablesColumns: List[(String, Long, String, Boolean)] = List(
    ("TABLEID", 36, "CHAR", false), ("TABLENAME", 128, "VARCHAR", false),
    ("TABLETYPE", 1, "CHAR", false), ("SCHEMAID", 36, "CHAR", false),
    ("TABLESCHEMANAME", 128, "VARCHAR", false), ("LOCKGRANULARITY", 1, "CHAR", false),
    getLongVarcharTuple("SERVERGROUPS", nullable = false), ("DATAPOLICY", 24, "VARCHAR", false),
    getLongVarcharTuple("PARTITIONATTRS"), getLongVarcharTuple("RESOLVER"),
    getLongVarcharTuple("EXPIRATIONATTRS"), getLongVarcharTuple("EVICTIONATTRS"),
    getLongVarcharTuple("DISKATTRS"), ("LOADER", 128, "VARCHAR", true),
    ("WRITER", 128, "VARCHAR", true), getLongVarcharTuple("LISTENERS"),
    getLongVarcharTuple("ASYNCLISTENERS"), ("GATEWAYENABLED", 0, "BOOLEAN", false),
    getLongVarcharTuple("GATEWAYSENDERS"), ("OFFHEAPENABLED", 0, "BOOLEAN", false),
    ("ROWLEVELSECURITYENABLED", 0, "BOOLEAN", false))

  def resultSetToDataset(session: SnappySession, stmt: Statement)
      (sql: String): Dataset[Row] = {
    if (stmt.execute(sql)) {
      val rs = stmt.getResultSet
      val schema = JdbcUtils.getSchema(rs, GemFireXDDialect)
      val dummyMetrics = new InputMetrics
      val rows = JdbcUtils.resultSetToSparkInternalRows(rs, schema, dummyMetrics)
          .map(_.copy()).toSeq
      session.internalCreateDataFrame(session.sparkContext.makeRDD(rows), schema)
    } else {
      implicit val encoder = RowEncoder(StructType(Nil))
      session.createDataset[Row](Nil)
    }
  }

  def testSYSTablesAndVTIs(executeSQL: String => Dataset[Row],
      hostName: String = ClientSharedUtils.getLocalHost.getCanonicalHostName,
      netServers: Seq[String] = Seq(""), locator: String = "", locatorNetServer: String = "",
      servers: Seq[String] = Nil, lead: String = ""): Unit = {
    var ds: Dataset[Row] = null
    var expectedColumns: List[String] = null
    var rs: Array[Row] = null
    var expectedRow: Row = null
    var expectedRows: Seq[Row] = null
    lazy val myId = Misc.getMyId.toString

    // ----- check querying on SYS.MEMBERS and SHOW MEMBERS -----

    ds = executeSQL("select id, kind, status, hostData, isElder, netServers, serverGroups " +
        "from sys.members")
    rs = ds.collect()

    // check for the single VM case or else the provided nodes
    def checkMembers(rs: Array[Row], forShow: Boolean): Unit = {
      if (locator.isEmpty) {
        assert(rs.length === 1)
        if (forShow) {
          expectedRow = Row(myId, hostName, "loner", "RUNNING", netServers.head, "")
        } else {
          expectedRow = Row(myId, "loner", "RUNNING", true, true, netServers.head, "")
        }
        assert(rs(0) === expectedRow)
      } else {
        assert(rs.length === 2 + servers.length)
        if (forShow) {
          expectedRows = Row(locator, hostName, "locator", "RUNNING", locatorNetServer, "") +:
              Row(lead, hostName, "primary lead", "RUNNING", "", "") +:
              servers.zip(netServers).map(p => Row(p._1, hostName, "datastore",
                "RUNNING", p._2, ""))
        } else {
          expectedRows = Row(locator, "locator", "RUNNING", false, true, locatorNetServer, "") +:
              Row(lead, "primary lead", "RUNNING", false, false, "", "") +:
              servers.zip(netServers).map(p => Row(p._1, "datastore",
                "RUNNING", true, false, p._2, ""))
        }
        assert(rs.sortBy(_.getString(0)) === expectedRows.sortBy(_.getString(0)))
      }
    }

    checkMembers(rs, forShow = false)

    ds = executeSQL("show members")
    expectedColumns = List("ID", "HOST", "KIND", "STATUS", "THRIFTSERVERS", "SERVERGROUPS")
    val expectedSizes = List(256, 256, 24, 12, 32672, 32672)
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema === StructType(expectedColumns.zip(expectedSizes).map(p =>
      StructField(p._1, StringType, nullable = false, getMetadata(p._1, p._2)))))
    checkMembers(rs, forShow = true)

    // ----- check queries on some SYS tables (SYSSCHEMAS and SYSTABLES) -----

    ds = executeSQL("select * from sys.sysSchemas")
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema === StructType(sysSchemasColumns.map(p =>
      StructField(p._1, StringType, nullable = false, getMetadata(p._1, p._2,
        if (p._1 == "SCHEMAID") "CHAR" else "VARCHAR")))))
    val expectedDefaultSchemas = List("APP", "NULLID", "SNAPPY_HIVE_METASTORE", "SQLJ",
      "SYS", "SYSCAT", "SYSCS_DIAG", "SYSCS_UTIL", "SYSFUN", "SYSIBM", "SYSPROC", "SYSSTAT")
    assert(rs.length === expectedDefaultSchemas.length)
    assert(rs.map(_.getString(1)).sorted === expectedDefaultSchemas)

    ds = executeSQL("select * from sys.sysTables where tableSchemaName = 'SYS'")
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema === StructType(sysTablesColumns.map { case (name, size, typeName, nullable) =>
      val dataType = typeName match {
        case "BOOLEAN" => BooleanType
        case _ => StringType
      }
      StructField(name, dataType, nullable, getMetadata(name, size, typeName))
    }))
    assert(rs.length === expectedSYSTables.length)
    assert(rs.map(_.getString(1)).sorted === expectedSYSTables)
    assert(rs.map(_.getString(4)).distinct === Array("SYS"))

    // ----- check queries on VTIs (except MEMBERS) -----

    rs = executeSQL("select * from sys.diskStoreIds").collect()
    // datadictionary, delta and "default" diskStores are created by default
    if (locator.isEmpty) {
      assert(rs.length === 3)
      assert(rs.map(r => r.getString(0) -> r.getString(1)).sorted === Array(
        myId -> "GFXD-DD-DISKSTORE", myId -> "GFXD-DEFAULT-DISKSTORE",
        myId -> "SNAPPY-INTERNAL-DELTA"))
    } else {
      // expect default disk stores on all the nodes (2 on locator, 1 on lead and 3 on server)
      assert(rs.length === 3 + 3 * servers.length)
      assert(rs.map(r => r.getString(0) -> r.getString(1)).toSeq.sorted === (Seq(
        locator -> "GFXD-DD-DISKSTORE", locator -> "GFXD-DEFAULT-DISKSTORE",
        lead -> "GFXD-DEFAULT-DISKSTORE") ++
          servers.flatMap(s => Seq(s -> "GFXD-DD-DISKSTORE", s -> "GFXD-DEFAULT-DISKSTORE",
            s -> "SNAPPY-INTERNAL-DELTA"))).sorted)
    }

    rs = executeSQL("select * from sys.indexes").collect()
    assert(rs.map(_.getString(0)).distinct === Array("SNAPPY_HIVE_METASTORE"))
    rs = executeSQL("select * from sys.indexes " +
        "where schemaName <> 'SNAPPY_HIVE_METASTORE'").collect()
    assert(rs.length === 0)

    rs = executeSQL("select * from sys.jars").collect()
    assert(rs.length === 0)

    rs = executeSQL("select * from sys.hiveTables").collect()
    assert(rs.length === 0)

    rs = executeSQL("select * from sys.sysPolicies").collect()
    assert(rs.length === 0)

    rs = executeSQL("select * from sys.tableStats").collect()
    assert(rs.length === 0)

    rs = executeSQL("select distinct schemaName, tableName from sys.vtis").collect()
    assert(rs.length === expectedVTIs.length + localVTIs.length)
    assert(rs.sortBy(_.getString(1)) === (expectedVTIs ++ localVTIs).sorted.map(Row("SYS", _)))
    rs = executeSQL("select distinct schemaName, tableName from sys.vtis " +
        s"where tableType != '${SysVTIs.LOCAL_VTI}' or tableName = 'MEMBERS'").collect()
    assert(rs.length === expectedVTIs.length)
    assert(rs.sortBy(_.getString(1)) === expectedVTIs.map(Row("SYS", _)))

    // ----- create some tables and repeat some of the checks accounting for the new tables -----

    executeSQL("create table rowTable1 (id int primary key, data string)")
    executeSQL("create table columnTable2 (id long, data string, data2 decimal) using column")

    // ----- check SYSTABLES for user tables -----

    ds = executeSQL("select * from sys.sysTables where tableSchemaName = 'APP'")
    rs = ds.collect().sortBy(_.getString(1))
    assert(rs.length === 3)
    assert(rs.map(r => (r.getString(1), r.getString(2), r.getString(4), r.getString(7))) === Array(
      ("COLUMNTABLE2", "C", "APP", "PERSISTENT_PARTITION"),
      ("ROWTABLE1", "T", "APP", "PERSISTENT_REPLICATE"),
      ("SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE_", "C", "APP", "PERSISTENT_PARTITION")))
    // check the presence of required attributed in partitioning and disk attributes
    val commonPartAttrs = "colocatedWith=null,recoveryDelay=-1,startupRecoveryDelay=0"
    var partAttrs = List(commonPartAttrs, null,
      "colocatedWith=/APP/COLUMNTABLE2,recoveryDelay=-1,startupRecoveryDelay=0")
    var resolvers = List("PARTITION BY PRIMARY KEY", null,
      s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'")
    val commonEvictionAttrs = " algorithm=lru-heap-percentage; action=overflow-to-disk; " +
        "sizer=GfxdObjectSizer"
    val commonDiskAttrs = "DiskStore is %d; Synchronous writes to disk"
    var diskAttrs = List(commonDiskAttrs.replace("%d", "SNAPPY-INTERNAL-DELTA"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"))
    assert(rs.map(r => r.getString(9) -> r.getString(12)) === resolvers.zip(diskAttrs))
    assert(rs.map(_.getString(11)).distinct === Array(commonEvictionAttrs))
    rs.zip(partAttrs).foreach { case (r, a) =>
      assert(if (a eq null) r.isNullAt(8) else r.getString(8).contains(a))
      assert(r.isNullAt(10))
    }

    // ----- check INDEXES for user tables -----

    rs = executeSQL("select * from sys.indexes where schemaName <> 'SNAPPY_HIVE_METASTORE' " +
        "order by tableName").collect()
    assert(rs.length === 3)
    // check for all primary indexes including the internal ones of column store tables
    assert(rs === Array(
      Row("APP", "COLUMNTABLE2", "2__COLUMNTABLE2__SNAPPYDATA_INTERNAL_ROWID",
        "+SNAPPYDATA_INTERNAL_ROWID", "UNIQUE", true, "PRIMARY KEY"),
      Row("APP", "ROWTABLE1", "2__ROWTABLE1__ID", "+ID", "UNIQUE", true, "PRIMARY KEY"),
      Row("APP", "SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE_",
        "2__SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE___UUID__PARTITIONID__COLUMNINDEX",
        "+UUID+PARTITIONID+COLUMNINDEX", "UNIQUE", true, "PRIMARY KEY")))

    // ----- create more tables and repeat the checks -----

    executeSQL("create table schema1.columnTable1 (id int, data date, data2 string) " +
        "using column options (partition_by 'id')")
    executeSQL("create schema schema2 authorization app")
    executeSQL("create table schema2.rowTable2 (id int primary key, data varchar(1024)) " +
        "using row options (partition_by 'id')")
    executeSQL("create index schema2.rowIndex2 on schema2.rowTable2(data)")

    // ----- check SYSTABLES for user tables -----

    ds = executeSQL("select * from sys.sysTables where " +
        "tableSchemaName = 'APP' or tableSchemaName like 'SCHEMA%'")
    rs = ds.collect().sortBy(_.getString(1))
    assert(rs.length === 6)
    assert(rs.map(r => (r.getString(1), r.getString(2), r.getString(4), r.getString(7))) === Array(
      ("COLUMNTABLE1", "C", "SCHEMA1", "PERSISTENT_PARTITION"),
      ("COLUMNTABLE2", "C", "APP", "PERSISTENT_PARTITION"),
      ("ROWTABLE1", "T", "APP", "PERSISTENT_REPLICATE"),
      ("ROWTABLE2", "T", "SCHEMA2", "PERSISTENT_PARTITION"),
      ("SNAPPYSYS_INTERNAL____COLUMNTABLE1_COLUMN_STORE_", "C", "SCHEMA1", "PERSISTENT_PARTITION"),
      ("SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE_", "C", "APP", "PERSISTENT_PARTITION")))
    // check the presence of required attributed in partitioning and disk attributes
    partAttrs = List(commonPartAttrs, commonPartAttrs, null, commonPartAttrs,
      "colocatedWith=/SCHEMA1/COLUMNTABLE1,recoveryDelay=-1,startupRecoveryDelay=0",
      "colocatedWith=/APP/COLUMNTABLE2,recoveryDelay=-1,startupRecoveryDelay=0")
    resolvers = List("PARTITION BY COLUMN (ID)", "PARTITION BY PRIMARY KEY", null,
      "PARTITION BY PRIMARY KEY", s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'",
      s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'")
    diskAttrs = List(commonDiskAttrs.replace("%d", "SNAPPY-INTERNAL-DELTA"),
      commonDiskAttrs.replace("%d", "SNAPPY-INTERNAL-DELTA"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"),
      commonDiskAttrs.replace("%d", "GFXD-DEFAULT-DISKSTORE"))
    assert(rs.map(r => r.getString(9) -> r.getString(12)) === resolvers.zip(diskAttrs))
    assert(rs.map(_.getString(11)).distinct === Array(commonEvictionAttrs))
    rs.zip(partAttrs).foreach { case (r, a) =>
      assert(if (a eq null) r.isNullAt(8) else r.getString(8).contains(a))
      assert(r.isNullAt(10))
    }

    // ----- check INDEXES for user tables -----

    rs = executeSQL("select * from sys.indexes where schemaName != 'SNAPPY_HIVE_METASTORE' " +
        "order by tableName, indexName").collect()
    assert(rs.length === 7)
    // check for all primary indexes including the internal ones of column store tables
    assert(rs === Array(
      Row("SCHEMA1", "COLUMNTABLE1", "2__COLUMNTABLE1__ID__SNAPPYDATA_INTERNAL_ROWID",
        "+ID+SNAPPYDATA_INTERNAL_ROWID", "UNIQUE", true, "PRIMARY KEY"),
      Row("APP", "COLUMNTABLE2", "2__COLUMNTABLE2__SNAPPYDATA_INTERNAL_ROWID",
        "+SNAPPYDATA_INTERNAL_ROWID", "UNIQUE", true, "PRIMARY KEY"),
      Row("APP", "ROWTABLE1", "2__ROWTABLE1__ID", "+ID", "UNIQUE", true, "PRIMARY KEY"),
      Row("SCHEMA2", "ROWTABLE2", "2__ROWTABLE2__ID", "+ID", "UNIQUE", true, "PRIMARY KEY"),
      Row("SCHEMA2", "ROWTABLE2", "ROWINDEX2", "+DATA", "NOT_UNIQUE", true, "LOCAL:SORTED"),
      Row("SCHEMA1", "SNAPPYSYS_INTERNAL____COLUMNTABLE1_COLUMN_STORE_",
        "2__SNAPPYSYS_INTERNAL____COLUMNTABLE1_COLUMN_STORE___UUID__PARTITIONID__COLUMNINDEX",
        "+UUID+PARTITIONID+COLUMNINDEX", "UNIQUE", true, "PRIMARY KEY"),
      Row("APP", "SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE_",
        "2__SNAPPYSYS_INTERNAL____COLUMNTABLE2_COLUMN_STORE___UUID__PARTITIONID__COLUMNINDEX",
        "+UUID+PARTITIONID+COLUMNINDEX", "UNIQUE", true, "PRIMARY KEY")))

    // ----- cleanup -----

    executeSQL("drop table rowTable1")
    executeSQL("drop table schema1.columnTable1")
    executeSQL("drop table schema2.rowTable2")
    executeSQL("drop table columnTable2")
    executeSQL("drop schema schema1")
    executeSQL("drop schema schema2")
  }

  def testDescribeAndShow(executeSQL: String => Dataset[Row]): Unit = {
    var ds: Dataset[Row] = null
    var expectedColumns: List[String] = null
    var rs: Array[Row] = null

    // ----- check SHOW SCHEMAS -----

    rs = executeSQL("show schemas").collect()
    assert(rs === Array(Row("APP"), Row("DEFAULT"), Row("SYS")))
    rs = executeSQL("show schemas like 'a*|s*'").collect()
    assert(rs === Array(Row("APP"), Row("SYS")))

    // ----- check DESCRIBE for schema-----

    rs = executeSQL("describe schema sys").collect()
    assert(rs === Array(Row("Database Name", "SYS"), Row("Description", "System Schema"),
      Row("Location", "")))
    rs = executeSQL("desc schema extended sys").collect()
    assert(rs === Array(Row("Database Name", "SYS"), Row("Description", "System Schema"),
      Row("Location", ""), Row("Properties", "")))

    // ----- check SHOW TABLES variants -----
    val allSYSTables = (expectedSYSTables ++ expectedVTIs).sorted

    rs = executeSQL("show tables").collect()
    assert(rs.length === 0)
    rs = executeSQL("show tables in app").collect()
    assert(rs.length === 0)

    rs = executeSQL("show tables from sys").collect()
    assert(rs.length === allSYSTables.length)
    assert(rs.sortBy(_.getString(1)) === allSYSTables.map(n => Row("SYS", n, false)))

    rs = executeSQL("show tables in sys like '[m-s]*'").collect()
    val filtered = (expectedSYSTables ++ expectedVTIs)
        .filter(n => n.charAt(0) >= 'M' && n.charAt(0) <= 'S').sorted
    assert(rs.length === filtered.length)
    assert(rs.sortBy(_.getString(1)) === filtered.map(n => Row("SYS", n, false)))

    // system schemas other than SYS should not be visible
    try {
      rs = executeSQL("show tables in sysibm").collect()
    } catch {
      case _: NoSuchDatabaseException => rs = Array.empty
      case se: SQLException if se.getSQLState == "42000" => rs = Array.empty
    }
    assert(rs.length === 0)

    // ----- check SHOW COLUMNS for a few SYS tables -----

    rs = executeSQL("show columns from sys.sysSchemas").collect()
    expectedColumns = List("SCHEMAID", "SCHEMANAME", "AUTHORIZATIONID", "DEFAULTSERVERGROUPS")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in sysTables from sys").collect()
    expectedColumns = sysTablesColumns.map(_._1)
    checkExpectedColumns(rs, expectedColumns)
    rs = executeSQL("show columns from SYS.sysTables from sys").collect()
    checkExpectedColumns(rs, expectedColumns)
    rs = executeSQL("show columns from sys.sysTables in SYS").collect()
    checkExpectedColumns(rs, expectedColumns)

    try {
      rs = executeSQL("show columns in sysTables from app").collect()
      fail("Expected error due to non-existent table")
    } catch {
      case _: NoSuchTableException => // expected
      case se: SQLException if se.getSQLState == "42000" => // expected
    }
    try {
      rs = executeSQL("show columns in sys.sysTables from app").collect()
      fail("Expected error due to conflicting schema names")
    } catch {
      case _: AnalysisException => // expected
      case se: SQLException if se.getSQLState == "42000" => // expected
    }

    // ----- check DESCRIBE for a few SYS tables -----

    ds = executeSQL("describe sys.sysSchemas")
    rs = ds.collect()
    expectedColumns = List("col_name", "data_type", "comment")
    val nullability = List(false, false, true)
    // check schema of the returned Dataset
    assert(ds.schema.map(_.copy(metadata = Metadata.empty)) === expectedColumns.zip(nullability)
        .map(p => StructField(p._1, StringType, p._2)))
    assert(rs.toSeq === sysSchemasColumns.map(p => Row(p._1, StringType.simpleString, null)))

    ds = executeSQL("desc extended sys.sysSchemas")
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema.map(_.copy(metadata = Metadata.empty)) === expectedColumns.zip(nullability)
        .map(p => StructField(p._1, StringType, p._2)))
    // last row is detailed information and an empty row before that (no partitioning information)
    assert(rs.length === sysSchemasColumns.length + 2)
    assert(rs.take(sysSchemasColumns.length).toSeq === sysSchemasColumns.map(
      p => Row(p._1, StringType.simpleString, null)))
    assert(rs(sysSchemasColumns.length + 1).getString(0) === "# Detailed Table Information")

    ds = executeSQL("desc sys.sysTables")
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema.map(_.copy(metadata = Metadata.empty)) === expectedColumns.zip(nullability)
        .map(p => StructField(p._1, StringType, p._2)))
    assert(rs.toSeq === sysTablesColumns.map {
      case (name, _, "BOOLEAN", _) => Row(name, BooleanType.simpleString, null)
      case (name, _, _, _) => Row(name, StringType.simpleString, null)
    })

    ds = executeSQL("describe extended sys.sysTables")
    rs = ds.collect()
    // check schema of the returned Dataset
    assert(ds.schema.map(_.copy(metadata = Metadata.empty)) === expectedColumns.zip(nullability)
        .map(p => StructField(p._1, StringType, p._2)))
    // last row is detailed information and an empty row before that (no partitioning information)
    assert(rs.length === sysTablesColumns.length + 2)
    assert(rs.take(sysTablesColumns.length).toSeq === sysTablesColumns.map {
      case (name, _, "BOOLEAN", _) => Row(name, BooleanType.simpleString, null)
      case (name, _, _, _) => Row(name, StringType.simpleString, null)
    })
    assert(rs(sysTablesColumns.length + 1).getString(0) === "# Detailed Table Information")

    // ----- check SHOW COLUMNS for VTIs -----

    rs = executeSQL("show columns in diskStoreIds from sys").collect()
    expectedColumns = List("MEMBERID", "NAME", "ID", "DIRS")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in sys.indexes").collect()
    expectedColumns = List("SCHEMANAME", "TABLENAME", "INDEXNAME", "COLUMNS_AND_ORDER",
      "UNIQUE", "CASESENSITIVE", "INDEXTYPE")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns from sys.jars").collect()
    expectedColumns = List("SCHEMA", "ALIAS", "ID")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in sys.members").collect()
    expectedColumns = List("ID", "KIND", "STATUS", "HOSTDATA", "ISELDER", "IPADDRESS", "HOST",
      "PID", "PORT", "ROLES", "NETSERVERS", "THRIFTSERVERS", "LOCATOR", "SERVERGROUPS",
      "SYSTEMPROPS", "GEMFIREPROPS", "BOOTPROPS", "MANAGERINFO")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns from sys.sysPolicies in sys").collect()
    expectedColumns = List("NAME", "SCHEMANAME", "TABLENAME", "POLICYFOR", "APPLYTO",
      "FILTER", "OWNER")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in sys.tableStats").collect()
    expectedColumns = List("TABLE", "IS_COLUMN_TABLE", "IS_REPLICATED_TABLE", "ROW_COUNT",
      "SIZE_IN_MEMORY", "TOTAL_SIZE", "BUCKETS")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in hiveTables in sys").collect()
    expectedColumns = List("SCHEMANAME", "TABLENAME", "TABLETYPE", "PROVIDER", "SOURCEPATH",
      "COMPRESSION", "COLUMNNAME", "TYPEID", "TYPENAME", "ORDINAL", "PRECISION", "SCALE",
      "MAXWIDTH", "NULLABLE", "VIEWTEXT")
    checkExpectedColumns(rs, expectedColumns)

    rs = executeSQL("show columns in SYS.VTIs").collect()
    expectedColumns = List("SCHEMANAME", "TABLENAME", "TABLETYPE", "COLUMNNAME", "TYPEID",
      "TYPENAME", "ORDINAL", "PRECISION", "SCALE", "DISPLAYWIDTH", "NULLABLE")
    checkExpectedColumns(rs, expectedColumns)

    // ----- empty SHOW TBLPROPERTIES for SYS tables/VTIs -----

    rs = executeSQL("show tblproperties SYS.SYSTABLES").collect()
    assert(rs.length === 0)
    rs = executeSQL("show tblproperties SYS.MEMBERS").collect()
    assert(rs.length === 0)

    // ----- create some tables and repeat some of the checks accounting for the new tables -----

    executeSQL("create table rowTable1 (id int primary key, data string)")
    executeSQL("create table columnTable2 (id long, data string, data2 decimal) using column")

    // ----- check SHOW SCHEMAS for user tables -----

    rs = executeSQL("show schemas").collect()
    assert(rs === Array(Row("APP"), Row("DEFAULT"), Row("SYS")))

    // ----- check SHOW TABLES for user tables -----

    rs = executeSQL("show tables in App").collect()
    assert(rs.length === 2)
    assert(rs.sortBy(_.getString(1)) === Array(
      Row("APP", "COLUMNTABLE2", false), Row("APP", "ROWTABLE1", false)))

    // ----- check DESCRIBE and SHOW COLUMNS for user tables -----

    rs = executeSQL("describe rowTable1").collect()
    assert(rs === Array(Row("ID", IntegerType.simpleString, null),
      Row("DATA", StringType.simpleString, null)))
    rs = executeSQL("describe extended columnTable2").collect()
    // last row is detailed information and an empty row before that (no partitioning information)
    assert(rs.length === 5)
    assert(rs.take(3) === Array(Row("ID", LongType.simpleString, null),
      Row("DATA", StringType.simpleString, null),
      Row("DATA2", DecimalType.SYSTEM_DEFAULT.simpleString, null)))
    assert(rs(4).getString(0) === "# Detailed Table Information")

    rs = executeSQL("show columns in rowTable1 from app").collect()
    expectedColumns = List("ID", "DATA")
    checkExpectedColumns(rs, expectedColumns)
    rs = executeSQL("show columns in columnTable2").collect()
    expectedColumns = List("ID", "DATA", "DATA2")
    checkExpectedColumns(rs, expectedColumns)

    // ----- check SHOW TBLPROPERTIES for user tables -----

    rs = executeSQL("show tblproperties rowTable1").collect()
    checkTableProperties(rs, isRowTable = true)
    rs = executeSQL("show tblproperties columnTable2").collect()
    checkTableProperties(rs, isRowTable = false)

    // create more tables and repeat the checks

    executeSQL("create schema schema1")
    executeSQL("create table schema1.columnTable1 (id int, data date, data2 string) " +
        "using column options (partition_by 'id')")
    executeSQL("create table schema2.rowTable2 (id int primary key, data string) " +
        "using row options (partition_by 'id')")

    // ----- check SHOW SCHEMAS for user tables -----

    rs = executeSQL("show schemas").collect()
    assert(rs === Array(Row("APP"), Row("DEFAULT"), Row("SCHEMA1"), Row("SCHEMA2"), Row("SYS")))

    // ----- check SHOW TABLES for user tables -----

    rs = executeSQL("show tables in schema1").collect()
    assert(rs.length === 1)
    assert(rs(0) === Row("SCHEMA1", "COLUMNTABLE1", false))
    rs = executeSQL("show tables in schema2").collect()
    assert(rs.length === 1)
    assert(rs(0) === Row("SCHEMA2", "ROWTABLE2", false))

    // ----- check DESCRIBE and SHOW COLUMNS for user tables -----

    rs = executeSQL("describe schema1.columnTable1").collect()
    assert(rs === Array(Row("ID", IntegerType.simpleString, null),
      Row("DATA", DateType.simpleString, null),
      Row("DATA2", StringType.simpleString, null)))
    rs = executeSQL("describe extended schema2.rowTable2").collect()
    // last row is detailed information and an empty row before that (no partitioning information)
    assert(rs.length === 4)
    assert(rs.take(2) === Array(Row("ID", IntegerType.simpleString, null),
      Row("DATA", StringType.simpleString, null)))
    assert(rs(3).getString(0) === "# Detailed Table Information")

    rs = executeSQL("show columns in schema1.columnTable1").collect()
    expectedColumns = List("ID", "DATA", "DATA2")
    checkExpectedColumns(rs, expectedColumns)
    rs = executeSQL("show columns in rowTable2 from schema2").collect()
    expectedColumns = List("ID", "DATA")
    checkExpectedColumns(rs, expectedColumns)

    // ----- check SHOW TBLPROPERTIES for user tables -----

    rs = executeSQL("show tblproperties schema1.columnTable1").collect()
    checkTableProperties(rs, isRowTable = false)
    rs = executeSQL("show tblproperties schema2.rowTable2").collect()
    checkTableProperties(rs, isRowTable = true)

    // ----- cleanup -----

    executeSQL("drop table rowTable1")
    executeSQL("drop table schema1.columnTable1")
    executeSQL("drop table schema2.rowTable2")
    executeSQL("drop table columnTable2")
    executeSQL("drop schema schema1")
    executeSQL("drop schema schema2")
  }

  def testDSIDWithSYSTables(executeSQL: String => Dataset[Row],
      netServers: Seq[String], locator: String = "", locatorNetServer: String = "",
      servers: Seq[String] = Nil, lead: String = ""): Unit = {
    var rs: Array[Row] = null
    lazy val myId = Misc.getMyId.toString
    val numRows = 1000000L

    rs = executeSQL("select dsid()").collect()
    assert(rs.length === 1)
    if (locator.isEmpty) {
      assert(rs === Array(Row(myId)))
    } else {
      assert(servers.contains(rs(0).getString(0)))
    }
    rs = executeSQL("select m.id, netservers from sys.members m where m.id = dsid()").collect()
    if (locator.isEmpty) {
      assert(rs === Array(Row(myId, netServers.head)))
    }

    // create a table and do group by queries to see data distribution
    executeSQL("create table columnTable1 using column options (partition_by 'id') as " +
        s"select id, ('dataForID' || id) data from range($numRows)")
    rs = executeSQL("select count(*), dsid() from columnTable1 group by dsid()").collect()
    if (locator.isEmpty) {
      assert(rs === Array(Row(numRows, myId)))
    } else {
      // with smart connector, the JDBC connections could all go to one server
      // or more because all nodes are running on same host in unit tests,
      // so cannot check for all servers to be present
      assert(rs.map(_.getLong(0)).sum === numRows)
      assert(rs.map(_.getString(1)).forall(servers.contains),
        s"Servers = $servers, Result = ${rs.toSeq}")
    }

    // join with sys.members to obtain additional member information
    // the members VTI should act like a replicated table
    rs = executeSQL("select cnt, m.id, m.netservers from (" +
        "select count(*) cnt, dsid() id from columnTable1 group by dsid()) t " +
        "inner join sys.members m on (t.id = m.id)").collect()
    if (locator.isEmpty) {
      assert(rs === Array(Row(numRows, myId, netServers.head)))
    } else {
      assert(rs.map(_.getLong(0)).sum === numRows)
      assert(rs.map(_.getString(1)).forall(servers.contains),
        s"Servers = $servers, Result = ${rs.toSeq}")
      assert(rs.map(_.getString(2)).forall(netServers.contains),
        s"NetServers = $netServers, Result = ${rs.toSeq}")
    }

    // check primary distribution
    executeSQL("set snappydata.preferPrimaries = true")
    rs = executeSQL("select cnt, m.id, m.netservers from (" +
        "select count(*) cnt, dsid() id from columnTable1 group by dsid()) t " +
        "left join sys.members m on (t.id = m.id)").collect()
    if (locator.isEmpty) {
      assert(rs === Array(Row(numRows, myId, netServers.head)))
    } else {
      assert(rs.map(_.getLong(0)).sum === numRows)
      assert(rs.map(_.getString(1)).forall(servers.contains),
        s"Servers = $servers, Result = ${rs.toSeq}")
      assert(rs.map(_.getString(2)).forall(netServers.contains),
        s"NetServers = $netServers, Result = ${rs.toSeq}")
    }
    executeSQL("set snappydata.preferPrimaries = false")

    executeSQL("drop table columnTable1")
  }
}
