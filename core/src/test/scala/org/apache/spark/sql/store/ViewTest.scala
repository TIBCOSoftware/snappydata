/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoinExec}
import org.apache.spark.sql.{AnalysisException, Row, SnappySession}

/**
 * Tests for temporary, global and persistent views.
 */
class ViewTest extends SnappyFunSuite {

  private val columnTable = "viewColTable"
  private val rowTable = "viewRowTable"
  private val numRows = 10
  private val viewQuery = "select id, addr, rank() over (order by id) as rank"
  private val viewTempMeta = Seq(Row("ID", "int", null), Row("ADDR", "string", null),
    Row("RANK", "int", null))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val session = this.snc.snappySession
    session.sql(s"create table $columnTable (id int, addr varchar(20)) using column " +
        "options (partition_by 'id')")
    session.sql(s"create table $rowTable (id int, addr varchar(20)) using row " +
        s"options (partition_by 'id', colocate_with '$columnTable')")

    val rows = (0 until numRows).map(i => Row(i, "address_" + (i + 1)))
    snc.insert(columnTable, rows: _*)
    snc.insert(rowTable, rows: _*)
  }

  private def getExpectedResult: Seq[Row] = {
    (0 until numRows).map(i => Row(i, "address_" + (i + 1), i + 1))
  }

  test("temporary view") {
    val session = this.snc.snappySession

    val tableMeta = Seq(Row("id", "int", null), Row("addr", "string", null))

    checkAnswer(session.sql(s"describe $columnTable"), tableMeta)
    checkAnswer(session.sql(s"describe $rowTable"), tableMeta)

    val expected = getExpectedResult

    // check temporary view and its meta-data for column table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    // should not be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.tableExists("viewOnTable") === false)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    // check the same for view on row table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $rowTable")

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.tableExists("viewOnTable") === false)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    session2.close()
  }

  test("global temporary view") {
    val session = this.snc.snappySession

    val expected = getExpectedResult

    // check temporary view and its meta-data for column table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session2.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expected)

    try {
      session.sql("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)

    // check the same for view on row table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session2.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expected)

    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)

    session2.close()
  }

  test("temporary view using") {
    val session = this.snc.snappySession

    // check temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airline = session.read.parquet(hfile)
    session.sql(s"create temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = session.table("airlineView")

    assert(session.sessionCatalog.tableExists("airlineView") === true)
    assert(airlineView.schema === airline.schema)
    checkAnswer(session.sql("select count(*) from airlineView"), Seq(Row(airline.count())))
    assert(airlineView.count() == airline.count())

    // should not be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("airlineView") === false)

    // drop and check unavailability
    session.sql("drop table airlineView")
    assert(session.sessionCatalog.tableExists("airlineView") === false)
    assert(session2.sessionCatalog.tableExists("airlineView") === false)

    session2.close()
  }

  test("global temporary view using") {
    val session = this.snc.snappySession

    // check global temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airline = session.read.parquet(hfile)
    session.sql(s"create global temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = session.table("airlineView")

    assert(session.sessionCatalog.getGlobalTempView("airlineView").isDefined)
    assert(airlineView.schema === airline.schema)
    checkAnswer(session.sql("select count(*) from airlineView"), Seq(Row(airline.count())))
    assert(airlineView.count() == airline.count())

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isDefined)
    checkAnswer(session2.sql("select count(*) from airlineView"), Seq(Row(airline.count())))

    try {
      session.sql("drop table airlineView")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session.sql("drop view airlineView")
    assert(session.sessionCatalog.getGlobalTempView("airlineView").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isEmpty)

    session2.close()
  }

  test("persistent view") {
    val expected = getExpectedResult
    // check temporary view and its meta-data for column table
    checkPersistentView(columnTable, rowTable, snc.snappySession, expected)
    // check the same for view on row table
    checkPersistentView(rowTable, columnTable, snc.snappySession, expected)
  }

  private def checkPersistentView(table: String, otherTable: String, session: SnappySession,
      expectedResult: Seq[Row]): Unit = {
    session.sql(s"create view viewOnTable as $viewQuery from $table")

    val viewMeta = Seq(Row("id", "int", null), Row("addr", "string", null),
      Row("rank", "int", null))

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expectedResult)

    // should be visible from another session
    var session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("viewOnTable") === true)
    checkAnswer(session2.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expectedResult)

    // test for SNAP-2205: see CompressionCodecId.isCompressed for a description of the problem
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // 21 columns mean 63 for ColumnStatsSchema so total of 64 fields including the COUNT
    // in the stats row which will fit in exactly one long for the nulls bitset
    val cols = (1 to 21).map(i => s"col$i string").mkString(", ")
    session.sql(s"CREATE TABLE test2205 ($cols) using column options (buckets '4')")

    val numElements = 10000
    val projection = (1 to 21).map(i => s"null as col$i")
    session.range(numElements).selectExpr(projection: _*).write.insertInto("test2205")

    checkAnswer(session.sql("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    // should be available after a restart
    session.close()
    session2.close()
    stopAll()
    val sys = InternalDistributedSystem.getConnectedInstance
    if (sys ne null) {
      sys.disconnect()
    }

    session2 = new SnappySession(sc)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === true)
    checkAnswer(session2.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expectedResult)

    checkAnswer(session2.sql("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    try {
      session2.sql("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session2.sql("drop view viewOnTable")
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)
    session2.sql("drop table test2205")

    // check colocated joins with VIEWs (SNAP-2204)

    val query = s"select c.id, r.addr from $columnTable c inner join $rowTable r on (c.id = r.id)"
    // first check with normal query
    var ds = session2.sql(query)
    checkAnswer(ds, expectedResult.map(r => Row(r.get(0), r.get(1))))
    var plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    val expectedResult2 = expectedResult.map(r => Row(r.get(0), r.get(1)))
    // check for normal view join with table
    session2.sql(s"create view viewOnTable as select id, addr, id + 1 from $table")
    ds = session2.sql("select t.id, v.addr from viewOnTable v " +
        s"inner join $otherTable t on (v.id = t.id)")
    checkAnswer(ds, expectedResult2)
    plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    session2.sql("drop view viewOnTable")
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    // next query on a join view
    session2.sql(s"create view viewOnJoin as $query")
    ds = session2.sql("select * from viewOnJoin")
    checkAnswer(ds, expectedResult2)
    plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    session2.sql("drop view viewOnJoin")
    assert(session2.sessionCatalog.tableExists("viewOnJoin") === false)
  }


  test("SNAP-2342 and SNAP-2346 nested query involving joins & union throws Exception") {
    val session = this.snc.snappySession

    session.sql(s"create table ujli ( " +
        "aagmaterial   string," +
        "accountassignmentgroup   string," +
        "accounttype   string," +
        "allocationcycle   string," +
        "allocationsegment   string," +
        "asset   string," +
        "billingdocument   string," +
        "billingdocumentitem   string," +
        "bravoequitycode   string," +
        "bravominorcode   string," +
        "bsegdocumentlinenumber   string," +
        "businessplace   string," +
        "businesstransaction   string," +
        "controllingarea   string," +
        "copadocumentnumber   string," +
        "copaobjectnumber   string," +
        "costcenter   string," +
        "costelement   string," +
        "countryofshiptocustomer   string," +
        "createdby   string," +
        "creationtime   string," +
        "customer   string," +
        "customergroup   string," +
        "debitcreditindicator   string," +
        "distributionchannel   string," +
        "division   string," +
        "documentdate   string," +
        "documentheadertext   string," +
        "documentlinenumberinsourcesystem   string," +
        "documentnumberinsourcesystem   string," +
        "documenttype   string," +
        "edgcreateditemindoc   string," +
        "entrydate   string," +
        "errorstatus   string," +
        "fidocumentquantity   string," +
        "fiscalperiod   string," +
        "fiscalyear   string," +
        "fsid   string," +
        "functionalareacode   string," +
        "glaccountcode   string," +
        "hleamount   string," +
        "indexfromcopa   string," +
        "itemcategory   string," +
        "itemtext   string," +
        "kitmaterial   string," +
        "kittype   string," +
        "leamount   string," +
        "lebillingtype   string," +
        "lecode   string," +
        "lecurrencycode   string," +
        "lesalesqty   string," +
        "lesalesqtyuom   string," +
        "ledgercode   string," +
        "localcompanycode   string," +
        "localdocumenttype   string," +
        "localfiscalperiod   string," +
        "localfiscalyear   string," +
        "localfunctionalareacode   string," +
        "localglaccountcode   string," +
        "locallecurrencycode   string," +
        "localledgercode   string," +
        "localmrccode   string," +
        "localprofitcenter   string," +
        "localsku   string," +
        "localversioncode   string," +
        "mrccode   string," +
        "parentdocumentnumberinsourcesystem   string," +
        "partnercostcenter   string," +
        "partnerfunctionalarea   string," +
        "partnerprofitcenter   string," +
        "partnersegment   string," +
        "payer   string," +
        "pcadocnumber   string," +
        "pcaitemnumber   string," +
        "plant   string," +
        "postingdate   string," +
        "postingkey   string," +
        "producthierarchy   string," +
        "psegment   string," +
        "rclnt   string," +
        "reference   string," +
        "referencedocument   string," +
        "referencetransaction   string," +
        "regionofshiptocustomer   string," +
        "salesdoctype   string," +
        "salesgroup   string," +
        "salesoffice   string," +
        "salesorder   string," +
        "salesorderitem   string," +
        "salesorganization   string," +
        "sectorproductgroup   string," +
        "shipto   string," +
        "sleamount   string," +
        "sourcesystemid   string," +
        "tradingpartner   string," +
        "transactioncode   string," +
        "transactioncurrencyamount   string," +
        "transactioncurrencycode   string," +
        "transactiontype   string," +
        "ujlkey   string," +
        "valuefieldfromcopa   string," +
        "vendor   string," +
        "versioncode   string )")

    session.sql ("create table ujs (" +
        "uuid   string," +
        "bravoequitycode   string," +
        "controllingarea   string," +
        "costcenter   string," +
        "creationtime   string," +
        "debitcreditindicator   string," +
        "errstatus   string," +
        "fiscalyear   string," +
        "fsid   string," +
        "functionalareacode   string," +
        "glaccountcode   string," +
        "hleamount   string," +
        "leamount   string," +
        "lecode   string," +
        "lecostelement   string," +
        "lecurrencycode   string," +
        "leplant   string," +
        "ledgercode   string," +
        "localcompanycode   string," +
        "localfiscalyear   string," +
        "localfunctionalareacode   string," +
        "localglaccountcode   string," +
        "locallecurrencycode   string," +
        "localledgercode   string," +
        "localmrccode   string," +
        "localprofitcenter   string," +
        "localversioncode   string," +
        "mrccode   string," +
        "partnerfunctionalarea   string," +
        "partnerprofitcenter   string," +
        "partnersegment   string," +
        "referencetransaction   string," +
        "sleamount   string," +
        "sourceadditionalkey   string," +
        "sourcesystemid   string," +
        "tradingpartner   string," +
        "transactioncurrencyamount   string," +
        "transactioncurrencycode   string," +
        "transactiontype   string," +
        "versioncode   string)")

    session.sql("create table gfs (" +
        "gfs string, " +
        " gfsdescription string, " +
        " globalfunctionalarea string )")

    session.sql("create table bravo (" +
        " bravo  string," +
        "bravodescription  string," +
        " gfs string, " +
        " gfsdescription string)")

    session.sql("create table gtw (" +
        "gfs string," +
        "gfsdescription  string," +
        "gtw  string," +
        "gtwdescription  string)")

    session.sql("create table coa (" +
        "accounttype   string," +
        "errorcode   string," +
        "errormessage   string," +
        "errorstatus   string," +
        "gfs   string," +
        "gfsdescription   string," +
        "globalfunctionalarea   string," +
        "indicevalue   string," +
        "localfunctionalarea   string," +
        "localgl   string," +
        "localgldescription   string)")

    session.sql(s"create or replace view TrialBalance as " +
        s"( select  leUniversal,gfs,first(gfsDescription) as gfsDescription, " +
        s"first(bravo) as bravo, " +
        s"first(bravoDescription) as bravoDescription,  first(gtw) as gtw, " +
        s"first(gtwDescription) as gtwDescription, " +
        s"first(globalFunctionalArea) as globalFunctionalArea," +
        s"format_number(sum(credit),2) as credit," +
        s" format_number(sum(debit),2) as debit,format_number(sum(total),2) as total from" +
        s" ( select a.leCode as leUniversal,a.localCompanyCode as leLocal," +
        s" a.mrcCode as mrcUniversal," +
        s" a.sourceSystemId as sourceSystem,a.glAccountCode as gfs," +
        s"a.localGlAccountCode as localGl," +
        s" SUM(hleAmount) as debit,SUM(sleAmount) as credit,SUM(leAmount) as total," +
        s" first(b.gfsDescription) as gfsDescription," +
        s" first(b.globalFunctionalArea) as globalFunctionalArea," +
        s" first((case when a.sourceSystemId='project_one' then e.localGlDescription " +
        s" when a.sourceSystemId='btb_latam' then b.gfsDescription else '' end)) " +
        s" as localGlDescription ," +
        s" first(c.bravoDescription) as bravoDescription," +
        s"first(d.gtwDescription) as gtwDescription, " +
        s" first(c.bravo) as bravo, first(d.gtw) as gtw from ( select ledgerCode,leCode," +
        s" localCompanyCode,mrcCode,fiscalYear,sourceSystemId,localGlAccountCode," +
        s" glAccountCode,last(localFunctionalAreaCode),SUM(leAmount) as leAmount," +
        s" SUM(hleAmount) as hleAmount,SUM(sleAmount) as sleAmount, glAccountCode ," +
        s" 'Local GL' as accountType,localGlAccountCode as localGl from " +
        s" ( select ledgerCode,leCode,localCompanyCode,mrcCode,fiscalYear,sourceSystemId," +
        s" localGlAccountCode,glAccountCode,localFunctionalAreaCode,leAmount,hleAmount,sleAmount" +
        s"  from ujli  where ledgerCode='0L' and leCode='7600' " +
        s" AND fiscalYear='2017' and fiscalPeriod<=3 AND sourceSystemId='btb_latam'  union all" +
        s" select ledgerCode,leCode,localCompanyCode,mrcCode,fiscalYear,sourceSystemId," +
        s" localGlAccountCode,glAccountCode,localFunctionalAreaCode,leAmount,hleAmount," +
        s" sleAmount  from ujs  where ledgerCode='0L' and leCode='7600'" +
        s" AND fiscalYear='2017' AND sourceSystemId='btb_latam' )  group by ledgerCode," +
        s" leCode,localCompanyCode,mrcCode,fiscalYear,sourceSystemId," +
        s" localGlAccountCode,glAccountCode ) a" +
        s" left join gfs b on (a.glAccountCode=b.gfs) left join " +
        s" bravo c " +
        s" on (a.glAccountCode=c.gfs)  left join gtw d on (a.glAccountCode=d.gfs)" +
        s" left join coa e on(a.accountType=e.accountType and " +
        s"  a.glAccountCode = e.gfs and a.localGl = e.localGl ) group by a.leCode," +
        s"a.localCompanyCode," +
        s" a.mrcCode,a.sourceSystemId,a.glAccountCode,a.localGlAccountCode," +
        s"c.bravo,d.gtw) group by leUniversal,gfs)")

    session.sql("select bravo from trialbalance group by bravo").collect

  }
}
