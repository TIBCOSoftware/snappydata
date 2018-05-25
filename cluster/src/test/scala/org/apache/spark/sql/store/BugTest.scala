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

import java.sql.DriverManager

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

class BugTest extends SnappyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("SNAP-2342 nested query involving joins & union throws Exception") {
    snc.sql(s"create table ujli ( " +
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

    snc.sql("create table ujs (" +
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

    snc.sql("create table gfs (" +
        "gfs string, " +
        " gfsdescription string, " +
        " globalfunctionalarea string )")

    snc.sql("create table bravo (" +
        " bravo  string," +
        "bravodescription  string," +
        " gfs string, " +
        " gfsdescription string)")

    snc.sql("create table gtw (" +
        "gfs string," +
        "gfsdescription  string," +
        "gtw  string," +
        "gtwdescription  string)")

    snc.sql("create table coa (" +
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

    snc.sql(s"create or replace view TrialBalance as " +
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
  }

  test("Bug SNAP-2332 . ParamLiteral found in First/Last Aggregate Function") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession

    val insertDF = snappy.range(50).selectExpr("id", "(id * 12) as k",
      "concat('val', cast(100 as string)) as s")

    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k bigint, s varchar(10)) " +
        "using column options(buckets '8')")
    insertDF.write.insertInto("test")
    val query1 = "select sum(id) as summ, first(s, true) as firstt, last(s, true) as lastt" +
        " from test having (first(s, true) = 'val100' or last(s, true) = 'val100' )"


    var ps = conn.prepareStatement(query1)
    var resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    var rs = snappy.sql(query1)
    rs.collect()
    rs = snappy.sql(query1)
    rs.collect()

    resultset = stmt.executeQuery(query1)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    val query2 = "select sum(id) summ , first(s) firstt, last(s) lastt " +
        " from test having (first(s) = 'val100' or last(s) = 'val100' )"

    ps = conn.prepareStatement(query2)
    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    resultset = stmt.executeQuery(query2)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    rs = snappy.sql(query2)
    rs.collect()


    stmt.execute(s"create or replace view X as ($query2)")
    val query3 = "select * from X where summ > 0"
    ps = conn.prepareStatement(query3)
    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    rs = snappy.sql(query3)
    rs.collect()
    rs = snappy.sql(query3)
    rs.collect()
    resultset = stmt.executeQuery(query3)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    val table1 = "create table ujli (" +
        "aagmaterial string," +
        "accountassignmentgroup string," +
        "accounttype string," +
        "allocationcycle string," +
        "allocationsegment string," +
        "asset string," +
        "billingdocument string," +
        "billingdocumentitem string," +
        "bravoequitycode string," +
        "bravominorcode string," +
        "bsegdocumentlinenumber string," +
        "businessplace string," +
        "businesstransaction string," +
        "controllingarea string," +
        "copadocumentnumber string," +
        "copaobjectnumber string," +
        "costcenter string," +
        "costelement string," +
        "countryofshiptocustomer string," +
        "createdby string," +
        "creationtime string," +
        "customer string," +
        "customergroup string," +
        "debitcreditindicator string," +
        "distributionchannel string," +
        "division string," +
        "documentdate string," +
        "documentheadertext string," +
        "documentlinenumberinsourcesystem string," +
        "documentnumberinsourcesystem string," +
        "documenttype string," +
        "edgcreateditemindoc string," +
        "entrydate string," +
        "errorstatus string," +
        "fidocumentquantity string," +
        "fiscalperiod string," +
        "fiscalyear string," +
        "fsid string," +
        "functionalareacode string," +
        "glaccountcode string," +
        "hleamount string," +
        "indexfromcopa string," +
        "itemcategory string," +
        "itemtext string," +
        "kitmaterial string," +
        "kittype string," +
        "leamount string," +
        "lebillingtype string," +
        "lecode string," +
        "lecurrencycode string," +
        "lesalesqty string," +
        "lesalesqtyuom string," +
        "ledgercode string," +
        "localcompanycode string," +
        "localdocumenttype string," +
        "localfiscalperiod string," +
        "localfiscalyear string," +
        "localfunctionalareacode string," +
        "localglaccountcode string," +
        "locallecurrencycode string," +
        "localledgercode string," +
        "localmrccode string," +
        "localprofitcenter string," +
        "localsku string," +
        "localversioncode string," +
        "mrccode string," +
        "orderx string," +
        "orderreason string," +
        "parentdocumentnumberinsourcesystem string," +
        "partnercostcenter string," +
        "partnerfunctionalarea string," +
        "partnerprofitcenter string," +
        "partnersegment string," +
        "payer string," +
        "pcadocnumber string," +
        "pcaitemnumber string," +
        "plant string," +
        "postingdate string," +
        "postingkey string," +
        "producthierarchy string," +
        "psegment string," +
        "rclnt string," +
        "reference string," +
        "referencedocument string," +
        "referencetransaction string," +
        "regionofshiptocustomer string," +
        "salesdoctype string," +
        "salesgroup string," +
        "salesoffice string," +
        "salesorder string," +
        "salesorderitem string," +
        "salesorganization string," +
        "sectorproductgroup string," +
        "shipto string," +
        "sleamount string," +
        "sourcesystemid string," +
        "tradingpartner string," +
        "transactioncode string," +
        "transactioncurrencyamount string," +
        "transactioncurrencycode string," +
        "transactiontype string," +
        "ujlkey string," +
        "valuefieldfromcopa string," +
        "vendor string," +
        "versioncode string)"

    val table2 = "create table ledger (" +
        "globalledgercode string," +
        "globalledgercodedescription string," +
        "localledgercode string, " +
        "localledgercodedescription string," +
        "sourcesystemid string)"

    val table3 = "create table bravo_hier (" +
        "effectivedate string," +
        "enddate string," +
        "franchisecode string," +
        "franchisedesc string," +
        "majorproductgroupcode string," +
        "majorproductgroupdesc string," +
        "minorproductgroupcode string," +
        "minorproductgroupdesc string," +
        "publicreportingsegmentcode string," +
        "publicreportingsegmentdesc string," +
        "subfranchisecode string," +
        "subfranchisedesc string," +
        "worldwidefranchisecode string," +
        "worldwidefranchisedesc string)"

    stmt.execute(table1)
    stmt.execute(table2)
    stmt.execute(table3)

    val view = "CREATE or replace view C1C2_V as (SELECT " +
        "A.fiscalYear,first(A.fiscalPeriod) as fiscalPeriod,A.leCode, A.localLedgerCode," +
        "A.sourceSystemId,  A.glAccountCode,A.bravoMinorCode, A.mrccode, " +
        " first(A.leCurrencyCode) as leCurrencyCode," +
        " first(A.versionCode) as versionCode, first(A.functionalAreaCode) as functionalAreaCode," +
        "SUM(A.leAmount ) as leAmount,  " +
        "first(B.globalLedgerCodeDescription) as globalLedgerCodeDescription," +
        " first(C.publicReportingSegmentCode) as publicReportingSegmentCode," +
        " first(C.franchiseCode) as franchiseCode,  " +
        "first(C.worldwideFranchiseCode) as worldwideFranchiseCode," +
        " first(C.subFranchiseCode) as subFranchiseCode, " +
        "first(C.majorProductGroupCode) as majorProductGroupCode" +
        " FROM ujli A  LEFT JOIN ledger B " +
        " ON A.sourceSystemId = B.sourceSystemId AND " +
        " B.globalLedgerCode = A.localLedgerCode LEFT JOIN bravo_hier C ON " +
        " A.bravoMinorCode = minorProductGroupCode WHERE A.localLedgerCode ='0L' " +
        " GROUP BY A.leCode, A.mrcCode, " +
        "A.fiscalYear, A.sourceSystemId, A.glAccountCode, A.bravoMinorCode, A.localLedgerCode " +
        "having ( SUM(A.leAmount ) > 0.001F or SUM(A.leAmount ) < -0.001F) );"

    stmt.execute(view)

    val q = "SELECT lecode FROM C1C2_V GROUP BY 1"
    ps = conn.prepareStatement(q)

    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getString(1)
    }

    conn.close()
    TestUtil.stopNetServer()

  }
}
