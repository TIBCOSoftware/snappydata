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
import java.util.Properties

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.util.TestUtils
import io.snappydata.{Constant, PlanTest, Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import org.apache.spark.SparkConf

class SecurityBugTest extends SnappyFunSuite with BeforeAndAfterAll {
  private val sysUser = "gemfire10"

  override def beforeAll(): Unit = {
    this.stopAll()
  }
  
  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)
    val conf = new org.apache.spark.SparkConf()
        .setAppName("BugTest")
        .setMaster("local[3]")
        .set(Attribute.AUTH_PROVIDER, ldapProperties.getProperty(Attribute.AUTH_PROVIDER))
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)

    if (addOn != null) {
      addOn(conf)
    } else {
      conf
    }
  }

  override def afterAll(): Unit = {
    this.stopAll()
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.clearProperty(k)
      System.clearProperty("gemfirexd." + k)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX  + k)
    }
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR)
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    System.setProperty("gemfirexd.authentication.required", "false")
  }

  test("Bug SNAP-2255 connection pool exhaustion") {
    val user1 = "gemfire1"
    val user2 = "gemfire2"

    val snc1 = snc.newSession()
    snc1.snappySession.conf.set(Attribute.USERNAME_ATTR, user1)
    snc1.snappySession.conf.set(Attribute.PASSWORD_ATTR, user1)

    snc1.sql(s"create table test (id  integer," +
        s" name STRING) using column")
    snc1.sql("insert into test values (1, 'name1')")
    snc1.sql(s"GRANT select ON TABLE  test TO  $user2")

    // TODO : Use the actual connection pool limit
    val limit = 500

    for (i <- 1 to limit) {
      val snc2 = snc.newSession()
      snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, user2)
      snc2.snappySession.conf.set(Attribute.PASSWORD_ATTR, user2)


      val rs = snc2.sql(s"select * from $user1.test").collect()
      assertEquals(1, rs.length)
    }
  }


  test("Bug SNAP-2332 . ParamLiteral found in First Aggregate Function") {
    val serverHostPort2 = TestUtil.startNetServer()

    val conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val stmt = conn.createStatement()
    val snappy = snc.snappySession
    snappy.sql(s"set ${Property.ColumnBatchSize.name}=5000")

    val insertDF = snappy.range(50).selectExpr("id", "(id * 12) as k",
      "concat('val', cast(100 as string)) as s")

    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k bigint, s varchar(10)) " +
        "using column options(buckets '8')")
    insertDF.write.insertInto("test")
    val query1 = "select sum(id) as summ, first(s, true) as firstt from test having " +
        "first(s, true) = 'val100'"
    var rs = snappy.sql(query1)
    rs.collect()
    rs = snappy.sql(query1)
    rs.collect()

    var resultset = stmt.executeQuery(query1)
    while(resultset.next()) {
      resultset.getDouble(1)
    }

    val query2 = "select sum(id) summ , first(s) firstt from test having first(s) = 'val100'"
    resultset = stmt.executeQuery(query2)
    while(resultset.next()) {
      resultset.getDouble(1)
    }
    rs = snappy.sql(query2)
    rs.collect()

    stmt.execute(s"create or replace view X as ($query2)")
    val query3 = "select * from X where summ > 0"
    rs = snappy.sql(query3)
    rs.collect()
    rs = snappy.sql(query3)
    rs.collect()
    resultset = stmt.executeQuery(query3)
    while(resultset.next()) {
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

    snappy.sql(table1)
    snappy.sql(table2)
    snappy.sql(table3)

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

    val q = "select fiscalPeriod , leCurrencyCode, leamount  from C1C2_V"

    snappy.sql(q).collect
    snappy.sql(q).collect
    resultset = stmt.executeQuery(q)
    while(resultset.next()) {
      resultset.getString(1)
    }
    val newSession1 = snappy.newSession()
    newSession1.sql(q).collect()

    newSession1.snappyContext.sparkContext.stop()

    snc.snappySession.sql(q).collect()


  }
}
