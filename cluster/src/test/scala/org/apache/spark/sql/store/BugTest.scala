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

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.{Constant, SnappyFunSuite}
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf

class BugTest extends SnappyFunSuite with BeforeAndAfterAll {
  private val sysUser = "gemfire10"

  override def beforeAll(): Unit = {
    this.stopAll()
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
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
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.clearProperty(k)
      System.clearProperty("gemfirexd." + k)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX + k)
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
    for (_ <- 1 to limit) {
      val snc2 = snc.newSession()
      snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, user2)
      snc2.snappySession.conf.set(Attribute.PASSWORD_ATTR, user2)


      val rs = snc2.sql(s"select * from $user1.test").collect()
      assertEquals(1, rs.length)
    }
  }

  test("SNAP-2342 nested query involving joins & union throws Exception") {
    val user1 = "gemfire1"
    val session = snc.newSession()
    session.snappySession.conf.set(Attribute.USERNAME_ATTR, user1)
    session.snappySession.conf.set(Attribute.PASSWORD_ATTR, user1)

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

    session.sql("create table ujs (" +
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
  }
}
