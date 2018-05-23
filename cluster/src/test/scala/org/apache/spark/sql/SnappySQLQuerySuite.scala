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
/*
 * Test for SPARK-10316 taken from Spark's DataFrameSuite having license as below.
 *
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

package org.apache.spark.sql

import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.Matchers._

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SnappyHashAggregateExec}
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.test.SQLTestData.TestData2


class SnappySQLQuerySuite extends SnappyFunSuite {

  private lazy val session: SnappySession = snc.snappySession

  // Ported test from Spark
  test("SNAP-1885 : left semi greater than predicate and equal operator") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
          TestData2(1, 1) ::
          TestData2(1, 2) ::
          TestData2(2, 1) ::
          TestData2(2, 2) ::
          TestData2(3, 1) ::
          TestData2(3, 2) :: Nil, 2))
    df.write.format("row").saveAsTable("testData2")

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.b and x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.a and x.a >= y.b + 1"),
      Seq(Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2))
    )
  }

  test("SNAP-1884 Join with temporary table not returning rows") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil))
    df.write.format("row").saveAsTable("lowerCaseData")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("subset1")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("subset2")
    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON " +
        "subset1.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset2 " +
        "ON subset2.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)

    snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    df.write.format("column").saveAsTable("collowerCaseData")

    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("colsubset1")
    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("colsubset2")
    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset1 ON " +
        "colsubset1.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset2 " +
        "ON colsubset2.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)
  }

  import session.implicits._

  test("SNAP-1840 -> uncorrelated scalar subquery") {

    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.write.format("row").saveAsTable("subqueryData")

    checkAnswer(
      session.sql("select -(select max(key) from subqueryData)"),
      Array(Row(-3))
    )

    checkAnswer(
      session.sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"),
      Array(Row(4))
    )

    checkAnswer(
      session.sql("select (select value from subqueryData limit 0)"),
      Array(Row(null))
    )

    checkAnswer(
      session.sql("select (select min(value) from subqueryData" +
          " where key = (select max(key) from subqueryData) - 1)"),
      Array(Row("two"))
    )
    session.dropTable("subqueryData", ifExists = true)
  }

  test("NOT EXISTS predicate subquery") {
    val row = identity[(java.lang.Integer, java.lang.Double)] _

    lazy val l = Seq(
      row(1, 2.0),
      row(1, 2.0),
      row(2, 1.0),
      row(2, 1.0),
      row(3, 3.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("a", "b")

    lazy val r = Seq(
      row(2, 3.0),
      row(2, 3.0),
      row(3, 2.0),
      row(4, 1.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("c", "d")

    l.write.format("row").saveAsTable("l")
    r.write.format("row").saveAsTable("r")

    checkAnswer(
      session.sql("select * from l where not exists (select * from r where l.a = r.c)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(null, null) :: Row(null, 5.0) :: Nil)
    checkAnswer(
      session.sql("select * from l where not exists " +
          "(select * from r where l.a = r.c and l.b < r.d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) ::
          Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

    session.dropTable("l", ifExists = true)
    session.dropTable("r", ifExists = true)
  }

  // taken from same test in Spark's DataFrameSuite
  test("SPARK-10316: allow non-deterministic expressions to project in PhysicalScan") {
    session.sql("create table rowTable (id long, id2 long) using row")
    session.range(1, 11).select($"id", $"id" * 2).write.insertInto("rowTable")
    val input = session.table("rowTable")

    val df = input.select($"id", rand(0).as('r))
    val result = df.as("a").join(df.filter($"r" < 0.5).as("b"), $"a.id" === $"b.id").collect()
    result.foreach { row =>
      assert(row.getDouble(1) - row.getDouble(3) === 0.0 +- 0.001)
    }
    session.sql("drop table rowTable")
  }

  test("AQP-292 snappy plan generation failure for aggregation on group by column") {
    session.sql("create table testTable (id long, tag string) using column options (buckets '2')")
    session.range(100000).selectExpr(
      "id", "concat('tag', cast ((id >> 6) as string)) as tag").write.insertInto("testTable")
    val query = "select tag, count(tag) c from testTable group by tag order by c desc"
    val rs = session.sql(query)
    // snappy aggregation should have been used for this query
    val plan = rs.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[SnappyHashAggregateExec]).isDefined)
    assert(plan.find(_.isInstanceOf[HashAggregateExec]).isEmpty)
    // collect the result to force default hashAggregateSize property take effect
    implicit val encoder = RowEncoder(rs.schema)
    val result = session.createDataset(rs.collect().toSeq)
    session.sql(s"set ${Property.HashAggregateSize} = -1")
    try {
      val ds = session.sql(query)
      val plan = ds.queryExecution.executedPlan
      assert(plan.find(_.isInstanceOf[SnappyHashAggregateExec]).isEmpty)
      assert(plan.find(_.isInstanceOf[HashAggregateExec]).isDefined)
      checkAnswer(result, ds.collect())
    } finally {
      session.sql(s"set ${Property.HashAggregateSize} = 0")
    }
  }

  test("Double exists") {
    val snc = new SnappySession(sc)
    snc.sql("create table r1(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row ")
    snc.sql("create table r2(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row")

    snc.sql("create table r3(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row")

    snc.insert("r1", Row(1, "1", "1", 100))
    snc.insert("r1", Row(2, "2", "2", 2))
    snc.insert("r1", Row(4, "4", "4", 4))
    snc.insert("r1", Row(7, "7", "7", 4))

    snc.insert("r2", Row(1, "1", "1", 1))
    snc.insert("r2", Row(2, "2", "2", 2))
    snc.insert("r2", Row(3, "3", "3", 3))

    snc.insert("r3", Row(1, "1", "1", 1))
    snc.insert("r3", Row(2, "2", "2", 2))
    snc.insert("r3", Row(4, "4", "4", 4))

    val df = snc.sql("select * from r1 where " +
        "(exists (select col1 from r2 where r2.col1=r1.col1) " +
        "or exists(select col1 from r3 where r3.col1=r1.col1))")

    df.collect()
    checkAnswer(df, Seq(Row(1, "1", "1", 100),
      Row(2, "2", "2", 2), Row(4, "4", "4", 4) ))
  }

  test("SNAP-2342 nested query involving joins & union throws Exception") {
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
  }
}

case class LowerCaseData(n: Int, l: String)
