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

package io.snappydata.benchmark.snappy

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import java.sql.{DriverManager, PreparedStatement, ResultSet, Statement}

/**
 * Created by kishor on 27/10/15.
 */
object TPCH_Snappy {

  var planFileStream: FileOutputStream = _
  var planPrintStream: PrintStream = _

  def close: Unit = if (planFileStream != null) {
    planPrintStream.close
    planFileStream.close()
  }

  def execute_statement(queryNumber: String, isResultCollection: Boolean, stmt: PreparedStatement,
      warmup: Integer, runsForAverage: Integer, avgPrintStream: PrintStream = null): Unit = {

    var queryFileStream = new FileOutputStream(new File(s"$queryNumber.out"))
    var queryPrintStream = new PrintStream(queryFileStream)

    var resultFormat = queryNumber match {
      case "1" => getResultString1
      case "2" => getResultString2
      case "3" => getResultString3
      case "4" => getResultString4
      case "5" => getResultString5
      case "6" => getResultString6
      case "7" => getResultString7
      case "8" => getResultString8
      case "9" => getResultString9
      case "10" => getResultString10
      case "11" => getResultString11
      case "12" => getResultString12
      case "13" => getResultString13
      case "14" => getResultString14
      case "15" => getResultString15
      case "16" => getResultString16
      case "17" => getResultString17
      case "18" => getResultString18
      case "19" => getResultString19
      case "20" => getResultString20
      case "21" => getResultString21
      case "22" => getResultString22
    }

    var rs: ResultSet = null
    try {
      // scalastyle:off println
      println(s"Started executing $queryNumber")
      if (isResultCollection) {
        rs = queryExecution(queryNumber, stmt)
        // queryPrintStream.println(s"$resultFormat")
        val rsmd = rs.getMetaData()
        val columnsNumber = rsmd.getColumnCount();
        var count: Int = 0
        while (rs.next()) {
          count += 1
          for (i <- 1 to columnsNumber) {
            if (i > 1) queryPrintStream.print(",")
            queryPrintStream.print(rs.getString(i))
          }
          queryPrintStream.println()
        }
        println(s"NUmber of results : $count")
        println(s"$queryNumber Result Collected in file $queryNumber.out")
        if (queryNumber.equals("q13")) {
          stmt.execute("drop view ViewQ13")
        }
        if (queryNumber.equals("q15")) {
          stmt.execute("drop view revenue")
        }
      } else {
        var totalTime: Long = 0
        for (i <- 1 to (warmup + runsForAverage)) {
          val startTime = System.currentTimeMillis()
          rs = queryExecution(queryNumber, stmt)
          // rs = stmt.executeQuery(query)
          while (rs.next()) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime
          queryPrintStream.println(s"$iterationTime")
          if (i > warmup) {
            totalTime += iterationTime
          }
          if (queryNumber.equals("q13")) {
            stmt.execute("drop view ViewQ13")
          }
          if (queryNumber.equals("q15")) {
            stmt.execute("drop view revenue")
          }
        }
        queryPrintStream.println(s"${totalTime / runsForAverage}")
        avgPrintStream.println(s"$queryNumber,${totalTime / runsForAverage}")
      }
      println(s"Finished executing $queryNumber")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        e.printStackTrace(queryPrintStream)
        e.printStackTrace(avgPrintStream)
        println(s" Exception while executing $queryNumber in written to file $queryNumber.txt")
      }
    } finally {
      if (isResultCollection) {
        queryPrintStream.close()
        queryFileStream.close()
      }

    }
    rs.close()
  }


  def execute(queryNumber: String, sqlContext: SQLContext, isResultCollection: Boolean,
      isSnappy: Boolean, itr: Int = 0, useIndex: Boolean = false, warmup: Int = 0,
      runsForAverage: Int = 1, avgPrintStream: PrintStream = null): Unit = {

    val planFileName = if (isSnappy) "Plan_Snappy.out" else "Plan_Spark.out"
    val queryFileName = if (isSnappy) s"Snappy_${queryNumber}.out" else s"Spark_${queryNumber}.out"

    if (planFileStream == null && planPrintStream == null) {
      planFileStream = new FileOutputStream(new File(planFileName))
      planPrintStream = new PrintStream(planFileStream)
    }

    val queryFileStream: FileOutputStream = new FileOutputStream(new File(queryFileName))
    val queryPrintStream: PrintStream = new PrintStream(queryFileStream)

    val resultFormat = queryNumber match {
      case "q" => getResultString
      case "1" => getResultString1
      case "2" => getResultString2
      case "3" => getResultString3
      case "4" => getResultString4
      case "5" => getResultString5
      case "6" => getResultString6
      case "7" => getResultString7
      case "8" => getResultString8
      case "9" => getResultString9
      case "10" => getResultString10
      case "11" => getResultString11
      case "12" => getResultString12
      case "13" => getResultString13
      case "14" => getResultString14
      case "15" => getResultString15
      case "16" => getResultString16
      case "17" => getResultString17
      case "18" => getResultString18
      case "19" => getResultString19
      case "20" => getResultString20
      case "21" => getResultString21
      case "22" => getResultString22
      case "1s" => getResultString1s
      case "3s" => getResultString3
      case "5s" => getResultString5s
      case "6s" => getResultString6
      case "10s" => getResultString10
    }

    // scalastyle:off println
    try {
      println(s"Started executing $queryNumber")
      if (isResultCollection) {
        val (resultSet, _) = queryExecution(queryNumber, sqlContext, useIndex, true)
        println(s"$queryNumber : ${resultSet.length}")

        for (row <- resultSet) {
          queryPrintStream.println(row.toSeq.map {
            case d: Double => "%18.4f".format(d).trim()
            case v => v
          }.mkString(","))
        }
        println(s"$queryNumber Result Collected in file $queryFileName")
      } else {
        var totalTimeForLast5Iterations: Long = 0
        queryPrintStream.println(queryNumber)
        for (i <- 1 to (warmup + runsForAverage)) {
          val startTime = System.currentTimeMillis()
          var cnts: Array[Row] = null
          if (i == 1) {
            cnts = queryExecution(queryNumber, sqlContext, useIndex, true)._1
          } else {
            cnts = queryExecution(queryNumber, sqlContext, useIndex)._1
          }
          for (s <- cnts) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime
          queryPrintStream.println(s"$iterationTime")
          if (i > warmup) {
            totalTimeForLast5Iterations += iterationTime
          }
          cnts = null
        }
        queryPrintStream.println(s"${totalTimeForLast5Iterations / runsForAverage}")
        avgPrintStream.println(s"$queryNumber,${totalTimeForLast5Iterations / runsForAverage}")
      }
      println(s"Finished executing $queryNumber")
    } catch {
      case e: Exception => {
        e.printStackTrace(queryPrintStream)
        e.printStackTrace(avgPrintStream)
        println(s" Exception while executing $queryNumber in written to file $queryFileName")
      }
    } finally {
      queryPrintStream.close()
      queryFileStream.close()
    }
    // scalastyle:on println
  }

  def printPlan(df: DataFrame, query: String): Unit = {
    // scalastyle:off println
    if (planPrintStream != null) {
      planPrintStream.println(query)
      planPrintStream.println(df.queryExecution.executedPlan)
    } else {
      df.explain(true)
    }
    // scalastyle:on println
  }

  def queryExecution(queryNumber: String, prepStatement: PreparedStatement): ResultSet = {

    val rs: ResultSet = queryNumber match {
      case "1" => {
        prepStatement.executeQuery()
      }
      case "2" => {
        prepStatement.executeQuery()
      }
      case "3" => {
        prepStatement.executeQuery()
      }
      case "4" => {
        prepStatement.executeQuery()
      }
      case "5" => {
        prepStatement.executeQuery()
      }
      case "6" => {
        prepStatement.executeQuery()
      }
      case "7" => {
        prepStatement.executeQuery()
      }
      case "8" => {
        prepStatement.executeQuery()
      }
      case "9" => {
        prepStatement.executeQuery()
      }
      case "10" => {
        prepStatement.executeQuery()
      }
      case "11" => {
        prepStatement.executeQuery()
      }
      case "12" => {
        prepStatement.executeQuery()
      }
      case "13" => {
        prepStatement.executeQuery()
      }
      case "14" => {
        prepStatement.executeQuery()
      }
      case "15" => {
        prepStatement.execute(getTempQuery15_Original())
        prepStatement.executeQuery(getQuery15_Original())
      }
      case "16" => {
        prepStatement.executeQuery()
      }
      case "17" => {
        prepStatement.executeQuery()
      }
      case "18" => {
        prepStatement.executeQuery()
      }
      case "19" => {
        prepStatement.executeQuery()
      }
      case "20" => {
        prepStatement.executeQuery()
      }
      case "21" => {
        prepStatement.executeQuery()
      }
      case "22" => {
        prepStatement.executeQuery()
      }
    }
    rs

  }


  def queryExecution(queryNumber: String, sqlContext: SQLContext, useIndex: Boolean,
      genPlan: Boolean = false): (scala.Array[org.apache.spark.sql.Row], DataFrame) = {
    val df = queryNumber match {
      case "1s" =>
        sqlContext.sql(getSampledQuery1)
      case "3s" =>
        sqlContext.sql(getSampledQuery3)
      case "5s" =>
        sqlContext.sql(getSampledQuery5)
      case "6s" =>
        sqlContext.sql(getSampledQuery6)
      case "10s" =>
        sqlContext.sql(getSampledQuery10)
      case "1" =>
        val df = sqlContext.sql(getQuery1)
        if (genPlan) {
          printPlan(df, "Q1")
        }
        df
      case "2" =>
        /*
        val result = sqlContext.sql(getTempQuery2())
        result.createOrReplaceTempView("ViewQ2")
        val df = sqlContext.sql(getQuery2())
        */
        val df = sqlContext.sql(getQuery2_Original)
        if (genPlan) {
          printPlan(df, "Q2")
        }
        df
      case "3" =>
        val df = sqlContext.sql(getQuery3)
        if (genPlan) {
          printPlan(df, "Q3")
        }
        df
      case "4" =>
        val df = sqlContext.sql(getQuery4)
        if (genPlan) {
          printPlan(df, "Q4")
        }
        df
      case "5" =>
        val df = sqlContext.sql(getQuery5)
        if (genPlan) {
          printPlan(df, "Q5")
        }
        df
      case "6" =>
        val df = sqlContext.sql(getQuery6)
        if (genPlan) {
          printPlan(df, "Q6")
        }
        df
      case "7" =>
        val df = sqlContext.sql(getQuery7)
        if (genPlan) {
          printPlan(df, "Q7")
        }
        df
      case "8" =>
        val df = sqlContext.sql(getQuery8(useIndex))
        if (genPlan) {
          printPlan(df, "Q8")
        }
        df
      case "9" =>
        val df = sqlContext.sql(getQuery9(useIndex))
        if (genPlan) {
          printPlan(df, "Q9")
        }
        df
      case "10" =>
        val df = sqlContext.sql(getQuery10)
        if (genPlan) {
          printPlan(df, "Q10")
        }
        df
      case "11" =>
        /*
         val result = sqlContext.sql(getTempQuery11)
         val res: Array[Row] = result.collect()
         var df: DataFrame = null
         var res1: Array[Row] = null
         df = sqlContext.sql(getQuery11(BigDecimal.apply(res(0).getDouble(0))))
         */
        val df = sqlContext.sql(getQuery11_Original)
        if (genPlan) {
          printPlan(df, "Q11")
        }
        df
      case "12" =>
        val df = sqlContext.sql(getQuery12)
        if (genPlan) {
          printPlan(df, "Q12")
        }
        df
      case "13" =>
        /*
        val result = sqlContext.sql(getTempQuery13(useIndex))
        result.createOrReplaceTempView("ViewQ13")
        val df = sqlContext.sql(getQuery13())
        */
        val df = sqlContext.sql(getQuery13_Original)
        if (genPlan) {
          printPlan(df, "Q13")
        }
        df
      case "14" =>
        val df = sqlContext.sql(getQuery14(useIndex))
        if (genPlan) {
          printPlan(df, "Q14")
        }
        df
      case "15" =>
        val result = sqlContext.sql(getTempQuery15_1)
        result.createOrReplaceTempView("revenue")

        val df = sqlContext.sql(getQuery15)
        if (genPlan) {
          printPlan(df, "Q15")
        }
        df
      case "16" =>
        val df = sqlContext.sql(getQuery16)
        if (genPlan) {
          printPlan(df, "Q16")
        }
        df
      case "17" =>
        /*
        val result = sqlContext.sql(getTempQuery17(useIndex))
        result.createOrReplaceTempView("ViewQ17")
        val df = sqlContext.sql(getQuery17(useIndex))
        */
        val df = sqlContext.sql(getQuery17_Original)
        if (genPlan) {
          printPlan(df, "Q17")
        }
        df
      case "18" =>
        val df = sqlContext.sql(getQuery18)
        if (genPlan) {
          printPlan(df, "Q18")
        }
        df
      case "19" =>
        val df = sqlContext.sql(getQuery19(useIndex))
        if (genPlan) {
          printPlan(df, "Q19")
        }
        df
      case "20" =>
//        val result = sqlContext.sql(getTempQuery20(useIndex))
//        result.createOrReplaceTempView("ViewQ20")
        val df = sqlContext.sql(getQuery20_Original)
        if (genPlan) {
          printPlan(df, "Q20")
        }
        df
      case "21" =>
        val df = sqlContext.sql(getQuery21)
        if (genPlan) {
          printPlan(df, "Q21")
        }
        df
      case "22" =>
        val df = sqlContext.sql(getQuery22_Original)
        if (genPlan) {
          printPlan(df, "Q22")
        }
        df
    }

    (df.collect(), df)
  }


  def getQuery: String =
  // "select N_NAME from NATION where N_REGIONKEY = (select R_REGIONKEY from REGION where
  // R_NAME='ASIA')"
    "select count(*) from LINEITEM"

  def getResultString1s: String =
    "sl_returnflag|l_linestatus|sum_qty|sum_qty_err|sum_base_price|sum_base_price_err" +
        "|sum_disc_price|sum_disc_price_err|sum_charge|sum_charge_err|" +
        "avg_qty|avg_qty_err|avg_price|avg_price_err|avg_disc|avg_disc_err|count_order"

  def getResultString5s: String =
    "N_NAME|revenue|revenue_err"

  def getResultString: String = ""

  def getSampledQuery1: String =
  // DELTA = 90
    " select" +
        "     l_returnflag," +
        "     l_linestatus," +
        "     sum(l_quantity) as sum_qty," +
        "     error estimate sum(l_quantity) as sum_qty_err," +
        "     sum(l_extendedprice) as sum_base_price," +
        "     error estimate sum(l_extendedprice) as sum_base_price_err," +
        "     sum(l_extendedprice*(1-l_discount)) as sum_disc_price," +
        "     error estimate sum(l_extendedprice*(1-l_discount)) as sum_disc_price_err," +
        "     sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
        "     error estimate sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge_err," +
        "     avg(l_quantity) as avg_qty," +
        "     error estimate avg(l_quantity) as avg_qty_err," +
        "     avg(l_extendedprice) as avg_price," +
        "     error estimate avg(l_extendedprice) as avg_price_err," +
        "     avg(l_discount) as avg_disc," +
        "     error estimate avg(l_discount) as avg_disc_err," +
        "     count(*) as count_order" +
        " from" +
        "     lineitem_sampled" +
        " where" +
        "     l_shipdate <= DATE_SUB('1998-12-01',90)" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"

  def getSampledQuery3: String = {
    //    1. SEGMENT = BUILDING;
    //    2. DATE = 1995-03-15.
    " select" +
        "     l_orderkey," +
        "     sum(l_extendedprice*(1-l_discount)) as revenue," +
        "     error estimate sum(l_extendedprice*(1-l_discount)) as revenue_err," +
        "     o_orderdate," +
        "     o_shippriority" +
        " from" +
        "     orders_sampled," +
        "     LINEITEM," +
        "     CUSTOMER" +
        " where" +
        "     C_MKTSEGMENT = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "     and o_orderdate < add_months('1995-03-15',0)" +
        "     and l_shipdate > add_months('1995-03-15',0) " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate"
  }

  def getSampledQuery3_1: String = {
    //    1. SEGMENT = BUILDING;
    //    2. DATE = 1995-03-15.
    " select" +
        "     l_orderkey," +
        "     sum(l_extendedprice*(1-l_discount)) as revenue," +
        "     error estimate sum(l_extendedprice*(1-l_discount)) as revenue_err," +
        "     o_orderdate," +
        "     o_shippriority" +
        " from" +
        "     ORDERS," +
        "     lineitem_sampled," +
        "     CUSTOMER" +
        " where" +
        "     C_MKTSEGMENT = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "     and o_orderdate < add_months('1995-03-15',0)" +
        "     and l_shipdate > add_months('1995-03-15',0) " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate"
  }

  def getSampledQuery5: String = {
    // 1. REGION = ASIA;
    // 2. DATE = 1994-01-01.
    " select" +
        "        N_NAME," +
        "        sum(l_extendedprice * (1 - l_discount)) as revenue," +
        "        error estimate sum(l_extendedprice * (1 - l_discount)) as revenue_err" +
        " from" +
        "        CUSTOMER," +
        "        ORDERS," +
        "        lineitem_sampled," +
        "        SUPPLIER," +
        "        NATION," +
        "        REGION" +
        " where" +
        "        C_CUSTKEY = o_custkey" +
        "        and l_orderkey = o_orderkey" +
        "        and l_suppkey = S_SUPPKEY" +
        "        and C_NATIONKEY = S_NATIONKEY" +
        "        and S_NATIONKEY = N_NATIONKEY" +
        "        and N_REGIONKEY = R_REGIONKEY" +
        "        and R_NAME = 'ASIA'" +
        "        and o_orderdate >= add_months('1994-01-01',0)" +
        // "        and o_orderdate < date '[DATE]' + interval '1' year" +
        "        and o_orderdate < add_months('1994-01-01', 12)" +
        " group by" +
        "        N_NAME" +
        " order by" +
        "        revenue desc"
  }

  def getSampledQuery6: String = {
    // 1. DATE = 1994-01-01;
    // 2. DISCOUNT = 0.06;
    // 3. QUANTITY = 24.
    " select" +
        "        sum(l_extendedprice*l_discount) as revenue," +
        "        error estimate sum(l_extendedprice*l_discount) as revenue_err" +
        " from" +
        "        lineitem_sampled" +
        " where" +
        "        l_shipdate >= add_months('1994-01-01',0)" +
        "        and l_shipdate < add_months('1994-01-01', 12)" +
        "        and l_discount between 0.06 - 0.01 and 0.06 + 0.01" +
        "        and l_quantity < 24"
  }

  def getSampledQuery10: String = {
    // 1.    DATE = 1993-10-01.
    "select" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         sum(l_extendedprice * (1 - l_discount)) as revenue," +
        "         error estimate sum(l_extendedprice * (1 - l_discount)) as revenue_err," +
        "         C_ACCTBAL," +
        "         N_NAME," +
        "         C_ADDRESS," +
        "         C_PHONE," +
        "         C_COMMENT" +
        " from" +
        "         orders_sampled," +
        "         LINEITEM," +
        "         NATION," +
        "         CUSTOMER" +
        " where" +
        "         C_CUSTKEY = o_custkey" +
        "         and l_orderkey = o_orderkey" +
        "         and o_orderdate >= add_months('1993-10-01',0)" +
        "         and o_orderdate < add_months('1993-10-01', 3)" +
        "         and l_returnflag = 'R'" +
        "         and C_NATIONKEY = N_NATIONKEY" +
        " group by" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         C_ACCTBAL," +
        "         C_PHONE," +
        "         N_NAME," +
        "         C_ADDRESS," +
        "         C_COMMENT" +
        " order by" +
        "         revenue desc"
  }

  def getSampledQuery10_1: String = {
    "select" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         sum(l_extendedprice * (1 - l_discount)) as revenue," +
        "         error estimate sum(l_extendedprice * (1 - l_discount)) as revenue_err," +
        "         C_ACCTBAL," +
        "         N_NAME," +
        "         C_ADDRESS," +
        "         C_PHONE," +
        "         C_COMMENT" +
        " from" +
        "         ORDERS," +
        "         lineitem_sampled," +
        "         NATION," +
        "         CUSTOMER" +
        " where" +
        "         C_CUSTKEY = o_custkey" +
        "         and l_orderkey = o_orderkey" +
        "         and o_orderdate >= add_months('1993-10-01',0)" +
        "         and o_orderdate < add_months('1993-10-01', 3)" +
        "         and l_returnflag = 'R'" +
        "         and C_NATIONKEY = N_NATIONKEY" +
        " group by" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         C_ACCTBAL," +
        "         C_PHONE," +
        "         N_NAME," +
        "         C_ADDRESS," +
        "         C_COMMENT" +
        " order by" +
        "         revenue desc"
  }

  def getQuery1: String = {
    // DELTA = 90
    " select" +
        "     l_returnflag," +
        "     l_linestatus," +
        "     sum(l_quantity) as sum_qty," +
        "     sum(l_extendedprice) as sum_base_price," +
        "     sum(l_extendedprice*(1-l_discount)) as sum_disc_price," +
        "     sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
        "     avg(l_quantity) as avg_qty," +
        "     avg(l_extendedprice) as avg_price," +
        "     avg(l_discount) as avg_disc," +
        "     count(*) as count_order" +
        " from" +
        "     LINEITEM" +
        " where" +
        "     l_shipdate <= DATE_SUB('1997-12-31',90)" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"
  }

  def getResultString1: String = {
    "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price" +
        "|avg_disc|count_order"
  }

  def getTempQuery2(): String = {
    " select" +
        "     min(ps_supplycost) as v_supplycost, ps_partkey as v_partkey" +
        " from" +
        "     PARTSUPP,SUPPLIER,NATION, REGION" +
        " where" +
        "     s_suppkey = ps_suppkey" +
        "     and s_nationkey = n_nationkey" +
        "     and n_regionkey = r_regionkey" +
        "     and r_name = 'ASIA'" +
        " group by" +
        "     ps_partkey"
  }

  def getQuery2(): String = {
      " select" +
          "     s_acctbal," +
          "     s_name," +
          "     n_name," +
          "     p_partkey," +
          "     p_mfgr," +
          "     s_address," +
          "     s_phone," +
          "     s_comment" +
          " from" +
          "     PART," +
          "     PARTSUPP," +
          "     SUPPLIER," +
          "     NATION," +
          "     REGION," +
          "     ViewQ2" +
          " where" +
          "     p_partkey = ps_partkey" +
          "     and s_suppkey = ps_suppkey" +
          "     and p_size = 24" +
          "     and p_type like '%STEEL'" +
          "     and s_nationkey = n_nationkey" +
          "     and n_regionkey = r_regionkey" +
          "     and r_name = 'ASIA'" +
          "     and p_partkey = v_partkey" +
          "     and ps_supplycost =  v_supplycost" +
          " order by" +
          "     s_acctbal desc," +
          "     n_name," +
          "     s_name," +
          "     p_partkey" +
          " limit 100"
  }

  def getQuery2_Original: String = {
    " select" +
      "     S_ACCTBAL," +
      "     S_NAME," +
      "     N_NAME," +
      "     P_PARTKEY," +
      "     P_MFGR," +
      "     S_ADDRESS," +
      "     S_PHONE," +
      "     S_COMMENT" +
      " from" +
      "     SUPPLIER," +
      "     NATION," +
      "     REGION," +
      "     PART," +
      "     PARTSUPP" +
      " where" +
      "     S_NATIONKEY = N_NATIONKEY" +
      "     and N_REGIONKEY = R_REGIONKEY" +
      "     and R_NAME = 'ASIA'" +
      "     and S_SUPPKEY = PS_SUPPKEY" +
      "     and P_PARTKEY = PS_PARTKEY" +
      "     and P_SIZE = 24" +
      "     and P_TYPE like '%STEEL'" +
      "     and PS_SUPPLYCOST = (" +
      "         select" +
      "             min(PS_SUPPLYCOST)" +
      "         from" +
      "             SUPPLIER, NATION," +
      "             REGION, PARTSUPP" +
      "         where" +
      "             S_NATIONKEY = N_NATIONKEY" +
      "             and N_REGIONKEY = R_REGIONKEY" +
      "             and R_NAME = 'ASIA'" +
      "             and S_SUPPKEY = PS_SUPPKEY" +
      "             and P_PARTKEY = PS_PARTKEY" +
      "            )" +
      " order by" +
      "     S_ACCTBAL desc," +
      "     N_NAME," +
      "     S_NAME," +
      "     P_PARTKEY" +
      " limit 100"
  }

  def getResultString2(): String = {
    "S_ACCTBAL|S_NAME|N_NAME|P_PARTKEY|P_MFGR|S_ADDRESS|S_PHONE|S_COMMENT"
  }

  def getQuery3: String = {
    " select" +
        "     l_orderkey," +
        "     sum(l_extendedprice*(1-l_discount)) as revenue," +
        "     o_orderdate," +
        "     o_shippriority" +
        " from" +
        "    ORDERS," +
        "    LINEITEM," +
        "    CUSTOMER " +
        " where" +
        "     C_MKTSEGMENT = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "      and o_orderdate < '1995-03-15'" +
        "     and l_shipdate > '1995-03-15' " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate" +
      " limit 10"
  }

  def getResultString3: String = {
    "l_orderkey|revenue|o_orderdate|o_shippriority"
  }

  def getQuery4: String = {
    // 1.DATE = 1993-07-01.
    " select" +
        "     o_orderpriority," +
        "     count(*) as order_count" +
        " from" +
        "     ORDERS" +
        " where" +
        "     o_orderdate >= '1993-07-01'" +
        "     and o_orderdate < add_months('1993-07-01',3)" +
        "     and exists (" +
        "         select" +
        "             l_orderkey" +
        "         from" +
        "             LINEITEM" +
        "         where" +
        "             l_orderkey = o_orderkey" +
        "             and l_commitdate < l_receiptdate" +
        "         )" +
        " group by" +
        "     o_orderpriority" +
        " order by" +
        "     o_orderpriority"
  }

  def getResultString4: String = {
    "o_orderpriority|order_count"
  }

  def getQuery5: String = {
    // 1. REGION = ASIA;
    // 2. DATE = 1994-01-01.
      " select" +
          "        n_name," +
          "        sum(l_extendedprice * (1 - l_discount)) as revenue" +
          " from" +
          "        SUPPLIER," +
          "        NATION," +
          "        REGION," +
          "        ORDERS," +
          "        LINEITEM," +
          "        CUSTOMER" +
          " where" +
          "        s_nationkey = n_nationkey" +
          "        and n_regionkey = r_regionkey" +
          "        and r_name = 'ASIA'" +
          "        and C_CUSTKEY = o_custkey" +
          "        and l_orderkey = o_orderkey" +
          "        and l_suppkey = s_suppkey" +
          "        and C_NATIONKEY = s_nationkey" +
          "        and o_orderdate >= '1994-01-01'" +
          "        and o_orderdate < add_months('1994-01-01', 12)" +
          " group by" +
          "        n_name" +
          " order by" +
          "        revenue desc"
  }

  def getResultString5: String = {
    "N_NAME|revenue"
  }

  def getQuery6: String = {
    // 1. DATE = 1994-01-01;
    // 2. DISCOUNT = 0.06;
    // 3. QUANTITY = 24.
    " select" +
        "        sum(l_extendedprice*l_discount) as revenue" +
        " from" +
        "        LINEITEM" +
        " where" +
        "        l_shipdate >= '1994-01-01'" +
        "        and l_shipdate < add_months('1994-01-01', 12)" +
        "        and l_discount between 0.06 - 0.01 and 0.06 + 0.01" +
        "        and l_quantity < 24"
  }

  def getResultString6: String = {
    "revenue"
  }

  def getQuery7: String = {
    //    1. NATION1 = FRANCE;
    //    2. NATION2 = GERMANY.
      "select" +
          "         supp_nation," +
          "         cust_nation," +
          "         l_year, " +
          "         sum(volume) as revenue" +
          " from (" +
          "         select" +
          "                 n1.n_name as supp_nation," +
          "                 n2.n_name as cust_nation," +
          //        "                 extract m(year from l_shipdate) as l_year," +
          "                 year(l_shipdate) as l_year," +
          "                 l_extendedprice * (1 - l_discount) as volume" +
          "         from" +
          "                 SUPPLIER," +
          "                 LINEITEM," +
          "                 ORDERS," +
          "                 CUSTOMER," +
          "                 NATION n1," +
          "                 NATION n2" +
          "         where" +
          "                 s_suppkey = l_suppkey" +
          "                 and o_orderkey = l_orderkey" +
          "                 and C_CUSTKEY = o_custkey" +
          "                 and s_nationkey = n1.n_nationkey" +
          "                 and C_NATIONKEY = n2.n_nationkey" +
          "                 and (" +
          "                         (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')" +
          "                      or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')" +
          "                 )" +
          "                 and l_shipdate between '1995-01-01' and '1996-12-31'" +
          "         ) as shipping" +
          " group by" +
          "         supp_nation," +
          "         cust_nation," +
          "         l_year" +
          " order by" +
          "         supp_nation," +
          "         cust_nation," +
          "         l_year"
  }

  def getResultString7: String = {
    "supp_nation|cust_nation|l_year|revenue"
  }

  def getQuery8(useIndex: Boolean): String = {
    //    1. NATION = BRAZIL;
    //    2. REGION = AMERICA;
    //    3. TYPE = ECONOMY ANODIZED STEEL.
    if (!useIndex) {
        "select" +
            "         o_year," +
            "         sum(case" +
            "                 when nation = 'BRAZIL'" +
            "                 then volume" +
            "                 else 0" +
            "                 end) / sum(volume) as mkt_share" +
            "         from (" +
            "                 select" +
            "                         year(o_orderdate) as o_year," +
            "                         l_extendedprice * (1-l_discount) as volume," +
            "                         n2.n_name as nation" +
            "                 from" +
            "                         LINEITEM," +
            "                         PART," +
            "                         ORDERS," +
            "                         CUSTOMER," +
            "                         NATION n1," +
            "                         REGION," +
            "                         NATION n2," +
            "                         SUPPLIER" +
            "                 where" +
            "                         p_partkey = l_partkey" +
            "                         and l_orderkey = o_orderkey" +
            "                         and o_custkey = C_CUSTKEY" +
            "                         and C_NATIONKEY = n1.n_nationkey" +
            "                         and n1.n_regionkey = r_regionkey" +
            "                         and r_name = 'AMERICA'" +
            "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
            "                         and p_type = 'ECONOMY ANODIZED STEEL'" +
            "                         and s_suppkey = l_suppkey" +
            "                         and s_nationkey = n2.n_nationkey" +
            "         ) as all_nations" +
            " group by" +
            "         o_year" +
            " order by" +
            "         o_year"
    } else {
        "select" +
            "         o_year," +
            "         sum(case" +
            "                 when nation = 'BRAZIL'" +
            "                 then volume" +
            "                 else 0" +
            "                 end) / sum(volume) as mkt_share" +
            "         from (" +
            "                 select" +
            "                         year(o_orderdate) as o_year," +
            "                         l_extendedprice * (1-l_discount) as volume," +
            "                         n2.n_name as nation" +
            "                 from" +
            "                         LINEITEM_PART," +
            "                         PART," +
            "                         SUPPLIER," +
            "                         ORDERS," +
            "                         CUSTOMER," +
            "                         NATION n1," +
            "                         NATION n2," +
            "                         REGION" +
            "                 where" +
            "                         p_partkey = l_partkey" +
            "                         and s_suppkey = l_suppkey" +
            "                         and l_orderkey = o_orderkey" +
            "                         and o_custkey = C_CUSTKEY" +
            "                         and C_NATIONKEY = n1.n_nationkey" +
            "                         and n1.n_regionkey = r_regionkey" +
            "                         and r_name = 'AMERICA'" +
            "                         and s_nationkey = n2.n_nationkey" +
            "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
            "                         and p_type = 'ECONOMY ANODIZED STEEL'" +
            "         ) as all_nations" +
            " group by" +
            "         o_year" +
            " order by" +
            "         o_year"
      }
  }

  def getResultString8: String = {
    "YEAR|MKT_SHARE"
  }

  def getQuery9(useIndex: Boolean): String = {
    // 1. COLOR = green.
    if (!useIndex) {
      "select" +
          "         nation," +
          "         o_year," +
          "         sum(amount) as sum_profit" +
          " from (" +
          "         select" +
          "                 n_name as nation," +
          "                 year(o_orderdate) as o_year," +
          "                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as " +
          "amount" +
          "         from" +
          "                 LINEITEM," +
          "                 PART," +
          "                 ORDERS," +
          "                 SUPPLIER," +
          "                 NATION," +
          "                 PARTSUPP" +
          "         where" +
          "                 s_suppkey = l_suppkey" +
          "                 and ps_suppkey = l_suppkey" +
          "                 and ps_partkey = l_partkey" +
          "                 and p_partkey = l_partkey" +
          "                 and o_orderkey = l_orderkey" +
          "                 and s_nationkey = n_nationkey" +
          "                 and p_name like '%green%'" +
          "         ) as profit" +
          " group by" +
          "         nation," +
          "         o_year" +
          " order by" +
          "         nation," +
          "         o_year desc"
    } else {
      "select" +
          "         nation," +
          "         o_year," +
          "         sum(amount) as sum_profit" +
          " from (" +
          "         select" +
          "                 n_name as nation," +
          "                 year(o_orderdate) as o_year," +
          "                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as " +
          "amount" +
          "         from" +
          "                 PART," +
          "                 LINEITEM_PART," +
          "                 PARTSUPP," +
          "                 SUPPLIER," +
          "                 ORDERS," +
          "                 NATION" +
          "         where" +
          "                 s_suppkey = l_suppkey" +
          "                 and ps_suppkey = l_suppkey" +
          "                 and ps_partkey = l_partkey" +
          "                 and p_partkey = l_partkey" +
          "                 and o_orderkey = l_orderkey" +
          "                 and s_nationkey = n_nationkey" +
          "                 and p_name like '%green%'" +
          "         ) as profit" +
          " group by" +
          "         nation," +
          "         o_year" +
          " order by" +
          "         nation," +
          "         o_year desc"

    }
  }

  def getResultString9: String = {
    "NATION|YEAR|SUM_PROFIT"
  }

  def getQuery10: String = {
    // 1.DATE = 1993-10-01.
      "select" +
          "         C_CUSTKEY," +
          "         C_NAME," +
          "         sum(l_extendedprice * (1 - l_discount)) as revenue," +
          "         C_ACCTBAL," +
          "         n_name," +
          "         C_ADDRESS," +
          "         C_PHONE," +
          "         C_COMMENT" +
          " from" +
          "         ORDERS," +
          "         LINEITEM," +
          "         CUSTOMER," +
          "         NATION" +
          " where" +
          "         C_CUSTKEY = o_custkey" +
          "         and l_orderkey = o_orderkey" +
          "         and o_orderdate >= '1993-10-01'" +
          "         and o_orderdate < add_months('1993-10-01', 3)" +
          "         and l_returnflag = 'R'" +
          "         and C_NATIONKEY = n_nationkey" +
          " group by" +
          "         C_CUSTKEY," +
          "         C_NAME," +
          "         C_ACCTBAL," +
          "         C_PHONE," +
          "         n_name," +
          "         C_ADDRESS," +
          "         C_COMMENT" +
          " order by" +
          "         revenue desc" +
        " limit 20"
  }

  def getResultString10: String = {
    "C_CUSTKEY|C_NAME|REVENUE|C_ACCTBAL|N_NAME|C_ADDRESS|C_PHONE|C_COMMENT"
  }

  def getTempQuery11: String = {
    " select" +
        "         sum(ps_supplycost * ps_availqty) * 0.0001" +
        " from" +
        "         PARTSUPP," +
        "         SUPPLIER," +
        "         NATION" +
        " where" +
        "         ps_suppkey = s_suppkey" +
        "         and s_nationkey = n_nationkey" +
        "         and n_name = 'GERMANY'"
    // }
  }

  def getQuery11(value: Any): String = {
    //    1. NATION = GERMANY;
    //    2. FRACTION = 0.0001.
    " select" +
        "         ps_partkey," +
        "         sum(ps_supplycost * ps_availqty) as value" +
        " from" +
        "         PARTSUPP," +
        "         SUPPLIER," +
        "         NATION" +
        " where" +
        "         ps_suppkey = s_suppkey" +
        "         and s_nationkey = n_nationkey" +
        "         and n_name = 'GERMANY'" +
        " group by" +
        "         ps_partkey having" +
        "         sum(ps_supplycost * ps_availqty) > " +
        value +
        " order by" +
        "         value desc"
  }

  def getQuery11_Original: String = {
    //    1. NATION = GERMANY;
    //    2. FRACTION = 0.0001.
    "select" +
      "         PS_PARTKEY," +
      "         sum(PS_SUPPLYCOST * PS_AVAILQTY) as value" +
      " from" +
      "         SUPPLIER," +
      "         NATION," +
      "         PARTSUPP" +
      " where" +
      "         PS_SUPPKEY = S_SUPPKEY" +
      "         and S_NATIONKEY = N_NATIONKEY" +
      "         and N_NAME = 'GERMANY'" +
      " group by" +
      "         PS_PARTKEY having" +
      "         sum(PS_SUPPLYCOST * PS_AVAILQTY) > (" +
      "                 select" +
      "                         sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001" +
      "                 from" +
      "                         SUPPLIER," +
      "                         NATION," +
      "                         PARTSUPP" +
      "                 where" +
      "                         PS_SUPPKEY = S_SUPPKEY" +
      "                         and S_NATIONKEY = N_NATIONKEY" +
      "                         and N_NAME = 'GERMANY'" +
      "         )" +
      " order by" +
      "         value desc"
  }

  def getResultString11(): String = {
    "PS_PARTKEY|VALUE"
  }


  def getQuery12: String = {
    //    1.SHIPMODE1 = MAIL;
    //    2. SHIPMODE2 = SHIP;
    //    3. DATE = 1994-01-01.
    "select" +
        "         l_shipmode," +
        "         sum(case" +
        "                 when o_orderpriority ='1-URGENT'" +
        "                 or o_orderpriority ='2-HIGH'" +
        "                 then 1" +
        "                 else 0" +
        "                 end" +
        "         ) as high_line_count," +
        "         sum(case" +
        "                 when o_orderpriority <> '1-URGENT'" +
        "                 and o_orderpriority <> '2-HIGH'" +
        "                 then 1" +
        "                 else 0" +
        "                 end" +
        "         ) as low_line_count" +
        " from" +
        "         ORDERS," +
        "         LINEITEM" +
        " where" +
        "         o_orderkey = l_orderkey" +
        "         and l_shipmode in ('MAIL', 'SHIP')" +
        "         and l_commitdate < l_receiptdate" +
        "         and l_shipdate < l_commitdate" +
        "         and l_receiptdate >= '1994-01-01'" +
        "         and l_receiptdate < add_months('1994-01-01',12)" +
        " group by" +
        "         l_shipmode" +
        " order by" +
        "         l_shipmode"
  }

  def getResultString12: String = {
    "L_SHIPMODE|HIGH_LINE_COUNT|LOW_LINE_COUNT"
  }

  def getTempQuery13(useIndex: Boolean): String = {
    if (!useIndex) {
      "select" +
          "        C_CUSTKEY," +
          "        count(o_orderkey) as c_count" +
          " from" +
          "        CUSTOMER left outer join ORDERS on" +
          "        C_CUSTKEY = o_custkey" +
          "        and o_comment not like '%special%requests%'" +
          " group by" +
          "        C_CUSTKEY"
    } else {
      "select" +
          "        C_CUSTKEY," +
          "        count(o_orderkey) as c_count" +
          " from" +
          "        CUSTOMER left outer join ORDERS_CUST on" +
          "        C_CUSTKEY = o_custkey" +
          "        and o_comment not like '%special%requests%'" +
          " group by" +
          "        C_CUSTKEY"
    }
  }

  def getQuery13(): String = {
    //    1. WORD1 = special.
    //    2. WORD2 = requests.
    "select" +
        "        c_count, " +
        "        count(*) as custdist" +
        " from " +
        "         ViewQ13" +
        " group by" +
        "         c_count" +
        " order by" +
        "         custdist desc," +
        "         c_count desc"
  }

  def getQuery13_Original: String = {
      //    1. WORD1 = special.
      //    2. WORD2 = requests.
      "select" +
          "         c_count, " +
          "         count(*) as custdist" +
          " from (" +
          "         select" +
          "                 C_CUSTKEY," +
          "                 count(o_orderkey) as c_count" +
          "         from" +
          "                 CUSTOMER left outer join ORDERS on" +
          "                 C_CUSTKEY = o_custkey" +
          "                 and o_comment not like '%special%requests%'" +
          "         group by" +
          "                 C_CUSTKEY" +
          "         ) as c_orders" +
          " group by" +
          "         c_count" +
          " order by" +
          "         custdist desc," +
          "         c_count desc"
  }

  def getResultString13(): String = {
    "C_COUNT|CUSTDIST"
  }

  def getQuery14(useIndex: Boolean): String = {
    // 1.DATE = 1995-09-01.
      if (!useIndex) {
        "select" +
            "         100.00 * sum(case" +
            "                 when p_type like 'PROMO%'" +
            "                 then l_extendedprice*(1-l_discount)" +
            "                 else 0" +
            "                 end" +
            "         ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue" +
            " from" +
            "         LINEITEM," +
            "         PART" +
            " where" +
            "         l_partkey = p_partkey" +
            "         and l_shipdate >= '1995-09-01'" +
            "         and l_shipdate < add_months ('1995-09-01', 1)"
      } else {
        "select" +
            "         100.00 * sum(case" +
            "                 when p_type like 'PROMO%'" +
            "                 then l_extendedprice*(1-l_discount)" +
            "                 else 0" +
            "                 end" +
            "         ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue" +
            " from" +
            "         LINEITEM_PART," +
            "         PART" +
            " where" +
            "         l_partkey = p_partkey" +
            "         and l_shipdate >= '1995-09-01'" +
            "         and l_shipdate < add_months ('1995-09-01', 1)"
      }
  }

  def getResultString14: String = {
    "PROMO_REVENUE"
  }

  def getTempQuery15_Original(): String = {
    "create view " +
        "        revenue as" +
        " select" +
        "      l_suppkey as supplier_no ," +
        "      sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
        " from" +
        "      LINEITEM" +
        " where" +
        "      l_shipdate >= '1996-01-01'" +
        "      and l_shipdate < '1996-01-01' + interval '3' month" +
        " group by" +
        "      l_suppkey"
  }

  def getQuery15_Original(): String = {
    "select" +
        "        s_suppkey," +
        "        s_name," +
        "        s_address," +
        "        s_phone," +
        "        total_revenue" +
        " from" +
        "        SUPPLIER," +
        "        revenue" +
        " where" +
        "        s_suppkey = supplier_no" +
        "        and total_revenue = (" +
        "                select" +
        "                        max(total_revenue)" +
        "                from" +
        "                        revenue" +
        "        )" +
        " order by" +
        "        s_suppkey;"
  }


  def getTempQuery15_1: String = {
    "select" +
        "         l_suppkey as supplier_no," +
        "         sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
        " from" +
        "         LINEITEM" +
        " where" +
        "         l_shipdate >= '1996-01-01'" +
        "         and l_shipdate < add_months('1996-01-01',3) " +
        " group by" +
        "         l_suppkey"
  }

  def getQuery15: String = {
    "select       " +
        "         s_suppkey," +
        "         s_name," +
        "         s_address," +
        "         s_phone," +
        "         total_revenue" +
        " from" +
        "         SUPPLIER, revenue" +
        " where" +
        "         s_suppkey = supplier_no" +
        "         and total_revenue = (" +
        "                       select" +
        "                             max(total_revenue)" +
        "                       from" +
        "                             revenue" +
        "   ) " +
        " order by" +
        "        s_suppkey"
  }

  def getResultString15: String = {
    "S_SUPPKEY|S_NAME|S_ADDRESS|S_PHONE|TOTAL_REVENUE"
  }

  def getQuery16: String = {
    //    1. BRAND = Brand#45.
    //    2. TYPE = MEDIUM POLISHED .
    //    3. SIZE1 = 49
    //    4. SIZE2 = 14
    //    5. SIZE3 = 23
    //    6. SIZE4 = 45
    //    7. SIZE5 = 19
    //    8. SIZE6 = 3
    //    9. SIZE7 = 36
    //    10. SIZE8 = 9.
    "select" +
      "         p_brand," +
      "         p_type," +
      "         p_size," +
      "         count(distinct ps_suppkey) as supplier_cnt" +
      " from" +
      "         PARTSUPP," +
      "         PART" +
      " where" +
      "         p_partkey = ps_partkey" +
      "         and p_brand <> 'Brand#45'" +
      "         and p_type not like 'MEDIUM POLISHED%'" +
      "         and p_size in (49, 14, 23, 45, 19, 3, 36, 9)" +
      "         and not exists (" +
      "                 select" +
      "                         s_suppkey" +
      "                 from" +
      "                         SUPPLIER" +
      "                 where" +
      "                         s_suppkey = ps_suppkey and" +
      "                         s_comment like '%Customer%Complaints%'" +
      "         )" +
      " group by" +
      "         p_brand," +
      "         p_type," +
      "         p_size" +
      " order by" +
      "         supplier_cnt desc," +
      "         p_brand," +
      "         p_type," +
      "         p_size"
  }

  def getQuery16_Original: String = {
    //    1. BRAND = Brand#45.
    //    2. TYPE = MEDIUM POLISHED .
    //    3. SIZE1 = 49
    //    4. SIZE2 = 14
    //    5. SIZE3 = 23
    //    6. SIZE4 = 45
    //    7. SIZE5 = 19
    //    8. SIZE6 = 3
    //    9. SIZE7 = 36
    //    10. SIZE8 = 9.
    "select" +
        "         p_brand," +
        "         p_type," +
        "         p_size," +
        "         count(distinct ps_suppkey) as supplier_cnt" +
        " from" +
        "         PARTSUPP," +
        "         PART" +
        " where" +
        "         p_partkey = ps_partkey" +
        "         and p_brand <> 'Brand#45'" +
        "         and p_type not like 'MEDIUM POLISHED%'" +
        "         and p_size in (49, 14, 23, 45, 19, 3, 36, 9)" +
        "         and not exists (" +
        "                 select" +
        "                         s_suppkey" +
        "                 from" +
        "                         SUPPLIER" +
        "                 where" +
        "                         s_suppkey = ps_suppkey" +
        "                         and s_comment like '%Customer%Complaints%'" +
        "         )" +
        " group by" +
        "         p_brand," +
        "         p_type," +
        "         p_size" +
        " order by" +
        "         supplier_cnt desc," +
        "         p_brand," +
        "         p_type," +
        "         p_size"
  }

  def getResultString16: String = {
    "P_BRAND|P_TYPE|P_SIZE|SUPPLIER_CNT"
  }

  def getTempQuery17(useIndex: Boolean): String = {
    if (!useIndex) {
      "select" +
          "        l_partkey as v_partkey, " +
          "        0.2 * avg(l_quantity) as v_quantity" +
          " from" +
          "        LINEITEM" +
          " group by" +
          "       l_partkey"
    } else {
      "select" +
          "        l_partkey as v_partkey, " +
          "        0.2 * avg(l_quantity) as v_quantity" +
          " from" +
          "        LINEITEM_PART" +
          " group by" +
          "       l_partkey"
    }
  }

  def getQuery17(useIndex: Boolean): String = {
    //    1. BRAND = Brand#23;
    //    2. CONTAINER = MED BOX.
      if (!useIndex) {
        "select" +
            "         sum(l_extendedprice) / 7.0 as avg_yearly" +
            " from" +
            "         LINEITEM," +
            "         PART," +
            "         ViewQ17" +
            " where" +
            "         p_partkey = l_partkey" +
            "         and p_brand = 'Brand#23'" +
            // "         and p_container = 'MED BOX'" +
            "         and p_container = 'SM PACK'" +
            "         and l_quantity < v_quantity" +
            "         and v_partkey = p_partkey"
      } else {
        "select" +
            "         sum(l_extendedprice) / 7.0 as avg_yearly" +
            " from" +
            "         LINEITEM_PART," +
            "         PART," +
            "         ViewQ17" +
            " where" +
            "         p_partkey = l_partkey" +
            "         and p_brand = 'Brand#23'" +
            // "         and p_container = 'MED BOX'" +
            "         and p_container = 'SM PACK'" +
            "         and l_quantity < v_quantity" +
            "         and v_partkey = p_partkey"
      }
  }

  def getQuery17_Original: String = {
    //    1. BRAND = Brand#23;
    //    2. CONTAINER = MED BOX.
    "select" +
      "         sum(l_extendedprice) / 7.0 as avg_yearly" +
      " from" +
      "         LINEITEM," +
      "         PART" +
      " where" +
      "         P_PARTKEY = l_partkey" +
      "         and P_BRAND = 'Brand#23'" +
      "         and P_CONTAINER = 'SM PACK'" +
      "         and l_quantity < (" +
      "                 select" +
      "                         0.2 * avg(l_quantity)" +
      "                 from" +
      "                         LINEITEM" +
      "                 where" +
      "                         l_partkey = P_PARTKEY" +
      "         )"
  }

  def getResultString17: String = {
    "AVG_YEARLY"
  }

  def getQuery18: String = {
    // 1.QUANTITY = 300

    "    select" +
        "    C_NAME," +
        "    C_CUSTKEY," +
        "    o_orderkey," +
        "    o_orderdate," +
        "    o_totalprice," +
        "    sum(l_quantity)" +
        "    from" +
        "    LINEITEM," +
        "    (" +
        "        select" +
        "            l_orderkey as o" +
        "            from" +
        "            LINEITEM" +
        "            group by" +
        "            l_orderkey having sum(l_quantity) > 300" +
        "        ) as temp," +
        "    ORDERS," +
        "    CUSTOMER" +
        "    where" +
        "    l_orderkey = temp.o" +
        "    and o_orderkey = l_orderkey" +
        "    and C_CUSTKEY = o_custkey" +
        "    group by" +
        "        C_NAME," +
        "    C_CUSTKEY," +
        "    o_orderkey," +
        "    o_orderdate," +
        "    o_totalprice" +
        "    order by" +
        "        o_totalprice desc," +
        "    o_orderdate limit 100"
  }

  def getResultString18: String = {
    "C_NAME|C_CUSTKEY|O_ORDERKEY|O_ORDERDATE|O_TOTALPRICE|Sum(L_QUANTITY)"
  }

  def getQuery19(useIndex: Boolean): String = {
    //    1. QUANTITY1 = 1.
    //    2. QUANTITY2 = 10.
    //    3. QUANTITY3 = 20.
    //    4. BRAND1 = Brand#12.
    //    5. BRAND2 = Brand#23.
    //    6. BRAND3 = Brand#34.
    if (!useIndex) {
      "select" +
          "         sum(l_extendedprice * (1 - l_discount) ) as revenue" +
          " from" +
          "         LINEITEM," +
          "         PART" +
          " where" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#12'" +
          "                 and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')" +
          "                 and l_quantity >= 1 and l_quantity <= 1 + 10" +
          "                 and p_size between 1 and 5" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#23'" +
          "                 and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')" +
          "                 and l_quantity >= 10 and l_quantity <= 10 + 10" +
          "                 and p_size between 1 and 10" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#34'" +
          "                 and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')" +
          "                 and l_quantity >= 20 and l_quantity <= 20 + 10" +
          "                 and p_size between 1 and 15" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )"
    } else {
      "select" +
          "         sum(l_extendedprice * (1 - l_discount) ) as revenue" +
          " from" +
          "         LINEITEM_PART," +
          "         PART" +
          " where" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#12'" +
          "                 and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')" +
          "                 and l_quantity >= 1 and l_quantity <= 1 + 10" +
          "                 and p_size between 1 and 5" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#23'" +
          "                 and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')" +
          "                 and l_quantity >= 10 and l_quantity <= 10 + 10" +
          "                 and p_size between 1 and 10" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = 'Brand#34'" +
          "                 and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')" +
          "                 and l_quantity >= 20 and l_quantity <= 20 + 10" +
          "                 and p_size between 1 and 15" +
          "                 and l_shipmode in ('AIR', 'AIR REG')" +
          "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
          "         )"
    }
    // }
  }

  def getResultString19: String = {
    "REVENUE"
  }

  def getTempQuery20(useIndex: Boolean): String = {
    if (!useIndex) {
      "select" +
          "               0.5 * sum(l_quantity) as v_quantity," +
          "               l_partkey as v_partkey," +
          "               l_suppkey as v_suppkey" +
          " from" +
          "               LINEITEM" +
          " where" +
          "               l_shipdate >= '1994-01-01'" +
          "               and l_shipdate < add_months('1994-01-01', 12)" +
          " group by" +
          "               l_partkey, l_suppkey"
    } else {
      "select" +
          "               0.5 * sum(l_quantity) as v_quantity," +
          "               l_partkey as v_partkey," +
          "               l_suppkey as v_suppkey" +
          " from" +
          "               LINEITEM_PART" +
          " where" +
          "               l_shipdate >= '1994-01-01'" +
          "               and l_shipdate < add_months('1994-01-01', 12)" +
          " group by" +
          "               l_partkey, l_suppkey"
    }
  }


/*  def getQuery20(): String = {
    //    1. COLOR = forest.
    //    2. DATE = 1994-01-01.
    //    3. NATION = CANADA.
      "select" +
          "         s_name," +
          "         s_address" +
          " from" +
          "         SUPPLIER, NATION" +
          " where" +
          "         s_suppkey in (" +
          "                 select" +
          "                         ps_suppkey" +
          "                 from" +
          "                         PARTSUPP, ViewQ20" +
          "                 where" +
          "                         ps_partkey in (" +
          "                                 select" +
          "                                         p_partkey" +
          "                                 from" +
          "                                         PART" +
          "                                 where" +
          "                                         p_name like 'khaki%'" +
          "                         )" +
          "                         and ps_availqty > v_quantity" +
          "                         and v_partkey = ps_partkey" +
          "                         and v_suppkey = ps_suppkey" +
          "         )" +
          "         and s_nationkey = n_nationkey" +
          "         and n_name = 'CANADA'" +
          " order by" +
          "         s_name"
  }
  */

  def getQuery20_Original: String = {
    //    1. COLOR = forest.
    //    2. DATE = 1994-01-01.
    //    3. NATION = CANADA.
    "select" +
      "         S_NAME," +
      "         S_ADDRESS" +
      " from" +
      "         SUPPLIER, NATION" +
      " where" +
      "         S_SUPPKEY in (" +
      "                 select" +
      "                         PS_SUPPKEY" +
      "                 from" +
      "                         PARTSUPP" +
      "                 where" +
      "                         PS_PARTKEY in (" +
      "                                 select" +
      "                                         P_PARTKEY" +
      "                                 from" +
      "                                         PART" +
      "                                 where" +
      "                                         P_NAME like 'khaki%'" +
      "                         )" +
      "                         and PS_AVAILQTY > (" +
      "                                 select" +
      "                                         0.5 * sum(l_quantity)" +
      "                                 from" +
      "                                         LINEITEM" +
      "                                 where" +
      "                                         l_partkey = PS_PARTKEY" +
      "                                         and l_suppkey = PS_SUPPKEY" +
      "                                         and l_shipdate >= '1994-01-01'" +
      "                                         and l_shipdate < add_months('1994-01-01', 12)" +
      "                         )" +
      "         )" +
      "         and S_NATIONKEY = N_NATIONKEY" +
      "         and N_NAME = 'CANADA'" +
      " order by" +
      "         S_NAME"
  }

  def getResultString20: String = {
    "S_NAME|S_ADDRESS"
  }

  def getQuery21: String = {
    // NATION = SAUDI ARABIA.
    "select" +
        "         s_name," +
        "         count(*) as numwait" +
        " from" +
        "         SUPPLIER," +
        "         LINEITEM l1," +
        "         ORDERS," +
        "         NATION" +
        " where" +
        "         s_suppkey = l1.l_suppkey" +
        "         and o_orderkey = l1.l_orderkey" +
        "         and o_orderstatus = 'F'" +
        "         and l1.l_receiptdate > l1.l_commitdate" +
        "         and not exists (" +
        "                 select" +
        "                         l3.l_orderkey" +
        "                 from" +
        "                         LINEITEM l3" +
        "                 where" +
        "                         l3.l_orderkey = l1.l_orderkey" +
        "                         and l3.l_suppkey <> l1.l_suppkey" +
        "                         and l3.l_receiptdate > l3.l_commitdate" +
        "         )" +
        "         and exists (" +
        "                 select" +
        "                         l2.l_orderkey" +
        "                 from" +
        "                         LINEITEM l2" +
        "                 where" +
        "                         l2.l_orderkey = l1.l_orderkey" +
        "                         and l2.l_suppkey <> l1.l_suppkey" +
        "         )" +
        "         and s_nationkey = n_nationkey" +
        "         and n_name = 'VIETNAM'" +
        " group by" +
        "         s_name" +
        " order by" +
        "         numwait desc," +
        "         s_name limit 100"
  }

  def getResultString21: String = {
    "S_NAME|NUMWAIT"
  }

  def getTempQuery22(useIndex: Boolean): String = {
    "select" +
        "         avg(C_ACCTBAL)" +
        " from" +
        "         CUSTOMER" +
        " where" +
        "         C_ACCTBAL > 0.00" +
        "         and SUBSTR (C_PHONE,1,2) in" +
        "         ('13','31','23','29','30','18','17')"

  }

  def getQuery22(value: String, useIndex: Boolean): String = {
    if (!useIndex) {
      "select" +
          "         cntrycode," +
          "         count(*) as numcust," +
          "         sum(C_ACCTBAL) as totacctbal" +
          " from (" +
          "         select" +
          "                 SUBSTR(C_PHONE,1,2) as cntrycode," +
          "                 C_ACCTBAL" +
          "         from" +
          "                 CUSTOMER  left outer join  ORDERS  on  o_custkey = C_CUSTKEY  " +
          "         where" +
          "                 SUBSTR(C_PHONE,1,2) IN" +
          "                         ('13','31','23','29','30','18','17')" +
          "                 and C_ACCTBAL > " +
          "                 " + value +
          "                 and o_orderkey IS NULL " +
          "         ) as custsale" +
          " group by" +
          "         cntrycode" +
          " order by" +
          "         cntrycode"
    } else {
      "select" +
          "         cntrycode," +
          "         count(*) as numcust," +
          "         sum(C_ACCTBAL) as totacctbal" +
          " from (" +
          "         select" +
          "                 SUBSTR(C_PHONE,1,2) as cntrycode," +
          "                 C_ACCTBAL" +
          "         from" +
          "                 CUSTOMER  left outer join  ORDERS_CUST  on  o_custkey = C_CUSTKEY  " +
          "         where" +
          "                 SUBSTR(C_PHONE,1,2) IN" +
          "                         ('13','31','23','29','30','18','17')" +
          "                 and C_ACCTBAL > " +
          "                 " + value +
          "                 and o_orderkey IS NULL " +
          "         ) as custsale" +
          " group by" +
          "         cntrycode" +
          " order by" +
          "         cntrycode"
    }
  }

  def getQuery22_Original: String = {
    //    1. I1 = 13.
    //    2. I2 = 31.
    //    3. I3 = 23.
    //    4. I4 = 29.
    //    5. I5 = 30.
    //    6. I6 = 18.
    //    7. I7 = 17.
    "select" +
      "         cntrycode," +
      "         count(*) as numcust," +
      "         sum(C_ACCTBAL) as totacctbal" +
      " from (" +
      "         select" +
      "                 SUBSTR(C_PHONE,1,2) as cntrycode," +
      "                 C_ACCTBAL" +
      "         from" +
      "                 CUSTOMER        " +
      "         where" +
      "                 SUBSTR(C_PHONE,1,2) in" +
      "                         ('13','31','23','29','30','18','17')" +
      "                 and C_ACCTBAL > (" +
      "                         select" +
      "                                 avg(C_ACCTBAL)" +
      "                         from" +
      "                                 CUSTOMER" +
      "                         where" +
      "                                 C_ACCTBAL > 0.00" +
      "                                 and SUBSTR(C_PHONE,1,2) in" +
      "                                         ('13','31','23','29','30','18','17')" +
      "                 )" +
      "                 and not exists (" +
      "                         select" +
      "                                 o_custkey" +
      "                         from" +
      "                                 ORDERS" +
      "                         where" +
      "                                 o_custkey = C_CUSTKEY" +
      "                 )" +
      "         ) as custsale" +
      " group by" +
      "         cntrycode" +
      " order by" +
      "         cntrycode"
  }


  def getResultString22(): String = {
    "CNTRYCODE|NUMCUST|TOTACCTBAL"
  }

}
