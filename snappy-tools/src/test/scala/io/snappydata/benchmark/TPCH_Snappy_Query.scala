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
package io.snappydata.benchmark

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext

object TPCH_Snappy_Query {

  var fOutStream = new FileOutputStream(new File(s"Performance.out"))
  var ps = new PrintStream(fOutStream)

  def close(): Unit ={
    ps.close()
    fOutStream.close()
  }
  def execute(queryNumber: String, sc: SparkContext, isResultCollection: Boolean, isSnappy:Boolean): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)

    if (isResultCollection) {
      fOutStream = new FileOutputStream(new File(s"$queryNumber.out"))
      ps = new PrintStream(fOutStream)
    }

    val resultFormat = queryNumber match {
      case "q" => getResultString()
      case "q1" => getResultString1()
      case "q2" => getResultString2()
      case "q3" => getResultString3()
      case "q4" => getResultString4()
      case "q5" => getResultString5()
      case "q6" => getResultString6()
      case "q7" => getResultString7()
      case "q8" => getResultString8()
      case "q9" => getResultString9()
      case "q10" => getResultString10()
      case "q11" => getResultString11()
      case "q12" => getResultString12()
      case "q13" => getResultString13()
      case "q14" => getResultString14()
      case "q15" => getResultString15()
      case "q16" => getResultString16()
      case "q17" => getResultString17()
      case "q18" => getResultString18()
      case "q19" => getResultString19()
      case "q20" => getResultString20()
      case "q21" => getResultString21()
      case "q22" => getResultString22()
      case "q1s" => getResultString1s()
      case "q3s" => getResultString3()
      case "q5s" => getResultString5s()
      case "q6s" => getResultString6()
      case "q10s" => getResultString10()
    }

    try {
      println(s"Started executing $queryNumber")
      if (isResultCollection) {
        val cnts = queryExecution(queryNumber, sc, isSnappy)
        //val cnts = snappyContext.sql(query).collect()
        println(s"$queryNumber : ${cnts.length}")
        ps.println(s"$resultFormat")
        for (s <- cnts) {
          var output = s.toString()
          output = output.replace("[", "").replace("]", "").replace(",", "|")
          ps.println(output)
        }
        println(s"$queryNumber Result Collected in file $queryNumber.out")
      } else {
        var totalTimeForLast5Iterations: Long = 0
        ps.println(queryNumber)
        for (i <- 1 to 3) {
          val startTime = System.currentTimeMillis()
          val cnts = queryExecution(queryNumber, sc, isSnappy)
          for (s <- cnts) {
            //just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime
          ps.println(s"$i,$iterationTime")
          if (i > 1) {
            totalTimeForLast5Iterations += iterationTime
          }

        }
        ps.println(s"Average time taken for last 2 iterations,${totalTimeForLast5Iterations / 2}")
      }
      println(s"Finished executing $queryNumber")
    } catch {
      case e: Exception => {
        e.printStackTrace(ps)
        println(s" Exception while executing $queryNumber in written to file $queryNumber.out")
      }
    } finally {
      if(isResultCollection) {
        ps.close()
        fOutStream.close()
      }
    }
  }

  def queryExecution(queryNumber:String, sc:SparkContext, isSnappy:Boolean): scala.Array[org.apache.spark.sql.Row] ={
    val snappyContext  = SnappyContext.getOrCreate(sc)

    val cnts : scala.Array[org.apache.spark.sql.Row] = queryNumber match {
      case "q1s" => {
        val df = snappyContext.sql(getSampledQuery1())
        df.collect()
      }

      case "q3s" => {
        val df = snappyContext.sql(getSampledQuery3())
        val cnt = df.collect()
        cnt
      }

      case "q5s" => {
        snappyContext.sql(getSampledQuery5()).collect()
      }

      case "q6s" => {
        snappyContext.sql(getSampledQuery6()).collect()
      }

      case "q10s" => {
        snappyContext.sql(getSampledQuery10()).collect()
      }
      case "q1" => {
        snappyContext.sql(getQuery1()).collect()
      }
      case "q2" => {
        val result = snappyContext.sql(getTempQuery2(isSnappy))
        result.registerTempTable("ViewQ2")
        snappyContext.sql(getQuery2(isSnappy)).collect()
      }
      case "q3" => {
        snappyContext.sql(getQuery3()).collect()
      }
      case "q4" => {
        snappyContext.sql(getQuery4()).collect()
      }
      case "q5" => {
        snappyContext.sql(getQuery5(isSnappy)).collect()
      }
      case "q6" => {
        snappyContext.sql(getQuery6()).collect()
      }
      case "q7" => {
        snappyContext.sql(getQuery7(isSnappy)).collect()
      }
      case "q8" => {
        snappyContext.sql(getQuery8(isSnappy)).collect()
      }
      case "q9" => {
        snappyContext.sql(getQuery9(isSnappy)).collect()
      }
      case "q10" => {
        snappyContext.sql(getQuery10(isSnappy)).collect()
      }
      case "q11" => {
        val result = snappyContext.sql(getTempQuery11(isSnappy))
        val res = result.collect()
        assert(res.length == 1)
        if(isSnappy) {
          snappyContext.sql(getQuery11(res(0).getDecimal(0), isSnappy)).collect()
        }else{
          snappyContext.sql(getQuery11(BigDecimal.apply(res(0).getDouble(0)), isSnappy)).collect()
        }
      }
      case "q12" => {
        snappyContext.sql(getQuery12()).collect()
      }
      case "q13" => {
        val result = snappyContext.sql(getTempQuery13())
        result.registerTempTable("ViewQ13")
        snappyContext.sql(getQuery13()).collect()
      }
      case "q14" => {
        snappyContext.sql(getQuery14(isSnappy)).collect()
      }
      case "q15" => {
        var result = snappyContext.sql(getTempQuery15_1())
        result.registerTempTable("revenue")

        result = snappyContext.sql(getTempQuery15_2())
        result.registerTempTable("ViewQ15")

        snappyContext.sql(getQuery15(isSnappy)).collect()
      }
      case "q16" => {
        snappyContext.sql(getQuery16(isSnappy)).collect()
      }
      case "q17" => {
        val result = snappyContext.sql(getTempQuery17())
        result.registerTempTable("ViewQ17")

        snappyContext.sql(getQuery17(isSnappy)).collect()
      }
      case "q18" => {
        snappyContext.sql(getQuery18()).collect()
      }
      case "q19" => {
        snappyContext.sql(getQuery19(isSnappy)).collect()
      }
      case "q20" => {
        val result = snappyContext.sql(getTempQuery20())
        result.registerTempTable("ViewQ20")
        snappyContext.sql(getQuery20(isSnappy)).collect()
      }
      case "q21" => {
        snappyContext.sql(getQuery21(isSnappy)).collect()
      }
      case "q22" => {
        val result = snappyContext.sql(getTempQuery22())
        val res = result.collect()
        assert(res.length == 1)
        if(isSnappy) {
          snappyContext.sql(getQuery22(res(0).getDecimal(0).toString)).collect()
        }else{
          snappyContext.sql(getQuery22(res(0).getDouble(0).toString)).collect()
        }
      }
    }
    cnts
  }


  def queryPlan(sc: SparkContext, isSnappy: Boolean): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)

    println(s"q1 plan : ${snappyContext.sql(getQuery1()).explain()}")

    println(s"Temp q2 plan : ${snappyContext.sql(getTempQuery2(isSnappy)).explain()}")
    var result = snappyContext.sql(getTempQuery2(isSnappy))
    result.registerTempTable("ViewQ2")
    println(s"q2 plan : ${snappyContext.sql(getQuery2(isSnappy)).explain()}")

    println(s"q3 plan : ${snappyContext.sql(getQuery3()).explain()}")

    println(s"q4 plan : ${snappyContext.sql(getQuery4()).explain()}")

    println(s"q5 plan : ${snappyContext.sql(getQuery5(isSnappy)).explain()}")

    println(s"q6 plan : ${snappyContext.sql(getQuery6()).explain()}")

    println(s"q7 plan : ${snappyContext.sql(getQuery7(isSnappy)).explain()}")

    println(s"q8 plan : ${snappyContext.sql(getQuery8(isSnappy)).explain()}")

    println(s"q9 plan : ${snappyContext.sql(getQuery9(isSnappy)).explain()}")

    println(s"q10 plan : ${snappyContext.sql(getQuery10(isSnappy)).explain()}")

    println(s"Temp q11 plan : ${snappyContext.sql(getTempQuery11(isSnappy)).explain()}")
     result = snappyContext.sql(getTempQuery11(isSnappy))
     var res = result.collect()
    assert(res.length == 1)
    if (isSnappy) {
      println(s"q11 plan : ${snappyContext.sql(getQuery11(res(0).getDecimal(0).toString, isSnappy)).explain()}")
    } else {
      println(s"q11 plan : ${snappyContext.sql(getQuery11(res(0).getDouble(0).toString, isSnappy)).explain()}")
    }

    println(s"q12 plan : ${snappyContext.sql(getQuery12()).explain()}")

    println(s"Temp q13 plan : ${snappyContext.sql(getTempQuery13()).explain()}")
    result = snappyContext.sql(getTempQuery13())
    result.registerTempTable("ViewQ13")
    println(s"q13 plan : ${snappyContext.sql(getQuery13()).explain()}")

    println(s"q14 plan : ${snappyContext.sql(getQuery14(isSnappy)).explain()}")

    println(s"Temp1 q15 plan : ${snappyContext.sql(getTempQuery15_1()).explain()}")
    result = snappyContext.sql(getTempQuery15_1())
    result.registerTempTable("revenue")
    println(s"Temp2 q15 plan : ${snappyContext.sql(getTempQuery15_2()).explain()}")
    result = snappyContext.sql(getTempQuery15_2())
    result.registerTempTable("ViewQ15")
    println(s"q15 plan : ${snappyContext.sql(getQuery15(isSnappy)).explain()}")


    println(s"q16 plan : ${snappyContext.sql(getQuery16(isSnappy)).explain()}")

    println(s"Temp q17 plan : ${snappyContext.sql(getTempQuery17()).explain()}")
    result = snappyContext.sql(getTempQuery17())
    result.registerTempTable("ViewQ17")
    println(s"q17 plan : ${snappyContext.sql(getQuery17(isSnappy)).explain()}")

    println(s"q18 plan : ${snappyContext.sql(getQuery18()).explain()}")

    //println(s"q19 plan : ${snappyContext.sql(getQuery19(isSnappy)).explain()}")

    println(s"Temp q20 plan : ${snappyContext.sql(getTempQuery20()).explain()}")
    result = snappyContext.sql(getTempQuery20())
    result.registerTempTable("ViewQ20")
    println(s"q20 plan : ${snappyContext.sql(getQuery20(isSnappy)).explain()}")

    //println(s"q21 plan : ${snappyContext.sql(getQuery21(isSnappy)).explain()}")

    println(s"Temp q22 plan : ${snappyContext.sql(getTempQuery22()).explain()}")
    result = snappyContext.sql(getTempQuery22())
    res = result.collect()
    assert(res.length == 1)
    if (isSnappy) {
      println(s"q22 plan : ${snappyContext.sql(getQuery22(res(0).getDecimal(0).toString)).explain()}")
      snappyContext.sql(getQuery22(res(0).getDecimal(0).toString)).collect()
    } else {
      println(s"q22 plan : ${snappyContext.sql(getQuery22(res(0).getDouble(0).toString)).explain()}")
      snappyContext.sql(getQuery22(res(0).getDouble(0).toString)).collect()
    }
  }


  def getQuery(): String = {
    //"select N_NAME from NATION where N_REGIONKEY = (select R_REGIONKEY from REGION where R_NAME='ASIA')"
     "select count(*) from LINEITEM"

  }

  def getResultString1s():String = {
    "sl_returnflag|l_linestatus|sum_qty|sum_qty_err|sum_base_price|sum_base_price_err|sum_disc_price|sum_disc_price_err|sum_charge|sum_charge_err|" +
        "avg_qty|avg_qty_err|avg_price|avg_price_err|avg_disc|avg_disc_err|count_order"
  }

  def getResultString5s():String = {
    "N_NAME|revenue|revenue_err"
  }

  def getResultString():String = {
    ""
  }

  def getSampledQuery1(): String = {
    //DELTA = 90
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
  }

  def getSampledQuery3(): String = {
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
        "     upper(trim(C_MKTSEGMENT)) = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "     and o_orderdate < '1995-03-15'" +
        "     and l_shipdate > '1995-03-15' " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate"
  }

  def getSampledQuery3_1(): String = {
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
        "     upper(trim(C_MKTSEGMENT)) = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "     and o_orderdate < '1995-03-15'" +
        "     and l_shipdate > '1995-03-15' " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate"
  }

  def getSampledQuery5(): String = {
    //1. REGION = ASIA;
    //2. DATE = 1994-01-01.
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
        "        and trim(upper(R_NAME)) = 'ASIA'" +
        "        and o_orderdate >= '1994-01-01'" +
        //"        and o_orderdate < date '[DATE]' + interval '1' year" +
        "        and o_orderdate < add_months('1994-01-01', 12)" +
        " group by" +
        "        N_NAME" +
        " order by" +
        "        revenue desc"
  }

  def getSampledQuery6(): String = {
    //1. DATE = 1994-01-01;
    //2. DISCOUNT = 0.06;
    //3. QUANTITY = 24.
    " select" +
        "        sum(l_extendedprice*l_discount) as revenue," +
        "        error estimate sum(l_extendedprice*l_discount) as revenue_err" +
        " from" +
        "        lineitem_sampled" +
        " where" +
        "        l_shipdate >= '1994-01-01'" +
        "        and l_shipdate < add_months('1994-01-01', 12)" +
        "        and l_discount between 0.06 - 0.01 and 0.06 + 0.01" +
        "        and l_quantity < 24"
  }

  def getSampledQuery10(): String = {
    //1.    DATE = 1993-10-01.
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
        "         and o_orderdate >= '1993-10-01'" +
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

  def getSampledQuery10_1(): String = {
    //1.    DATE = 1993-10-01.
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
        "         and o_orderdate >= '1993-10-01'" +
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

  def getQuery1(): String = {
    //DELTA = 90
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
        "     l_shipdate <= DATE_SUB('1998-12-01',90)" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"
  }

  def getResultString1():String = {
    "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order"
  }

  def getTempQuery2(isSnappy:Boolean):String = {
    if(isSnappy) {
      " select" +
          "     min(PS_SUPPLYCOST) as V_SUPPLYCOST, PS_PARTKEY as V_PARTKEY" +
          " from" +
          "     NATION, REGION," +
          "     PARTSUPP, SUPPLIER" +
          " where" +
          "     S_SUPPKEY = PS_SUPPKEY" +
          "     and S_NATIONKEY = N_NATIONKEY" +
          "     and N_REGIONKEY = R_REGIONKEY" +
          "     and upper(trim(R_NAME)) = 'EUROPE'" +
          " group by" +
          "     PS_PARTKEY"
    }else{
      " select" +
          "     min(ps_supplycost) as v_supplycost, ps_partkey as v_partkey" +
          " from" +
          "     NATION, REGION," +
          "     PARTSUPP, SUPPLIER" +
          " where" +
          "     s_suppkey = ps_suppkey" +
          "     and s_nationkey = n_nationkey" +
          "     and n_regionkey = r_regionkey" +
          "     and upper(trim(r_name)) = 'EUROPE'" +
          " group by" +
          "     ps_partkey"
    }
  }

  def getQuery2(isSnappy:Boolean): String = {
    //    1. SIZE = 15;
    //    2. TYPE = BRASS;
    //    3. REGION = EUROPE
    if(isSnappy) {
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
          "     PART," +
          "     PARTSUPP," +
          "     NATION," +
          "     REGION," +
          "     SUPPLIER," +
          "     ViewQ2" +
          " where" +
          "     P_PARTKEY = PS_PARTKEY" +
          "     and S_SUPPKEY = PS_SUPPKEY" +
          "     and P_SIZE = 15" +
          "     and P_TYPE like '%BRASS'" +
          "     and S_NATIONKEY = N_NATIONKEY" +
          "     and N_REGIONKEY = R_REGIONKEY" +
          "     and trim(upper(R_NAME)) = 'EUROPE'" +
          "     and P_PARTKEY = V_PARTKEY" +
          "     and PS_SUPPLYCOST =  V_SUPPLYCOST" +
          " order by" +
          "     S_ACCTBAL desc," +
          "     N_NAME," +
          "     S_NAME," +
          "     P_PARTKEY"
    }else{
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
          "     NATION," +
          "     REGION," +
          "     SUPPLIER," +
          "     ViewQ2" +
          " where" +
          "     p_partkey = ps_partkey" +
          "     and s_suppkey = ps_suppkey" +
          "     and p_size = 15" +
          "     and p_type like '%BRASS'" +
          "     and s_nationkey = n_nationkey" +
          "     and n_regionkey = r_regionkey" +
          "     and trim(upper(r_name)) = 'EUROPE'" +
          "     and p_partkey = v_partkey" +
          "     and ps_supplycost =  v_supplycost" +
          " order by" +
          "     s_acctbal desc," +
          "     n_name," +
          "     s_name," +
          "     p_partkey"
    }
  }
  def getResultString2():String = {
    "S_ACCTBAL|S_NAME|N_NAME|P_PARTKEY|P_MFGR|S_ADDRESS|S_PHONE|S_COMMENT"
  }

  def getQuery3(): String = {
    //    1. SEGMENT = BUILDING;
    //    2. DATE = 1995-03-15.
    " select" +
        "     l_orderkey," +
        "     sum(l_extendedprice*(1-l_discount)) as revenue," +
        "     o_orderdate," +
        "     o_shippriority" +
        " from" +
        "     CUSTOMER," +
        "     ORDERS," +
        "     LINEITEM" +
        " where" +
        "     upper(trim(C_MKTSEGMENT)) = 'BUILDING'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "     and o_orderdate < '1995-03-15'" +
        "     and l_shipdate > '1995-03-15' " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     o_orderdate"
  }

  def getResultString3():String = {
    "l_orderkey|revenue|o_orderdate|o_shippriority"
  }

  def getQuery4(): String = {
    //1.DATE = 1993-07-01.
        " select" +
        "     o_orderpriority," +
        "     count(*) as order_count" +
        " from" +
        "     ORDERS" +
        " where" +
        "     o_orderdate >= '1993-07-01'" +
        //" and o_orderdate < '1993-07-01' + interval '3' month" +
        //"     and o_orderdate < DATE_ADD('1993-07-01',90)" +
        "     and o_orderdate < add_months('1993-07-01',3)" +
        "     and exists (" +
        "         select" +
        "             *" +
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

  def getResultString4():String = {
    "o_orderpriority|order_count"
  }

  def getQuery5(isSnappy:Boolean): String = {
    //1. REGION = ASIA;
    //2. DATE = 1994-01-01.
    if(isSnappy) {
      " select" +
          "        N_NAME," +
          "        sum(l_extendedprice * (1 - l_discount)) as revenue" +
          " from" +
          "        CUSTOMER," +
          "        ORDERS," +
          "        LINEITEM," +
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
          "        and trim(upper(R_NAME)) = 'ASIA'" +
          "        and o_orderdate >= '1994-01-01'" +
          //"        and o_orderdate < date '[DATE]' + interval '1' year" +
          "        and o_orderdate < add_months('1994-01-01', 12)" +
          " group by" +
          "        N_NAME" +
          " order by" +
          "        revenue desc"
    }else{
      " select" +
          "        n_name," +
          "        sum(l_extendedprice * (1 - l_discount)) as revenue" +
          " from" +
          "        CUSTOMER," +
          "        ORDERS," +
          "        LINEITEM," +
          "        SUPPLIER," +
          "        NATION," +
          "        REGION" +
          " where" +
          "        C_CUSTKEY = o_custkey" +
          "        and l_orderkey = o_orderkey" +
          "        and l_suppkey = s_suppkey" +
          "        and C_NATIONKEY = s_nationkey" +
          "        and s_nationkey = n_nationkey" +
          "        and n_regionkey = r_regionkey" +
          "        and trim(upper(r_name)) = 'ASIA'" +
          "        and o_orderdate >= '1994-01-01'" +
          //"        and o_orderdate < date '[DATE]' + interval '1' year" +
          "        and o_orderdate < add_months('1994-01-01', 12)" +
          " group by" +
          "        n_name" +
          " order by" +
          "        revenue desc"
    }
  }

  def getResultString5():String = {
    "N_NAME|revenue"
  }

  def getQuery6(): String = {
    //1. DATE = 1994-01-01;
    //2. DISCOUNT = 0.06;
    //3. QUANTITY = 24.
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

  def getResultString6():String = {
    "revenue"
  }

  def getQuery7(isSnappy:Boolean): String = {
//    1. NATION1 = FRANCE;
//    2. NATION2 = GERMANY.
    if (isSnappy) {
      "select" +
          "         supp_nation," +
          "         cust_nation," +
          "         l_year, " +
          "         sum(volume) as revenue" +
          " from (" +
          "         select" +
          "                 n1.N_NAME as supp_nation," +
          "                 n2.N_NAME as cust_nation," +
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
          "                 S_SUPPKEY = l_suppkey" +
          "                 and o_orderkey = l_orderkey" +
          "                 and C_CUSTKEY = o_custkey" +
          "                 and S_NATIONKEY = n1.N_NATIONKEY" +
          "                 and C_NATIONKEY = n2.N_NATIONKEY" +
          "                 and (" +
          "                         (trim(upper(n1.N_NAME)) = 'FRANCE' and trim(upper(n2.N_NAME)) = 'GERMANY')" +
          "                      or (trim(upper(n1.N_NAME)) = 'GERMANY' and trim(upper(n2.N_NAME)) = 'FRANCE')" +
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
    } else {
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
          "                         (trim(upper(n1.n_name)) = 'FRANCE' and trim(upper(n2.n_name)) = 'GERMANY')" +
          "                      or (trim(upper(n1.n_name)) = 'GERMANY' and trim(upper(n2.n_name)) = 'FRANCE')" +
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
  }

  def getResultString7():String = {
    "supp_nation|cust_nation|l_year|revenue"
  }

  def getQuery8(isSnappy:Boolean): String = {
//    1. NATION = BRAZIL;
//    2. REGION = AMERICA;
//    3. TYPE = ECONOMY ANODIZED STEEL.
    if (isSnappy) {
      "select" +
          "         o_year," +
          "         sum(case" +
          "                 when trim(upper(nation)) = 'BRAZIL'" +
          "                 then volume" +
          "                 else 0" +
          "                 end) / sum(volume) as mkt_share" +
          "         from (" +
          "                 select" +
          "                         year(o_orderdate) as o_year," +
          "                         l_extendedprice * (1-l_discount) as volume," +
          "                         n2.N_NAME as nation" +
          "                 from" +
          "                         PART," +
          "                         SUPPLIER," +
          "                         LINEITEM," +
          "                         ORDERS," +
          "                         CUSTOMER," +
          "                         NATION n1," +
          "                         NATION n2," +
          "                         REGION" +
          "                 where" +
          "                         P_PARTKEY = l_partkey" +
          "                         and S_SUPPKEY = l_suppkey" +
          "                         and l_orderkey = o_orderkey" +
          "                         and o_custkey = C_CUSTKEY" +
          "                         and C_NATIONKEY = n1.N_NATIONKEY" +
          "                         and n1.N_REGIONKEY = R_REGIONKEY" +
          "                         and trim(upper(R_NAME)) = 'AMERICA'" +
          "                         and S_NATIONKEY = n2.N_NATIONKEY" +
          "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
          "                         and trim(upper(P_TYPE)) = 'ECONOMY ANODIZED STEEL'" +
          "         ) as all_nations" +
          " group by" +
          "         o_year" +
          " order by" +
          "         o_year"
    } else {
      "select" +
          "         o_year," +
          "         sum(case" +
          "                 when trim(upper(nation)) = 'BRAZIL'" +
          "                 then volume" +
          "                 else 0" +
          "                 end) / sum(volume) as mkt_share" +
          "         from (" +
          "                 select" +
          "                         year(o_orderdate) as o_year," +
          "                         l_extendedprice * (1-l_discount) as volume," +
          "                         n2.n_name as nation" +
          "                 from" +
          "                         PART," +
          "                         SUPPLIER," +
          "                         LINEITEM," +
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
          "                         and trim(upper(r_name)) = 'AMERICA'" +
          "                         and s_nationkey = n2.n_nationkey" +
          "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
          "                         and trim(upper(p_type)) = 'ECONOMY ANODIZED STEEL'" +
          "         ) as all_nations" +
          " group by" +
          "         o_year" +
          " order by" +
          "         o_year"
    }
  }

  def getResultString8():String = {
    "YEAR|MKT_SHARE"
  }

  def getQuery9(isSnappy:Boolean): String = {
    //1. COLOR = green.
    if(isSnappy) {
      "select" +
          "         nation," +
          "         o_year," +
          "         sum(amount) as sum_profit" +
          " from (" +
          "         select" +
          "                 N_NAME as nation," +
          "                 year(o_orderdate) as o_year," +
          "                 l_extendedprice * (1 - l_discount) - PS_SUPPLYCOST * l_quantity as amount" +
          "         from" +
          "                 PART," +
          "                 SUPPLIER," +
          "                 LINEITEM," +
          "                 PARTSUPP," +
          "                 ORDERS," +
          "                 NATION" +
          "         where" +
          "                 S_SUPPKEY = l_suppkey" +
          "                 and PS_SUPPKEY = l_suppkey" +
          "                 and PS_PARTKEY = l_partkey" +
          "                 and P_PARTKEY = l_partkey" +
          "                 and o_orderkey = l_orderkey" +
          "                 and S_NATIONKEY = N_NATIONKEY" +
          "                 and P_NAME like '%green%'" +
          "         ) as profit" +
          " group by" +
          "         nation," +
          "         o_year" +
          " order by" +
          "         nation," +
          "         o_year desc"
    }else{
      "select" +
          "         nation," +
          "         o_year," +
          "         sum(amount) as sum_profit" +
          " from (" +
          "         select" +
          "                 n_name as nation," +
          "                 year(o_orderdate) as o_year," +
          "                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount" +
          "         from" +
          "                 PART," +
          "                 SUPPLIER," +
          "                 LINEITEM," +
          "                 PARTSUPP," +
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

  def getResultString9():String = {
    "NATION|YEAR|SUM_PROFIT"
  }
  def getQuery10(isSappy:Boolean): String = {
    //1.    DATE = 1993-10-01.
    if(isSappy) {
      "select" +
          "         C_CUSTKEY," +
          "         C_NAME," +
          "         sum(l_extendedprice * (1 - l_discount)) as revenue," +
          "         C_ACCTBAL," +
          "         N_NAME," +
          "         C_ADDRESS," +
          "         C_PHONE," +
          "         C_COMMENT" +
          " from" +
          "         CUSTOMER," +
          "         ORDERS," +
          "         LINEITEM," +
          "         NATION" +
          " where" +
          "         C_CUSTKEY = o_custkey" +
          "         and l_orderkey = o_orderkey" +
          "         and o_orderdate >= '1993-10-01'" +
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
    }else{
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
          "         CUSTOMER," +
          "         ORDERS," +
          "         LINEITEM," +
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
          "         revenue desc"
    }
  }

  def getResultString10():String = {
    "C_CUSTKEY|C_NAME|REVENUE|C_ACCTBAL|N_NAME|C_ADDRESS|C_PHONE|C_COMMENT"
  }

  def getTempQuery11(isSnappy:Boolean):String = {
    if(isSnappy) {
      " select" +
          "         sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001" +
          " from" +
          "         PARTSUPP," +
          "         SUPPLIER," +
          "         NATION" +
          " where" +
          "         PS_SUPPKEY = S_SUPPKEY" +
          "         and S_NATIONKEY = N_NATIONKEY" +
          "         and trim(upper(N_NAME)) = 'GERMANY'"
    }else{
      " select" +
          "         sum(ps_supplycost * ps_availqty) * 0.0001" +
          " from" +
          "         PARTSUPP," +
          "         SUPPLIER," +
          "         NATION" +
          " where" +
          "         ps_suppkey = s_suppkey" +
          "         and s_nationkey = n_nationkey" +
          "         and trim(upper(n_name)) = 'GERMANY'"
    }
  }

  def getQuery11(value : Any, isSnappy:Boolean): String = {
//    1. NATION = GERMANY;
//    2. FRACTION = 0.0001.
    if(isSnappy) {
      " select" +
          "         PS_PARTKEY," +
          "         sum(PS_SUPPLYCOST * PS_AVAILQTY) as value" +
          " from" +
          "         PARTSUPP," +
          "         SUPPLIER," +
          "         NATION" +
          " where" +
          "         PS_SUPPKEY = S_SUPPKEY" +
          "         and S_NATIONKEY = N_NATIONKEY" +
          "         and trim(upper(N_NAME)) = 'GERMANY'" +
          " group by" +
          "         PS_PARTKEY having" +
          "         sum(PS_SUPPLYCOST * PS_AVAILQTY) > " +
          value +
          " order by" +
          "         value desc"
    }else{
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
          "         and trim(upper(n_name)) = 'GERMANY'" +
          " group by" +
          "         ps_partkey having" +
          "         sum(ps_supplycost * ps_availqty) > " +
          value +
          " order by" +
          "         value desc"
    }
  }

  def getResultString11():String = {
    "PS_PARTKEY|VALUE"
  }


  def getQuery12(): String = {
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

  def getResultString12():String = {
    "L_SHIPMODE|HIGH_LINE_COUNT|LOW_LINE_COUNT"
  }

  def getTempQuery13():String={
        "select" +
        "        C_CUSTKEY," +
        "        count(o_orderkey) as c_count" +
        " from" +
        "        CUSTOMER left outer join ORDERS on" +
        "        C_CUSTKEY = o_custkey" +
        "        and o_comment not like '%special%requests%'" +
        " group by" +
        "        C_CUSTKEY"
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

  def getResultString13():String = {
    "C_COUNT|CUSTDIST"
  }
  def getQuery14(isSnappy:Boolean): String = {
    //1.DATE = 1995-09-01.
    if (isSnappy) {
      "select" +
          "         100.00 * sum(case" +
          "                 when P_TYPE like 'PROMO%'" +
          "                 then l_extendedprice*(1-l_discount)" +
          "                 else 0" +
          "                 end" +
          "         ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue" +
          " from" +
          "         LINEITEM," +
          "         PART" +
          " where" +
          "         l_partkey = P_PARTKEY" +
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
          "         LINEITEM," +
          "         PART" +
          " where" +
          "         l_partkey = p_partkey" +
          "         and l_shipdate >= '1995-09-01'" +
          "         and l_shipdate < add_months ('1995-09-01', 1)"
    }
  }

  def getResultString14():String = {
    "PROMO_REVENUE"
  }

  def getTempQuery15_1():String = {
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

  def getTempQuery15_2():String = {
      "select" +
      "          max(total_revenue) as max_revenue" +
      " from" +
      "          revenue"
  }

  def getQuery15(isSNappy:Boolean): String = {
    if (isSNappy) {
      "select       " +
          "         S_SUPPKEY," +
          "         S_NAME," +
          "         S_ADDRESS," +
          "         S_PHONE," +
          "         total_revenue" +
          " from" +
          "         supplier, revenue" +
          " where" +
          "         S_SUPPKEY = supplier_no" +
          "         and total_revenue = (" +
          "                       select" +
          "                             max_revenue" +
          "                       from" +
          "                             ViewQ15" +
          "   ) " +
          " order by" +
          "        S_SUPPKEY"
    } else {
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
          "                             max_revenue" +
          "                       from" +
          "                             ViewQ15" +
          "   ) " +
          " order by" +
          "        s_suppkey"
    }
  }

  def getResultString15():String = {
    "S_SUPPKEY|S_NAME|S_ADDRESS|S_PHONE|TOTAL_REVENUE"
  }

  def getQuery16(isSnappy:Boolean): String = {
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
    if (isSnappy) {
      "select" +
          "         P_BRAND," +
          "         P_TYPE," +
          "         P_SIZE," +
          "         count(distinct PS_SUPPKEY) as supplier_cnt" +
          " from" +
          "         PARTSUPP," +
          "         PART" +
          " where" +
          "         P_PARTKEY = PS_PARTKEY" +
          "         and P_BRAND <> 'Brand#45'" +
          "         and P_TYPE not like 'MEDIUM POLISHED%'" +
          "         and P_SIZE in (49, 14, 23, 45, 19, 3, 36, 9)" +
          "         and PS_SUPPKEY not in (" +
          "                 select" +
          "                         S_SUPPKEY" +
          "                 from" +
          "                         SUPPLIER" +
          "                 where" +
          "                         S_COMMENT like '%Customer%Complaints%'" +
          "         )" +
          " group by" +
          "         P_BRAND," +
          "         P_TYPE," +
          "         P_SIZE" +
          " order by" +
          "         supplier_cnt desc," +
          "         P_BRAND," +
          "         P_TYPE," +
          "         P_SIZE"
    } else {
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
          "         and ps_suppkey not in (" +
          "                 select" +
          "                         s_suppkey" +
          "                 from" +
          "                         SUPPLIER" +
          "                 where" +
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
  }

  def getResultString16():String = {
    "P_BRAND|P_TYPE|P_SIZE|SUPPLIER_CNT"
  }

  def getTempQuery17(): String = {
        "select" +
        "        l_partkey as v_partkey, " +
        "        0.2 * avg(l_quantity) as v_quantity" +
        " from" +
        "        LINEITEM" +
        " group by" +
        "       l_partkey"
  }

  def getQuery17(isSnappy:Boolean): String = {
//    1. BRAND = Brand#23;
//    2. CONTAINER = MED BOX.
    if (isSnappy) {
      "select" +
          "         sum(l_extendedprice) / 7.0 as avg_yearly" +
          " from" +
          "         LINEITEM," +
          "         PART," +
          "         ViewQ17" +
          " where" +
          "         P_PARTKEY = l_partkey" +
          "         and trim(P_BRAND) = 'Brand#23'" +
          "         and trim(upper(P_CONTAINER)) = 'MED BOX'" +
          "         and l_quantity < v_quantity" +
          "         and v_partkey = P_PARTKEY"
    } else {
      "select" +
          "         sum(l_extendedprice) / 7.0 as avg_yearly" +
          " from" +
          "         LINEITEM," +
          "         PART," +
          "         ViewQ17" +
          " where" +
          "         p_partkey = l_partkey" +
          "         and trim(p_brand) = 'Brand#23'" +
          "         and trim(upper(p_container)) = 'MED BOX'" +
          "         and l_quantity < v_quantity" +
          "         and v_partkey = p_partkey"
    }
  }

  def getResultString17():String = {
    "AVG_YEARLY"
  }
  def getQuery18(): String = {
    //1.QUANTITY = 300
    "select" +
        "         C_NAME," +
        "         C_CUSTKEY," +
        "         o_orderkey," +
        "         o_orderdate," +
        "         o_totalprice," +
        "         sum(l_quantity)" +
        " from" +
        "         CUSTOMER," +
        "         ORDERS," +
        "         LINEITEM" +
        " where" +
        "         o_orderkey in (" +
        "                 select" +
        "                         l_orderkey" +
        "                 from" +
        "                         LINEITEM" +
        "                 group by" +
        "                         l_orderkey having sum(l_quantity) > 314" +
        "         )" +
        "         and C_CUSTKEY = o_custkey" +
        "         and o_orderkey = l_orderkey" +
        " group by" +
        "         C_NAME," +
        "         C_CUSTKEY," +
        "         o_orderkey," +
        "         o_orderdate," +
        "         o_totalprice" +
        " order by" +
        "         o_totalprice desc," +
        "         o_orderdate"
  }

  def getResultString18():String = {
    "C_NAME|C_CUSTKEY|O_ORDERKEY|O_ORDERDATE|O_TOTALPRICE|Sum(L_QUANTITY)"
  }
  def getQuery19(isSnappy:Boolean): String = {
//    1. QUANTITY1 = 1.
//    2. QUANTITY2 = 10.
//    3. QUANTITY3 = 20.
//    4. BRAND1 = Brand#12.
//    5. BRAND2 = Brand#23.
//    6. BRAND3 = Brand#34.
    if(isSnappy) {
      "select" +
          "         sum(l_extendedprice * (1 - l_discount) ) as revenue" +
          " from" +
          "         LINEITEM," +
          "         PART" +
          " where" +
          "         (" +
          "                 P_PARTKEY = l_partkey" +
          "                 and P_BRAND = ‘Brand#12’" +
          "                 and P_CONTAINER in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)" +
          "                 and l_quantity >= 1 and l_quantity <= 1 + 10" +
          "                 and P_SIZE between 1 and 5" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )" +
          "         or" +
          "         (" +
          "                 P_PARTKEY = l_partkey" +
          "                 and P_BRAND = ‘Brand#23’" +
          "                 and P_CONTAINER in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)" +
          "                 and l_quantity >= 10 and l_quantity <= 10 + 10" +
          "                 and P_SIZE between 1 and 10" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )" +
          "         or" +
          "         (" +
          "                 P_PARTKEY = l_partkey" +
          "                 and P_BRAND = ‘Brand#34’" +
          "                 and P_CONTAINER in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)" +
          "                 and l_quantity >= 20 and l_quantity <= 20 + 10" +
          "                 and P_SIZE between 1 and 15" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )"
    }else{
      "select" +
          "         sum(l_extendedprice * (1 - l_discount) ) as revenue" +
          " from" +
          "         LINEITEM," +
          "         PART" +
          " where" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = ‘Brand#12’" +
          "                 and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)" +
          "                 and l_quantity >= 1 and l_quantity <= 1 + 10" +
          "                 and p_size between 1 and 5" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = ‘Brand#23’" +
          "                 and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)" +
          "                 and l_quantity >= 10 and l_quantity <= 10 + 10" +
          "                 and p_size between 1 and 10" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )" +
          "         or" +
          "         (" +
          "                 p_partkey = l_partkey" +
          "                 and p_brand = ‘Brand#34’" +
          "                 and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)" +
          "                 and l_quantity >= 20 and l_quantity <= 20 + 10" +
          "                 and p_size between 1 and 15" +
          "                 and l_shipmode in (‘AIR’, ‘AIR REG’)" +
          "                 and l_shipinstruct = ‘DELIVER IN PERSON’" +
          "         )"
    }
  }

  def getResultString19():String = {
    "REVENUE"
  }

  def getTempQuery20(): String={
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
  }

  def getQuery20(isSnappy:Boolean): String = {
//    1. COLOR = forest.
//    2. DATE = 1994-01-01.
//    3. NATION = CANADA.
    if (isSnappy) {
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
          "                         PARTSUPP, ViewQ20" +
          "                 where" +
          "                         PS_PARTKEY in (" +
          "                                 select" +
          "                                         P_PARTKEY" +
          "                                 from" +
          "                                         PART" +
          "                                 where" +
          "                                         P_NAME like 'forest%'" +
          "                         )" +
          "                         and PS_AVAILQTY > v_quantity" +
          "                         and v_partkey = PS_PARTKEY" +
          "                         and v_suppkey = PS_SUPPKEY" +
          "         )" +
          "         and S_NATIONKEY = N_NATIONKEY" +
          "         and trim(upper(N_NAME)) = 'CANADA'" +
          " order by" +
          "         S_NAME"
    } else {
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
          "                                         p_name like 'forest%'" +
          "                         )" +
          "                         and ps_availqty > v_quantity" +
          "                         and v_partkey = ps_partkey" +
          "                         and v_suppkey = ps_suppkey" +
          "         )" +
          "         and s_nationkey = n_nationkey" +
          "         and trim(upper(n_name)) = 'CANADA'" +
          " order by" +
          "         s_name"
    }
  }

  def getResultString20():String = {
    "S_NAME|S_ADDRESS"
  }

  def getQuery21(isSnappy:Boolean): String = {
    //NATION = SAUDI ARABIA.
    if(isSnappy) {
      "select" +
          "         S_NAME," +
          "         count(*) as numwait" +
          " from" +
          "         SUPPLIER," +
          "         LINEITEM l1," +
          "         ORDERS," +
          "         NATION" +
          " where" +
          "         S_SUPPKEY = l1.l_suppkey" +
          "         and o_orderkey = l1.l_orderkey" +
          "         and o_orderstatus = 'F'" +
          "         and l1.l_receiptdate > l1.l_commitdate" +
          "         and exists (" +
          "                 select" +
          "                         *" +
          "                 from" +
          "                         LINEITEM l2" +
          "                 where" +
          "                         l2.l_orderkey = l1.l_orderkey" +
          "                         and l2.l_suppkey <> l1.l_suppkey" +
          "         )" +
          "         and not exists (" +
          "                 select" +
          "                         *" +
          "                 from" +
          "                         LINEITEM l3" +
          "                 where" +
          "                         l3.l_orderkey = l1.l_orderkey" +
          "                         and l3.l_suppkey <> l1.l_suppkey" +
          "                         and l3.l_receiptdate > l3.l_commitdate" +
          "         )" +
          "         and S_NATIONKEY = N_NATIONKEY" +
          "         and N_NAME = 'SAUDI ARABIA'" +
          " group by" +
          "         S_NAME" +
          " order by" +
          "         numwait desc," +
          "         S_NAME"
    }else{
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
          "         and exists (" +
          "                 select" +
          "                         *" +
          "                 from" +
          "                         LINEITEM l2" +
          "                 where" +
          "                         l2.l_orderkey = l1.l_orderkey" +
          "                         and l2.l_suppkey <> l1.l_suppkey" +
          "         )" +
          "         and not exists (" +
          "                 select" +
          "                         *" +
          "                 from" +
          "                         LINEITEM l3" +
          "                 where" +
          "                         l3.l_orderkey = l1.l_orderkey" +
          "                         and l3.l_suppkey <> l1.l_suppkey" +
          "                         and l3.l_receiptdate > l3.l_commitdate" +
          "         )" +
          "         and s_nationkey = n_nationkey" +
          "         and n_name = 'SAUDI ARABIA'" +
          " group by" +
          "         s_name" +
          " order by" +
          "         numwait desc," +
          "         s_name"
    }
  }

  def getResultString21():String = {
    "S_NAME|NUMWAIT"
  }

  def getTempQuery22() : String={
        "select" +
        "         avg(C_ACCTBAL)" +
        " from" +
        "         CUSTOMER" +
        " where" +
        "         C_ACCTBAL > 0.00" +
        "         and SUBSTR (C_PHONE,1,2) in" +
        "         (\"13\",\"31\",\"23\",\"29\",\"30\",\"18\",\"17\")"

  }
  def getQuery22(value:String): String = {
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
        "                 CUSTOMER  left outer join  ORDERS  on  o_custkey = C_CUSTKEY  " +
        "         where" +
        "                 SUBSTR(C_PHONE,1,2) IN" +
        "                         (\"13\",\"31\",\"23\",\"29\",\"30\",\"18\",\"17\")" +
        "                 and C_ACCTBAL > " +
        "                 " + value +
        "                 and o_orderkey IS NULL " +
        "         ) as custsale" +
        " group by" +
        "         cntrycode" +
        " order by" +
        "         cntrycode"
  }

  def getResultString22():String = {
    "CNTRYCODE|NUMCUST|TOTACCTBAL"
  }


}

