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
package io.snappydata.benchmark.kuduimpala

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{ResultSet, Statement}


object TPCH_Impala {

  var avgFileStream:FileOutputStream = new FileOutputStream(new File(s"Average.out"))
  var avgPrintStream:PrintStream = new PrintStream(avgFileStream)

   def close(): Unit ={
     avgPrintStream.close()
     avgFileStream.close()
   }

   def execute(queryNumber: String, isResultCollection: Boolean, stmt: Statement, warmup:Integer, runsForAverage:Integer): Unit = {

     var queryFileStream = new FileOutputStream(new File(s"$queryNumber.out"))
     var queryPrintStream = new PrintStream(queryFileStream)


     var resultFormat = queryNumber match {
       case "1" => getResultString1()
       case "2" => getResultString2()
       case "3" => getResultString3()
       case "4" => getResultString4()
       case "5" => getResultString5()
       case "6" => getResultString6()
       case "7" => getResultString7()
       case "8" => getResultString8()
       case "9" => getResultString9()
       case "10" => getResultString10()
       case "11" => getResultString11()
       case "12" => getResultString12()
       case "13" => getResultString13()
       case "14" => getResultString14()
       case "15" => getResultString15()
       case "16" => getResultString16()
       case "17" => getResultString17()
       case "18" => getResultString18()
       case "19" => getResultString19()
       case "20" => getResultString20()
       case "21" => getResultString21()
       case "22" => getResultString22()
     }

     var rs: ResultSet = null
     try {
       println(s"Started executing $queryNumber")
       queryPrintStream.println(s"$queryNumber")
       if(isResultCollection){
         rs = queryExecution(queryNumber, stmt)
         //rs = stmt.executeQuery(query)
         queryPrintStream.println(s"$resultFormat")
         val rsmd = rs.getMetaData()
         val columnsNumber = rsmd.getColumnCount();
         var count : Int = 0
         while (rs.next()) {
           count += 1
           for (i:Int <- 1 to columnsNumber) {
             if (i > 1) queryPrintStream.print(",")
             queryPrintStream.print(rs.getString(i))
           }
           queryPrintStream.println()
         }
         println(s"Number of results : $count")
         println(s"$queryNumber Result Collected in file $queryNumber.out")
         if (queryNumber.equals("q15")) {
           stmt.execute("drop view revenue")
         }
       } else {
         var totalTime: Long = 0
         var bestTime: Long=0
         for (i <- 1 to (warmup + runsForAverage)) {
           val startTime = System.currentTimeMillis()
           rs = queryExecution(queryNumber, stmt)
           //rs = stmt.executeQuery(query)
           while (rs.next()) {
             //just iterating over result
           }
           val endTime = System.currentTimeMillis()
           val iterationTime = endTime - startTime
           if(i==1){
             bestTime = iterationTime
           }else{
             if(iterationTime < bestTime)
               bestTime = iterationTime
           }
           queryPrintStream.println(s"$iterationTime")
           if (i > warmup) {
             totalTime += iterationTime
           }
           if (queryNumber.equals("q15")) {
             stmt.execute("drop view revenue")
           }
         }
         queryPrintStream.println(s"${totalTime / runsForAverage}")
         avgPrintStream.println(s"$queryNumber,$bestTime / ${totalTime /runsForAverage}")
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
       if(isResultCollection) {
         queryPrintStream.close()
         queryFileStream.close()
         avgPrintStream.close()
         avgFileStream.close()
       }

     }
     rs.close()
   }

   def queryExecution(queryNumber:String, stmt:Statement): ResultSet ={

     val rs : ResultSet = queryNumber match {
       case "1" => {
         stmt.executeQuery(getQuery1())
       }
       case "2" => {
         stmt.executeQuery(getQuery2())
       }
       case "3" => {
         stmt.executeQuery(getQuery3())
       }
       case "4" => {
         stmt.executeQuery(getQuery4())
       }
       case "5" => {
         stmt.executeQuery(getQuery5())
       }
       case "6" => {
         stmt.executeQuery(getQuery6())
       }
       case "7" => {
         stmt.executeQuery(getQuery7())
       }
       case "8" => {
         stmt.executeQuery(getQuery8())
       }
       case "9" => {
         stmt.executeQuery(getQuery9())
       }
       case "10" => {
         stmt.executeQuery(getQuery10())
       }
       case "11" => {
         stmt.executeQuery(getQuery11())
       }
       case "12" => {
         stmt.executeQuery(getQuery12())
       }
       case "13" => {
         stmt.executeQuery(getQuery13())
       }
       case "14" => {
         stmt.executeQuery(getQuery14())
       }
       case "15" => {
         stmt.execute(getTempQuery15())
         stmt.executeQuery(getQuery15())
       }
       case "16" => {
         stmt.executeQuery(getQuery16())
       }
       case "17" => {
         stmt.executeQuery(getQuery17())
       }
       case "18" => {
         stmt.executeQuery(getQuery18())
       }
       case "19" => {
         stmt.executeQuery(getQuery19())
       }
       case "20" => {
         stmt.executeQuery(getQuery20())
       }
       case "21" => {
         stmt.executeQuery(getQuery21())
       }
       case "22" => {
         stmt.executeQuery(getQuery22())
       }
     }
     rs

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
       "     l_shipdate <= DATE_SUB('1997-12-31',90)" +
       " group by" +
       "     l_returnflag," +
       "     l_linestatus" +
       " order by" +
       "     l_returnflag," +
       "     l_linestatus"
   }

   def getResultString1(): String = {
     "l_returnflag l_linestatus sum_qty sum_base_price sum_disc_price sum_charge avg_qty avg_price avg_disc count_order"
   }

   def getQuery2(): String = {
     //    1. SIZE = 15;
     //    2. TYPE = BRASS;
     //    3. REGION = EUROPE

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
         "     SUPPLIER," +
         "     PARTSUPP," +
         "     NATION," +
         "     REGION" +
         " where" +
         "     P_PARTKEY = PS_PARTKEY" +
         "     and S_SUPPKEY = PS_SUPPKEY" +
         "     and P_SIZE = 24" +
         "     and P_TYPE like '%STEEL'" +
         "     and S_NATIONKEY = N_NATIONKEY" +
         "     and N_REGIONKEY = R_REGIONKEY" +
         "     and R_NAME = 'ASIA'" +
         "     and PS_SUPPLYCOST = (" +
         "         select" +
         "             min(PS_SUPPLYCOST)" +
         "         from" +
         "             PARTSUPP, SUPPLIER," +
         "             NATION, REGION" +
         "         where" +
         "             P_PARTKEY = PS_PARTKEY" +
         "             and S_SUPPKEY = PS_SUPPKEY" +
         "             and S_NATIONKEY = N_NATIONKEY" +
         "             and N_REGIONKEY = R_REGIONKEY" +
         "             and R_NAME = 'ASIA'" +
         "            )" +
         " order by" +
         "     S_ACCTBAL desc," +
         "     N_NAME," +
         "     S_NAME," +
         "     P_PARTKEY limit 100"

   }

   def getResultString2(): String = {
     "S_ACCTBAL S_NAME N_NAME P_PARTKEY P_MFGR S_ADDRESS S_PHONE S_COMMENT"
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
         "     C_MKTSEGMENT = 'BUILDING'" +
         "     and C_CUSTKEY = o_custkey" +
         "     and l_orderkey = o_orderkey" +
         "     and o_orderdate < '1995-03-15'" +
         "     and l_shipdate > '1995-03-15' " +
         " group by" +
         "     l_orderkey," +
         "     o_orderdate," +
         "     o_shippriority" +
         " order by" +
         "     o_orderdate limit 10"

   }

   def getResultString3(): String = {
     "l_orderkey revenue o_orderdate o_shippriority"
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

   def getResultString4(): String = {
     "o_orderpriority order_count"
   }

   def getQuery5(): String = {
     //1. REGION = ASIA;
     //2. DATE = 1994-01-01.
     " select" +
       "        n_name," +
       "        sum(l_extendedprice * (1 - l_discount)) as revenue" +
       " from" +
       "        ORDERS," +
       "        LINEITEM," +
       "        SUPPLIER," +
       "        NATION," +
       "        REGION," +
       "        CUSTOMER" +
       " where" +
       "        C_CUSTKEY = o_custkey" +
       "        and l_orderkey = o_orderkey" +
       "        and l_suppkey = s_suppkey" +
       "        and C_NATIONKEY = s_nationkey" +
       "        and s_nationkey = n_nationkey" +
       "        and n_regionkey = r_regionkey" +
       "        and r_name = 'ASIA'" +
       "        and o_orderdate >= '1994-01-01'" +
       "        and o_orderdate < add_months('1994-01-01', 12)" +
       " group by" +
       "        n_name" +
       " order by" +
       "        revenue desc"
   }

   def getResultString5(): String = {
     "N_NAME revenue"
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

  def getQuery7(): String = {
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

  def getResultString7():String = {
    "supp_nation|cust_nation|l_year|revenue"
  }

  def getQuery8(): String = {
    //    1. NATION = BRAZIL;
    //    2. REGION = AMERICA;
    //    3. TYPE = ECONOMY ANODIZED STEEL.
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
        "                         ORDERS," +
        "                         CUSTOMER," +
        "                         SUPPLIER," +
        "                         NATION n1," +
        "                         REGION," +
        "                         NATION n2," +
        "                         PART" +
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

  def getResultString8():String = {
    "YEAR|MKT_SHARE"
  }

  def getQuery9(): String = {
    //1. COLOR = green.
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
        "                 LINEITEM," +
        "                 ORDERS," +
        "                 SUPPLIER," +
        "                 NATION," +
        "                 PART," +
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
  }

  def getResultString9():String = {
    "NATION|YEAR|SUM_PROFIT"
  }
  def getQuery10(): String = {
    //1.    DATE = 1993-10-01.
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
      "         revenue desc limit 20"
    // }
  }

  def getResultString10():String = {
    "C_CUSTKEY|C_NAME|REVENUE|C_ACCTBAL|N_NAME|C_ADDRESS|C_PHONE|C_COMMENT"
  }

  def getQuery11(): String = {
     //    1. NATION = GERMANY;
     //    2. FRACTION = 0.0001.
     "select" +
         "         PS_PARTKEY," +
         "         sum(PS_SUPPLYCOST * PS_AVAILQTY) as value" +
         " from" +
         "         PARTSUPP," +
         "         SUPPLIER," +
         "         NATION" +
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
         "                         PARTSUPP," +
         "                         SUPPLIER," +
         "                         NATION" +
         "                 where" +
         "                         PS_SUPPKEY = S_SUPPKEY" +
         "                         and S_NATIONKEY = N_NATIONKEY" +
         "                         and N_NAME = 'GERMANY'" +
         "         )" +
         " order by" +
         "         value desc"
   }

   def getResultString11(): String = {
     "PS_PARTKEY VALUE"
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

   def getResultString12(): String = {
     "L_SHIPMODE HIGH_LINE_COUNT LOW_LINE_COUNT"
   }

   def getTempQuery13():String={
     "create view"+
         " ViewQ13 as"+
           " select" +
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

     "select "+
       "	c_count, "+
       "        count(*) as custdist "+
       " from "+
       "    ("+
       "      select "+
       "     	 c_custkey,"+
       "     	 count(o_orderkey) as c_count"+
       "      from"+
       "	 customer c left outer join "+
       "	 orders o on c.c_custkey = o.o_custkey"+
       "	 and not o.o_comment like '%special%requests%'"+
       "	 group by c_custkey ) "+
       "    c_orders"+
       "    group by c_count"+
       "    order by custdist desc,"+
       "    c_count desc"

   }

 //  def getQuery13(): String = {
 //    //    1. WORD1 = special.
 //    //    2. WORD2 = requests.
 //    "select" +
 //        "         c_count, " +
 //        "         count(*) as custdist" +
 //        " from (" +
 //        "         select" +
 //        "                 C_CUSTKEY," +
 //        "                 count(o_orderkey)" +
 //        "         from" +
 //        "                 CUSTOMER left outer join ORDERS on" +
 //        "                 C_CUSTKEY = o_custkey" +
 //        "                 and o_comment not like ‘%special%requests%’" +
 //        "         group by" +
 //        "                 C_CUSTKEY" +
 //        "         )as c_orders (C_CUSTKEY, c_count)" +
 //        " group by" +
 //        "         c_count" +
 //        " order by" +
 //        "         custdist desc," +
 //        "         c_count desc"
 //  }

   def getResultString13(): String = {
     "C_COUNT CUSTDIST"
   }

   def getQuery14(): String = {
     //1.DATE = 1995-09-01.
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

   }

   def getResultString14(): String = {
     "PROMO_REVENUE"
   }


   def getTempQuery15(): String = {
     "create view " +
         "        revenue as" +
         " select" +
         "      l_suppkey as supplier_no ," +
         "      sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
         " from" +
         "      LINEITEM" +
         " where" +
         "      l_shipdate >= '1996-01-01'" +
         "      and l_shipdate < add_months('1996-01-01',3)" +
         " group by" +
         "      l_suppkey"
   }

   def getQuery15(): String = {
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

   def getResultString15(): String = {
     ""
   }

   def getQuery16(): String = {
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
   }

   def getResultString16(): String = {
     "P_BRAND P_TYPE P_SIZE SUPPLIER_CNT"
   }

   def getQuery17(): String = {
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
         //" )"

   }

   def getResultString17(): String = {
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
         "                         l_orderkey having" +
         "                         sum(l_quantity) > 300" +
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
         "         o_orderdate limit 100"
   }

   def getResultString18(): String = {
     "C_NAME C_CUSTKEY O_ORDERKEY O_ORDERDATE O_TOTALPRICE Sum(L_QUANTITY)"
   }

   def getQuery19(): String = {
     //    1. QUANTITY1 = 1.
     //    2. QUANTITY2 = 10.
     //    3. QUANTITY3 = 20.
     //    4. BRAND1 = Brand#12.
     //    5. BRAND2 = Brand#23.
     //    6. BRAND3 = Brand#34.
     //"select sum(l_extendedprice * (1 - l_discount)) as revenue from LINEITEM, PART where (P_PARTKEY = l_partkey and P_BRAND = ‘Brand#12’ and P_CONTAINER in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’) and l_quantity >= 1 and l_quantity <= 1 + 10 and P_SIZE between 1 and 5 and l_shipmode in (‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’) or (P_PARTKEY = l_partkey and P_BRAND = ‘Brand#23’ and P_CONTAINER in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’) and l_quantity >= 10 and l_quantity <= 10 + 10 and P_SIZE between 1 and 10 and l_shipmode in (‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’ ) or ( P_PARTKEY = l_partkey and P_BRAND = ‘Brand#34’ and P_CONTAINER in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’) and l_quantity >= 20 and l_quantity <= 20 + 10 and P_SIZE between 1 and 15 and l_shipmode in (‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’ )"
     "select" +
         "      sum(l_extendedprice * (1 - l_discount)) as revenue" +
         " from" +
         "         LINEITEM," +
         "         PART" +
         " where" +
         "         (" +
         "                 P_PARTKEY = l_partkey" +
         "                 and P_BRAND = 'Brand#12'" +
         "                 and P_CONTAINER in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')" +
         "                 and l_quantity >= 1 and l_quantity <= 1 + 10" +
         "                 and l_shipmode in ('AIR', 'AIR REG')" +
         "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
         "                 and P_SIZE between 1 and 5" +
         "         )" +
         "         or" +
         "         (" +
         "                 P_PARTKEY = l_partkey" +
         "                 and P_BRAND = 'Brand#23'" +
         "                 and P_CONTAINER in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')" +
         "                 and l_quantity >= 10 and l_quantity <= 10 + 10" +
         "                 and l_shipmode in ('AIR', 'AIR REG')" +
         "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
         "                 and P_SIZE between 1 and 10" +
         "         )" +
         "         or" +
         "         (" +
         "                 P_PARTKEY = l_partkey" +
         "                 and P_BRAND = 'Brand#34'" +
         "                 and P_CONTAINER in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')" +
         "                 and l_quantity >= 20 and l_quantity <= 20 + 10" +
         "                 and l_shipmode in ('AIR', 'AIR REG')" +
         "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
         "                 and P_SIZE between 1 and 15" +
         "         )"
   }

   def getResultString19(): String = {
     "REVENUE"
   }

   def getQuery20(): String = {
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

   def getResultString20(): String = {
     "S_NAME S_ADDRESS"
   }

   def getQuery21(): String = {
     //NATION = SAUDI ARABIA.
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
         "         and N_NAME = 'VIETNAM'" +
         " group by" +
         "         S_NAME" +
         " order by" +
         "         numwait desc," +
         "         S_NAME limit 100"
   }

   def getResultString21(): String = {
     "S_NAME NUMWAIT"
   }

   def getQuery22(): String = {
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
         "                                 *" +
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
     "CNTRYCODE NUMCUST TOTACCTBAL"
   }
 }
