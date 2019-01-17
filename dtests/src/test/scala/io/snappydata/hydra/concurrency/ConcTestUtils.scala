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
package io.snappydata.hydra.concurrency

import java.io.PrintWriter
import java.sql.SQLException
import java.util.Random

import io.snappydata.hydra.SnappyTestUtils
import io.snappydata.hydra.northwind.{NWPLQueries, NWQueries}
import util.TestException

import org.apache.spark.sql.{SQLContext, SnappyContext}

object ConcTestUtils {

  val Q1_tpch: String = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, " +
      "sum(l_extendedprice) " +
      "as sum_base_price, sum(l_extendedprice*(1-l_discount)) " +
      "as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) " +
      "as sum_charge, avg(l_quantity) " +
      "as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc," +
      " count(*) as count_order " +
      "from LINEITEM where l_shipdate <= DATE_SUB('1997-12-31', 90 )" +
      "group by l_returnflag, l_linestatus order by  l_returnflag, l_linestatus"
  val Q2_tpch: String = "select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, " +
      "S_ADDRESS, S_PHONE, S_COMMENT from SUPPLIER, NATION, REGION, PART, " +
      "PARTSUPP where S_NATIONKEY = N_NATIONKEY and N_REGIONKEY = R_REGIONKEY " +
      "and R_NAME = 'ASIA' and S_SUPPKEY = PS_SUPPKEY  and P_PARTKEY = PS_PARTKEY " +
      "and P_SIZE = 24 and P_TYPE like '%STEEL' " +
      "and PS_SUPPLYCOST = (select min(PS_SUPPLYCOST) " +
      "from SUPPLIER, NATION, REGION, PARTSUPP " +
      "where S_NATIONKEY = N_NATIONKEY and N_REGIONKEY = R_REGIONKEY " +
      "and R_NAME = 'ASIA' and S_SUPPKEY = PS_SUPPKEY and P_PARTKEY = PS_PARTKEY) " +
      "order by S_ACCTBAL desc, N_NAME, S_NAME, P_PARTKEY limit 100"
  val Q3_tpch: String = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) " +
      "as revenue, o_orderdate, o_shippriority " +
      "from ORDERS, LINEITEM, CUSTOMER where C_MKTSEGMENT = 'BUILDING' " +
      "and C_CUSTKEY = o_custkey  and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' " +
      "and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority " +
      "order by l_orderkey limit 10"
  val Q4_tpch: String = "select o_orderpriority, count(*) " +
      "as order_count from  ORDERS " +
      "where o_orderdate >= '1993-07-01' " +
      "and o_orderdate < add_months('1993-07-01',3) " +
      "and exists (select l_orderkey from LINEITEM  " +
      "where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) " +
      "group by o_orderpriority order by o_orderpriority"
  val Q5_tpch: String = "select n_name, sum(l_extendedprice * (1 - l_discount)) " +
      "as revenue from SUPPLIER, NATION, REGION, ORDERS, LINEITEM, CUSTOMER " +
      "where s_nationkey = n_nationkey and n_regionkey = r_regionkey " +
      "and r_name = 'ASIA' and C_CUSTKEY = o_custkey and l_orderkey = o_orderkey " +
      "and l_suppkey = s_suppkey and C_NATIONKEY = s_nationkey and o_orderdate >= '1994-01-01' " +
      "and o_orderdate < add_months('1994-01-01', 12) group by n_name order by revenue desc"
  val Q6_tpch: String = "select sum(l_extendedprice*l_discount) " +
      "as revenue from LINEITEM where l_shipdate >= '1994-01-01' " +
      "and l_shipdate < add_months('1994-01-01', 12) " +
      "and l_discount between 0.06- 0.01 and 0.06 + 0.01 and l_quantity < 24"
  val Q7_tpch: String = "select supp_nation, cust_nation, l_year, sum(volume) " +
      "as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) " +
      "as l_year, l_extendedprice * (1 - l_discount) " +
      "as volume from SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2 " +
      "where s_suppkey = l_suppkey and o_orderkey = l_orderkey and C_CUSTKEY = o_custkey " +
      "and s_nationkey = n1.n_nationkey and C_NATIONKEY = n2.n_nationkey " +
      "and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') " +
      "or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) " +
      "and l_shipdate between '1995-01-01' and '1996-12-31') " +
      "as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"
  val Q8_tpch: String = "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume)" +
      " as mkt_share from (select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) " +
      "as volume, n2.n_name " +
      "as nation from LINEITEM, PART, ORDERS, CUSTOMER, NATION n1, REGION, NATION n2, SUPPLIER " +
      "where p_partkey = l_partkey and l_orderkey = o_orderkey " +
      "and o_custkey = C_CUSTKEY and C_NATIONKEY = n1.n_nationkey and n1.n_regionkey = r_regionkey " +
      "and r_name = 'AMERICA' and o_orderdate between '1995-01-01' and '1996-12-31' " +
      "and p_type = 'ECONOMY ANODIZED STEEL' and s_suppkey = l_suppkey and s_nationkey = n2.n_nationkey)" +
      " as all_nations group by o_year order by o_year"
  val Q9_tpch: String = "select nation, o_year, sum(amount) as sum_profit " +
      "from (select n_name as nation, year(o_orderdate) " +
      "as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity " +
      "as amount from LINEITEM, PART, ORDERS, SUPPLIER, NATION, PARTSUPP " +
      "where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey " +
      "and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%')" +
      " as profit group by nation, o_year order by nation, o_year desc"
  val Q10_tpch: String = "select C_CUSTKEY, C_NAME, sum(l_extendedprice * (1 - l_discount)) " +
      "as revenue, C_ACCTBAL, n_name, C_ADDRESS, C_PHONE, C_COMMENT " +
      "from ORDERS, LINEITEM, CUSTOMER, NATION where C_CUSTKEY = o_custkey " +
      "and l_orderkey = o_orderkey and o_orderdate >= '1993-10-01' " +
      "and o_orderdate < add_months('1993-10-01', 3) and l_returnflag = 'R' " +
      "and C_NATIONKEY = n_nationkey " +
      "group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, n_name, C_ADDRESS, C_COMMENT" +
      " order by revenue desc limit 20"
  val Q11_tpch: String = "select PS_PARTKEY, sum(PS_SUPPLYCOST * PS_AVAILQTY) " +
      "as value from SUPPLIER,NATION,PARTSUPP " +
      "where PS_SUPPKEY = S_SUPPKEY and S_NATIONKEY = N_NATIONKEY " +
      "and N_NAME = 'GERMANY' group by PS_PARTKEY " +
      "having sum(PS_SUPPLYCOST * PS_AVAILQTY) > (select sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0000001 " +
      "from SUPPLIER, NATION, PARTSUPP " +
      "where PS_SUPPKEY = S_SUPPKEY and S_NATIONKEY = N_NATIONKEY and N_NAME = 'GERMANY')" +
      " order by value desc"
  val Q12_tpch: String = "select l_shipmode," +
      "sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end)" +
      " as high_line_count," +
      "sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) " +
      "as low_line_count " +
      "from ORDERS, LINEITEM where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') " +
      "and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' " +
      "and l_receiptdate < add_months('1994-01-01',12) group by l_shipmode order by l_shipmode"
  val Q13_tpch: String = "select c_count, count(*) " +
      "as custdist from (select C_CUSTKEY, count(o_orderkey) " +
      "as c_count from CUSTOMER left outer join ORDERS " +
      "on C_CUSTKEY = o_custkey and o_comment not like '%special%requests%' " +
      "group by C_CUSTKEY ) as c_orders group by c_count order by custdist desc, c_count desc"
  val Q14_tpch: String = "select 100.00 * sum(case when p_type like 'PROMO%' " +
      "then l_extendedprice*(1-l_discount) else 0 end ) / sum(l_extendedprice * (1 - l_discount)) " +
      "as promo_revenue from LINEITEM, PART where l_partkey = p_partkey and l_shipdate >= '?'" +
      " and l_shipdate < add_months ('1995-09-01', 1)"
  val Q15_tpch: String = "select s_suppkey, s_name, s_address, s_phone, total_revenue " +
      "from SUPPLIER, revenue " +
      "where s_suppkey = supplier_no and total_revenue = (select max(total_revenue) from  revenue )" +
      " order by s_suppkey"
  val Q16_tpch: String = "select p_brand, p_type, p_size, " +
      "count(distinct ps_suppkey) as supplier_cnt " +
      "from PARTSUPP, PART where p_partkey = ps_partkey and p_brand <> 'Brand#45' " +
      "and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9)" +
      " and not exists ( select s_suppkey from SUPPLIER" +
      " where s_suppkey = ps_suppkey and s_comment like '%Customer%Complaints%' )" +
      " group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size"
  val Q17_tpch: String = "select sum(l_extendedprice) / 7.0 as avg_yearly" +
      " from LINEITEM, PART where P_PARTKEY = l_partkey and P_BRAND = 'Brand#23' " +
      "and P_CONTAINER = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity)" +
      " from LINEITEM where l_partkey = P_PARTKEY)"
  val Q18_tpch: String = "select C_NAME, C_CUSTKEY, o_orderkey, o_orderdate, o_totalprice, " +
      "sum(l_quantity) from CUSTOMER, ORDERS, LINEITEM where o_orderkey " +
      "in (select l_orderkey from LINEITEM group by l_orderkey having sum(l_quantity) > 300 ) " +
      "and C_CUSTKEY = o_custkey and o_orderkey = l_orderkey " +
      "group by C_NAME, C_CUSTKEY, o_orderkey, o_orderdate, o_totalprice " +
      "order by o_totalprice desc, o_orderdate limit 100"
  val Q19_tpch: String = "select sum(l_extendedprice * (1 - l_discount) ) as revenue" +
      " from LINEITEM, PART where ( p_partkey = l_partkey and p_brand = 'Brand#12'" +
      " and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1" +
      " and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG')" +
      " and l_shipinstruct = 'DELIVER IN PERSON' ) or " +
      "( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container " +
      "in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 " +
      "and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode " +
      "in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) " +
      "or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container " +
      "in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 " +
      "and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode " +
      "in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )"
  val Q20_tpch: String = "select S_NAME, S_ADDRESS from SUPPLIER, NATION " +
      "where S_SUPPKEY in ( select PS_SUPPKEY from PARTSUPP where PS_PARTKEY " +
      "in ( select P_PARTKEY from PART where P_NAME like 'forest%' ) " +
      "and PS_AVAILQTY > ( select 0.5 * sum(l_quantity) from LINEITEM " +
      "where l_partkey = PS_PARTKEY and l_suppkey = PS_SUPPKEY " +
      "and l_shipdate >= '1994-01-01' and l_shipdate < add_months('1994-01-01', 12) ) ) " +
      "and S_NATIONKEY = N_NATIONKEY and N_NAME = 'CANADA' order by S_NAME"
  val Q21_tpch: String = "select s_name, count(*) as numwait " +
      "from SUPPLIER, LINEITEM l1, ORDERS, NATION " +
      "where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey " +
      "and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate " +
      "and not exists ( select l3.l_orderkey from LINEITEM l3 " +
      "where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey " +
      "and l3.l_receiptdate > l3.l_commitdate ) and exists ( select l2.l_orderkey " +
      "from LINEITEM l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) " +
      "and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' " +
      "group by s_name order by numwait desc, s_name limit 100"
  val Q22_tpch: String = "select cntrycode, count(*) as numcust, sum(C_ACCTBAL) as totacctbal " +
      "from ( select SUBSTR(C_PHONE,1,2) as cntrycode, C_ACCTBAL from CUSTOMER " +
      "where SUBSTR(C_PHONE,1,2) in  ('13','31','23','29','30','18','17') " +
      "and C_ACCTBAL > ( select avg(C_ACCTBAL) from CUSTOMER where C_ACCTBAL > 0.00 " +
      "and SUBSTR(C_PHONE,1,2) in ('13','31','23','29','30','18','17') ) and " +
      "not exists ( select o_custkey from ORDERS where o_custkey = C_CUSTKEY ) ) as custsale" +
      " group by cntrycode order by cntrycode"

  def queryVect: scala.Vector[String] = Vector(Q1_tpch, Q2_tpch, Q3_tpch, Q4_tpch, Q5_tpch,
    Q6_tpch, Q7_tpch, Q8_tpch, Q9_tpch, Q10_tpch, Q11_tpch, Q12_tpch, Q13_tpch, Q14_tpch,
    Q15_tpch, Q16_tpch, Q17_tpch, Q18_tpch, Q19_tpch, Q20_tpch, Q21_tpch, Q22_tpch)

  def validateAnalyticalQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q37" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q37, "Q37",
          tableType, pw, sqlContext)
        case "Q55" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q55, "Q55",
          tableType, pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q36, "Q36",
          tableType, pw, sqlContext)
        case "Q56" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q56, "Q56",
          tableType, pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q38, "Q38",
          tableType, pw, sqlContext)
        // scalastyle:off println
        case _ => println("OK")
        // scalastyle:on println
      }
    }
  }

  def validatePointLookUPQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    for (q <- NWPLQueries.queries) {
      q._1 match {
        case "Q1" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q1, "Q1",
          tableType, pw, sqlContext)
        case "Q2" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q2, "Q2",
          tableType, pw, sqlContext)
        case "Q3" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q3, "Q3",
          tableType, pw, sqlContext)
        case "Q4" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q4, "Q4",
          tableType, pw, sqlContext)
        case "Q5" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q5, "Q5",
          tableType, pw, sqlContext)
        case "Q6" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q6, "Q6",
          tableType, pw, sqlContext)
        case "Q7" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q7, "Q7",
          tableType, pw, sqlContext)
        case "Q8" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q8, "Q8",
          tableType, pw, sqlContext)
        case "Q9" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q9, "Q9",
          tableType, pw, sqlContext)
        case "Q10" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q5, "Q10",
          tableType, pw, sqlContext)
        case "Q11" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q11, "Q11",
          tableType, pw, sqlContext)
        case "Q12" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q12, "Q12",
          tableType, pw, sqlContext)
        case "Q13" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q13, "Q13",
          tableType, pw, sqlContext)
        case "Q14" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q14, "Q14",
          tableType, pw, sqlContext)
        case "Q15" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q15, "Q15",
          tableType, pw, sqlContext)
        case "Q16" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q16, "Q16",
          tableType, pw, sqlContext)
        case "Q17" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q17, "Q17",
          tableType, pw, sqlContext)
        case "Q18" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q18, "Q18",
          tableType, pw, sqlContext)
        case "Q19" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q19, "Q19",
          tableType, pw, sqlContext)
        case "Q20" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q20, "Q20",
          tableType, pw, sqlContext)
        case "Q21" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q21, "Q21",
          tableType, pw, sqlContext)
        case "Q22" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q22, "Q22",
          tableType, pw, sqlContext)
        case "Q23" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q23, "Q23",
          tableType, pw, sqlContext)
        case "Q24" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q24, "Q24",
          tableType, pw, sqlContext)
        case "Q25" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q25, "Q25",
          tableType, pw, sqlContext)
        case "Q26" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q26, "Q28",
          tableType, pw, sqlContext)
        case "Q27" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q27, "Q27",
          tableType, pw, sqlContext)
        case "Q28" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q28, "Q28",
          tableType, pw, sqlContext)
        case "Q29" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q29, "Q29",
          tableType, pw, sqlContext)
        case "Q30" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q30, "Q30",
          tableType, pw, sqlContext)
        case "Q31" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q31, "Q31",
          tableType, pw, sqlContext)
        case "Q32" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q32, "Q32",
          tableType, pw, sqlContext)
        case "Q33" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q33, "Q33",
          tableType, pw, sqlContext)
        case "Q34" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q34, "Q34",
          tableType, pw, sqlContext)
        case "Q35" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q35, "Q35",
          tableType, pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q36, "Q36",
          tableType, pw, sqlContext)
        case "Q37" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q37, "Q37",
          tableType, pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q38, "Q38",
          tableType, pw, sqlContext)
        case "Q39" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q39, "Q39",
          tableType, pw, sqlContext)
        case "Q40" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q40, "Q40",
          tableType, pw, sqlContext)
        case "Q41" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q41, "Q41",
          tableType, pw, sqlContext)
        case "Q42" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q42, "Q42",
          tableType, pw, sqlContext)
        case "Q43" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q43, "Q43",
          tableType, pw, sqlContext)
        case "Q44" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q44, "Q44",
          tableType, pw, sqlContext)
        case "Q45" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q45, "Q45",
          tableType, pw, sqlContext)
        case "Q46" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q46, "Q46",
          tableType, pw, sqlContext)
        case "Q47" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q47, "Q47",
          tableType, pw, sqlContext)
        case "Q48" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q48, "Q48",
          tableType, pw, sqlContext)
        case "Q49" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q49, "Q49",
          tableType, pw, sqlContext)
        case "Q50" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q50, "Q50",
          tableType, pw, sqlContext)
        case "Q51" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q51, "Q51",
          tableType, pw, sqlContext)
        /* case "Q52" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q52, "Q52",
          tableType, pw, sqlContext) */
        // scalastyle:off println
        case _ => println("OK")
        // scalastyle:on println
      }
    }
  }


  def runAnalyticalQueries(snc: SnappyContext, pw: PrintWriter, warmUpTimeSec: Long,
      totalTaskTime: Long): Unit = {
    var query: String = null
    var numAggregationQueriesExecuted = 0
    query = "create or replace temporary view revenue as select  l_suppkey as supplier_no, " +
        "sum(l_extendedprice * (1 - l_discount)) as total_revenue " +
        "from LINEITEM where l_shipdate >= '1993-02-01'" +
        " and l_shipdate <  add_months('1996-01-01',3) group by l_suppkey"
    snc.sql(query)
    var startTime: Long = System.currentTimeMillis
    var endTime: Long = startTime + warmUpTimeSec * 1000
    while (endTime > System.currentTimeMillis)
      try {
        val queryNum: Int = new Random().nextInt(queryVect.size)
        query = queryVect(queryNum)
        snc.sql(query)
      } catch {
        case se: SQLException =>
          throw new TestException("Got exception while executing Analytical query:" + query, se)
      }
    startTime = System.currentTimeMillis
    endTime = startTime + totalTaskTime * 1000
    while (endTime > System.currentTimeMillis)
      try {
        val queryNum: Int = new Random().nextInt(queryVect.size)
        query = queryVect(queryNum)
        snc.sql(query)
        numAggregationQueriesExecuted += 1
        // val queryExecutionTime: Long = queryExecutionEndTime - startTime
      }
      catch {
        case se: SQLException =>
          throw new TestException("Got exception while executing Analytical query:" + query, se)
      }
    // scalastyle:off println
    pw.println(s"Total number of analytical queries executed: ${numAggregationQueriesExecuted}")
  }
}
