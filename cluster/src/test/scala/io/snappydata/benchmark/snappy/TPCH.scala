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
package io.snappydata.benchmark.snappy

trait TPCHBase {
  type QUERY_TYPE = Seq[String] // we can add parameter type check later.
}

/**
 * Original TPCH queries as per specification.
 */
trait TPCH extends TPCHBase {

  def q1: String =
    s"""
       |select
       |      l_returnflag,
       |      l_linestatus,
       |      sum(l_quantity) as sum_qty,
       |      sum(l_extendedprice) as sum_base_price,
       |      sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
       |      sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
       |      avg(l_quantity) as avg_qty,
       |      avg(l_extendedprice) as avg_price,
       |      avg(l_discount) as avg_disc,
       |      count(*) as count_order
       |from
       |      lineitem
       |where
       |      l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
       |group by
       |      l_returnflag,
       |      l_linestatus
       |order by
       |      l_returnflag,
       |      l_linestatus
     """.stripMargin

  def q1Tokens: QUERY_TYPE = Seq("date '1998-12-01' - interval '[DELTA]' day (3)")

  def q2: String =
    s"""
       |select
       |      s_acctbal,
       |      s_name,
       |      n_name,
       |      p_partkey,
       |      p_mfgr,
       |      s_address,
       |      s_phone,
       |      s_comment
       |from
       |      part,
       |      supplier,
       |      partsupp,
       |      nation,
       |      region
       |where
       |      p_partkey = ps_partkey
       |      and s_suppkey = ps_suppkey
       |      and p_size = [SIZE]
       |      and p_type like '%[TYPE]'
       |      and s_nationkey = n_nationkey
       |      and n_regionkey = r_regionkey
       |      and r_name = '[REGION]'
       |      and ps_supplycost = (
       |      select
       |          min(ps_supplycost)
       |      from
       |          partsupp, supplier,
       |          nation, region
       |      where
       |          p_partkey = ps_partkey
       |          and s_suppkey = ps_suppkey
       |          and s_nationkey = n_nationkey
       |          and n_regionkey = r_regionkey
       |          and r_name = '[REGION]'
       |      )
       |order by
       |      s_acctbal desc,
       |      n_name,
       |      s_name,
       |      p_partkey
     """.stripMargin

  def q2Tokens: QUERY_TYPE = Seq("[SIZE]", "[TYPE]", "[REGION]")

  def q3: String =
    s"""
       |select
       |      l_orderkey,
       |      sum(l_extendedprice*(1-l_discount)) as revenue,
       |      o_orderdate,
       |      o_shippriority
       |from
       |      customer,
       |      orders,
       |      lineitem
       |where
       |      c_mktsegment = '[SEGMENT]'
       |      and c_custkey = o_custkey
       |      and l_orderkey = o_orderkey
       |      and o_orderdate < date '[DATE]'
       |      and l_shipdate > date '[DATE]'
       |group by
       |      l_orderkey,
       |      o_orderdate,
       |      o_shippriority
       |order by
       |      revenue desc,
       |      o_orderdate
     """.stripMargin

  def q3Tokens: QUERY_TYPE = Seq("[SEGMENT]", "date '[DATE]'")

  def q4: String =
    s"""
       |select
       |      o_orderpriority,
       |      count(*) as order_count
       |from
       |      orders
       |where
       |      o_orderdate >= date '[DATE]'
       |      and o_orderdate < date '[DATE] ' + interval '3' month
       |      and exists (
       |      select
       |            *
       |      from
       |            lineitem
       |      where
       |            l_orderkey = o_orderkey
       |            and l_commitdate < l_receiptdate
       |      )
       |group by
       |      o_orderpriority
       |order by
       |      o_orderpriority
     """.stripMargin

  // note the additional space for interval token. date '[DATE] ' gets distinguished from
  // date '[DATE]' and hence replaceAll doesn't changes it. Didn't wanted to get into
  // regex here.
  def q4Tokens: QUERY_TYPE = Seq("date '[DATE]'", "date '[DATE] ' + interval '3' month")

  def q5: String =
    s"""
       |select
       |      n_name,
       |      sum(l_extendedprice * (1 - l_discount)) as revenue
       |from
       |      customer,
       |      orders,
       |      lineitem,
       |      supplier,
       |      nation,
       |      region
       |where
       |      c_custkey = o_custkey
       |      and l_orderkey = o_orderkey
       |      and l_suppkey = s_suppkey
       |      and c_nationkey = s_nationkey
       |      and s_nationkey = n_nationkey
       |      and n_regionkey = r_regionkey
       |      and r_name = '[REGION]'
       |      and o_orderdate >= date '[DATE]'
       |      and o_orderdate < date '[DATE] ' + interval '1' year
       |group by
       |      n_name
       |order by
       |      revenue desc
     """.stripMargin

  def q5Tokens: QUERY_TYPE = Seq("[REGION]", "date '[DATE]'", "date '[DATE] ' + interval '1' year")

  def q6: String =
    s"""
       |select
       |      sum(l_extendedprice*l_discount) as revenue
       |from
       |      lineitem
       |where
       |      l_shipdate >= date '[DATE]'
       |      and l_shipdate < date '[DATE] ' + interval '1' year
       |      and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
       |      and l_quantity < [QUANTITY]
     """.stripMargin

  def q6Tokens: QUERY_TYPE = Seq("[DISCOUNT]", "date '[DATE]'",
    "date '[DATE] ' + interval '1' year", "[QUANTITY]")

  def q7: String =
    s"""
       |select
       |      supp_nation,
       |      cust_nation,
       |      l_year,
       |      sum(volume) as revenue
       |from (
       |      select
       |            n1.n_name as supp_nation,
       |            n2.n_name as cust_nation,
       |            extract(year from l_shipdate) as l_year,
       |            l_extendedprice * (1 - l_discount) as volume
       |      from
       |            supplier,
       |            lineitem,
       |            orders,
       |            customer,
       |            nation n1,
       |            nation n2
       |      where
       |            s_suppkey = l_suppkey
       |            and o_orderkey = l_orderkey
       |            and c_custkey = o_custkey
       |            and s_nationkey = n1.n_nationkey
       |            and c_nationkey = n2.n_nationkey
       |            and (
       |                  (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
       |                or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')
       |            )
       |            and l_shipdate between date '1995-01-01' and date '1996-12-31'
       |      ) as shipping
       |group by
       |      supp_nation,
       |      cust_nation,
       |      l_year
       |order by
       |      supp_nation,
       |      cust_nation,
       |      l_year
     """.stripMargin

  def q7Tokens: QUERY_TYPE = Seq("between date '1995-01-01' and date '1996-12-31'",
    "extract(year from ",
    "[NATION1]", "[NATION2]")

  def q8: String =
    s"""
       |select
       |      o_year,
       |      sum(case
       |            when nation = '[NATION]' then volume
       |            else 0
       |          end) / sum(volume) as mkt_share
       |from (
       |      select
       |          extract(year from o_orderdate) as o_year,
       |          l_extendedprice * (1-l_discount) as volume,
       |          n2.n_name as nation
       |      from
       |          part,
       |          supplier,
       |          lineitem,
       |          orders,
       |          customer,
       |          nation n1,
       |          nation n2,
       |          region
       |      where
       |          p_partkey = l_partkey
       |          and s_suppkey = l_suppkey
       |          and l_orderkey = o_orderkey
       |          and o_custkey = c_custkey
       |          and c_nationkey = n1.n_nationkey
       |          and n1.n_regionkey = r_regionkey
       |          and r_name = '[REGION]'
       |          and s_nationkey = n2.n_nationkey
       |          and o_orderdate between date '1995-01-01' and date '1996-12-31'
       |          and p_type = '[TYPE]'
       |      ) as all_nations
       |group by
       |      o_year
       |order by
       |      o_year
     """.stripMargin

  def q8Tokens: QUERY_TYPE = Seq("between date '1995-01-01' and date '1996-12-31'",
    "extract(year from ",
    "[NATION]", "[REGION]", "[TYPE]")

  def q9: String =
    s"""
       |select
       |      nation,
       |      o_year,
       |      sum(amount) as sum_profit
       |from (
       |      select
       |          n_name as nation,
       |          extract(year from o_orderdate) as o_year,
       |          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
       |      from
       |          part,
       |          supplier,
       |          lineitem,
       |          partsupp,
       |          orders,
       |          nation
       |      where
       |          s_suppkey = l_suppkey
       |          and ps_suppkey = l_suppkey
       |          and ps_partkey = l_partkey
       |          and p_partkey = l_partkey
       |          and o_orderkey = l_orderkey
       |          and s_nationkey = n_nationkey
       |          and p_name like '%[COLOR]%'
       |      ) as profit
       |group by
       |      nation,
       |      o_year
       |order by
       |      nation,
       |      o_year desc
     """.stripMargin

  def q9Tokens: QUERY_TYPE = Seq("extract(year from ","[COLOR]")

  def q10: String =
    s"""
       |select
       |      c_custkey,
       |      c_name,
       |      sum(l_extendedprice * (1 - l_discount)) as revenue,
       |      c_acctbal,
       |      n_name,
       |      c_address,
       |      c_phone,
       |      c_comment
       |from
       |      customer,
       |      orders,
       |      lineitem,
       |      nation
       |where
       |      c_custkey = o_custkey
       |      and l_orderkey = o_orderkey
       |      and o_orderdate >= date '[DATE]'
       |      and o_orderdate < date '[DATE] ' + interval '3' month
       |      and l_returnflag = 'R'
       |      and c_nationkey = n_nationkey
       |group by
       |      c_custkey,
       |      c_name,
       |      c_acctbal,
       |      c_phone,
       |      n_name,
       |      c_address,
       |      c_comment
       |order by
       |      revenue desc
     """.stripMargin

  def q10Tokens: QUERY_TYPE = Seq("date '[DATE]'", "date '[DATE] ' + interval '3' month")

  def q11: String =
    s"""
       |select
       |      ps_partkey,
       |      sum(ps_supplycost * ps_availqty) as value
       |from
       |      partsupp,
       |      supplier,
       |      nation
       |where
       |      ps_suppkey = s_suppkey
       |      and s_nationkey = n_nationkey
       |      and n_name = '[NATION]'
       |group by
       |      ps_partkey having
       |      sum(ps_supplycost * ps_availqty) > (
       |      select
       |            sum(ps_supplycost * ps_availqty) * [FRACTION]
       |      from
       |            partsupp,
       |            supplier,
       |            nation
       |      where
       |            ps_suppkey = s_suppkey
       |            and s_nationkey = n_nationkey
       |            and n_name = '[NATION]'
       |      )
       |order by
       |      value desc
     """.stripMargin

  def q11Tokens: QUERY_TYPE = Seq("[NATION]", "[FRACTION]")

  def q12: String =
    s"""
       |select
       |      l_shipmode,
       |      sum(case
       |            when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH'
       |              then 1
       |            else 0
       |          end
       |      ) as high_line_count,
       |      sum(case
       |            when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
       |              then 1
       |            else 0
       |          end
       |      ) as low_line_count
       |from
       |      orders,
       |      lineitem
       |where
       |      o_orderkey = l_orderkey
       |      and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
       |      and l_commitdate < l_receiptdate
       |      and l_shipdate < l_commitdate
       |      and l_receiptdate >= date '[DATE]'
       |      and l_receiptdate < date '[DATE] ' + interval '1' year
       |group by
       |      l_shipmode
       |order by
       |      l_shipmode
     """.stripMargin

  def q12Tokens: QUERY_TYPE = Seq("[SHIPMODE1]", "[SHIPMODE2]", "date '[DATE]'",
    "date '[DATE] ' + interval '1' year")

  def q13: String =
    s"""
       |select
       |      c_count,
       |      count(*) as custdist
       |from (
       |      select
       |            c_custkey,
       |            count(o_orderkey) as c_count
       |      from
       |            customer left outer join orders on
       |            c_custkey = o_custkey
       |            and o_comment not like '%[WORD1]%[WORD2]%'
       |      group by
       |            c_custkey
       |      ) as c_orders
       |group by
       |      c_count
       |order by
       |      custdist desc,
       |      c_count desc
     """.stripMargin

  def q13Tokens: QUERY_TYPE = Seq("[WORD1]", "[WORD2]")

  def q14: String =
    s"""
       |select
       |      100.00 * sum(case
       |                      when p_type like 'PROMO%'
       |                        then l_extendedprice*(1-l_discount)
       |                      else 0
       |                   end
       |      ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
       |from
       |      lineitem,
       |      part
       |where
       |      l_partkey = p_partkey
       |      and l_shipdate >= date '[DATE]'
       |      and l_shipdate < date '[DATE] ' + interval '1' month
     """.stripMargin

  def q14Tokens: QUERY_TYPE = Seq("date '[DATE]'", "date '[DATE] ' + interval '1' month")

  def q15v: String =
    s"""
       |create view revenue[STREAM_ID] (supplier_no, total_revenue) as
       |      select
       |            l_suppkey as supplier_no,
       |            sum(l_extendedprice * (1 - l_discount)) as total_revenue
       |      from
       |            lineitem
       |      where
       |            l_shipdate >= date '[DATE]'
       |            and l_shipdate < date '[DATE] ' + interval '3' month
       |      group by
       |            l_suppkey
     """.stripMargin

  def q15vTokens: QUERY_TYPE = Seq("[STREAM_ID]", "date '[DATE]'",
    "date '[DATE] ' + interval '3' month")

  def q15: String =
    s"""
       |select
       |      s_suppkey,
       |      s_name,
       |      s_address,
       |      s_phone,
       |      total_revenue
       |from
       |      supplier,
       |      revenue[STREAM_ID]
       |where
       |      s_suppkey = supplier_no
       |      and total_revenue = (
       |      select
       |            max(total_revenue)
       |      from
       |            revenue[STREAM_ID]
       |      )
       |order by
       |      s_suppkey
     """.stripMargin

  def q15Tokens: QUERY_TYPE = Seq("[STREAM_ID]")

  def q16: String =
    s"""
       |select
       |      p_brand,
       |      p_type,
       |      p_size,
       |      count(distinct ps_suppkey) as supplier_cnt
       |from
       |      partsupp,
       |      part
       |where
       |      p_partkey = ps_partkey
       |      and p_brand <> '[BRAND]'
       |      and p_type not like '[TYPE]%'
       |      and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
       |      and ps_suppkey not in (
       |      select
       |            s_suppkey
       |      from
       |            supplier
       |      where
       |            s_comment like '%Customer%Complaints%'
       |      )
       |group by
       |      p_brand,
       |      p_type,
       |      p_size
       |order by
       |      supplier_cnt desc,
       |      p_brand,
       |      p_type,
       |      p_size
     """.stripMargin

  def q16Tokens: QUERY_TYPE = Seq("[BRAND]", "[TYPE]", "[SIZE1]", "[SIZE2]", "[SIZE3]", "[SIZE4]",
    "[SIZE5]", "[SIZE6]", "[SIZE7]", "[SIZE8]")

  def q17: String =
    s"""
       |select
       |      sum(l_extendedprice) / 7.0 as avg_yearly
       |      from
       |      lineitem,
       |      part
       |where
       |      p_partkey = l_partkey
       |      and p_brand = '[BRAND]'
       |      and p_container = '[CONTAINER]'
       |      and l_quantity < (
       |      select
       |            0.2 * avg(l_quantity)
       |      from
       |            lineitem
       |      where
       |            l_partkey = p_partkey
       |      )
     """.stripMargin

  def q17Tokens: QUERY_TYPE = Seq("[BRAND]", "[CONTAINER]")

  def q18: String =
    s"""
       |select
       |      c_name,
       |      c_custkey,
       |      o_orderkey,
       |      o_orderdate,
       |      o_totalprice,
       |      sum(l_quantity)
       |from
       |      customer,
       |      orders,
       |      lineitem
       |where
       |      o_orderkey in (
       |      select
       |            l_orderkey
       |      from
       |            lineitem
       |      group by
       |            l_orderkey
       |      having
       |            sum(l_quantity) > [QUANTITY]
       |      )
       |      and c_custkey = o_custkey
       |      and o_orderkey = l_orderkey
       |group by
       |      c_name,
       |      c_custkey,
       |      o_orderkey,
       |      o_orderdate,
       |      o_totalprice
       |order by
       |      o_totalprice desc,
       |      o_orderdate
     """.stripMargin

  def q18Tokens: QUERY_TYPE = Seq("[QUANTITY]")

  def q19: String =
    s"""
       |select
       |      sum(l_extendedprice * (1 - l_discount) ) as revenue
       |from
       |      lineitem,
       |      part
       |where
       |      (
       |        p_partkey = l_partkey
       |        and p_brand = '[BRAND1]'
       |        and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
       |        and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
       |        and p_size between 1 and 5
       |        and l_shipmode in ('AIR', 'AIR REG')
       |        and l_shipinstruct = 'DELIVER IN PERSON'
       |      )
       |      or
       |      (
       |        p_partkey = l_partkey
       |        and p_brand = '[BRAND2]'
       |        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
       |        and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
       |        and p_size between 1 and 10
       |        and l_shipmode in ('AIR', 'AIR REG')
       |        and l_shipinstruct = 'DELIVER IN PERSON'
       |      )
       |      or
       |      (
       |        p_partkey = l_partkey
       |        and p_brand = '[BRAND3]'
       |        and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
       |        and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
       |        and p_size between 1 and 15
       |        and l_shipmode in ('AIR', 'AIR REG')
       |        and l_shipinstruct = 'DELIVER IN PERSON'
       |      )
     """.stripMargin

  def q19Tokens: QUERY_TYPE = Seq("[BRAND1]", "[QUANTITY1]",
    "[BRAND2]", "[QUANTITY2]",
    "[BRAND3]", "[QUANTITY3]")

  def q20: String =
    s"""
       |select
       |      s_name,
       |      s_address
       |from
       |      supplier, nation
       |where
       |      s_suppkey in (
       |      select
       |            ps_suppkey
       |      from
       |            partsupp
       |      where
       |            ps_partkey in (
       |              select
       |                  p_partkey
       |              from
       |                  part
       |              where
       |                  p_name like '[COLOR]%'
       |            )
       |      and ps_availqty > (
       |              select
       |                  0.5 * sum(l_quantity)
       |              from
       |                  lineitem
       |              where
       |                  l_partkey = ps_partkey
       |                  and l_suppkey = ps_suppkey
       |                  and l_shipdate >= date '[DATE]'
       |                  and l_shipdate < date '[DATE] ' + interval '1' year
       |            )
       |      )
       |      and s_nationkey = n_nationkey
       |      and n_name = '[NATION]'
       |order by
       |      s_name
     """.stripMargin

  def q20Tokens: QUERY_TYPE = Seq("[COLOR]", "date '[DATE]'",
    "date '[DATE] ' + interval '1' year", "[NATION]")

  def q21: String =
    s"""
       |select
       |      s_name,
       |      count(*) as numwait
       |from
       |      supplier,
       |      lineitem l1,
       |      orders,
       |      nation
       |where
       |      s_suppkey = l1.l_suppkey
       |      and o_orderkey = l1.l_orderkey
       |      and o_orderstatus = 'F'
       |      and l1.l_receiptdate > l1.l_commitdate
       |      and exists (
       |            select
       |                *
       |            from
       |                lineitem l2
       |            where
       |                l2.l_orderkey = l1.l_orderkey
       |                and l2.l_suppkey <> l1.l_suppkey
       |      )
       |      and not exists (
       |            select
       |                *
       |            from
       |                lineitem l3
       |            where
       |                l3.l_orderkey = l1.l_orderkey
       |                and l3.l_suppkey <> l1.l_suppkey
       |                and l3.l_receiptdate > l3.l_commitdate
       |      )
       |      and s_nationkey = n_nationkey
       |      and n_name = '[NATION]'
       |group by
       |      s_name
       |order by
       |      numwait desc,
       |      s_name
   """.stripMargin

  def q21Tokens: QUERY_TYPE = Seq("[NATION]")

  def q22: String =
    s"""
       |select
       |      cntrycode,
       |      count(*) as numcust,
       |      sum(c_acctbal) as totacctbal
       |from (
       |      select
       |          substring(c_phone from 1 for 2) as cntrycode,
       |          c_acctbal
       |      from
       |          customer
       |      where
       |          substring(c_phone from 1 for 2) in
       |            ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
       |          and c_acctbal > (
       |                  select
       |                      avg(c_acctbal)
       |                  from
       |                      customer
       |                  where
       |                      c_acctbal > 0.00
       |                      and substring(c_phone from 1 for 2) in
       |                        ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
       |          )
       |          and not exists (
       |                  select
       |                      *
       |                  from
       |                      orders
       |                  where
       |                      o_custkey = c_custkey
       |          )
       |      ) as custsale
       |group by
       |      cntrycode
       |order by
       |      cntrycode
     """.stripMargin

  def q22Tokens: QUERY_TYPE = Seq("substring(c_phone from 1 for 2)", "[I1]", "[I2]", "[I3]",
    "[I4]", "[I5]", "[I6]", "[I7]")


}

trait Adapter extends TPCHBase {
  protected val qNumPattern = "(\\d+)(.*)".r
  val viewName = "(?s)\\W+(create\\s+view\\s+)(\\w+)(.*?as)(.*)".r

  protected val defaults = Map(
    " 1 0 " -> "90",
    " 2 0 " -> "24",
    " 2 1 " -> "STEEL",
    " 2 2 " -> "ASIA",
    " 3 0 " -> "BUILDING",
    " 3 1 " -> "1995-03-15",
    " 4 0 " -> "1993-07-01",
    " 4 1 " -> "1993-07-01",
    " 5 0 " -> "ASIA",
    " 5 1 " -> "1994-01-01",
    " 5 2 " -> "1994-01-01",
    " 6 0 " -> "0.06",
    " 6 1 " -> "1994-01-01",
    " 6 2 " -> "1994-01-01",
    " 6 3 " -> "24",
    " 7 0 " -> "<<<string replace>>>",
    " 7 1 " -> "<<<string replace>>>",
    " 7 2 " -> "FRANCE",
    " 7 3 " -> "GERMANY",
    " 8 0 " -> "<<<string replace>>>",
    " 8 1 " -> "<<<string replace>>>",
    " 8 2 " -> "BRAZIL",
    " 8 3 " -> "AMERICA",
    " 8 4 " -> "ECONOMY ANODIZED STEEL",
    " 9 0 " -> "<<<string replace>>>",
    " 9 1 " -> "green",
    "10 0 " -> "1993-10-01",
    "10 1 " -> "1993-10-01",
    "11 0 " -> "GERMANY",
    "11 1 " -> "0.0001",
    "12 0 " -> "MAIL",
    "12 1 " -> "SHIP",
    "12 2 " -> "1994-01-01",
    "12 3 " -> "1994-01-01",
    "13 0 " -> "special",
    "13 1 " -> "requests",
    "14 0 " -> "1995-09-01",
    "14 1 " -> "1995-09-01",
    "15 0 " -> "_1",
    "15v 0 " -> "_1",
    "15v 1 " -> "1996-01-01",
    "15v 2 " -> "1996-01-01",
    "16 0 " -> "Brand#45",
    "16 1 " -> "MEDIUM POLISHED",
    "16 2 " -> "49",
    "16 3 " -> "14",
    "16 4 " -> "23",
    "16 5 " -> "45",
    "16 6 " -> "19",
    "16 7 " -> "3",
    "16 8 " -> "36",
    "16 9 " -> "9",
    "17 0 " -> "Brand#23",
    "17 1 " -> "SM PACK",
    "18 0 " -> "300",
    "19 0 " -> "Brand#12",
    "19 1 " -> "1",
    "19 2 " -> "Brand#23",
    "19 3 " -> "10",
    "19 4 " -> "Brand#34",
    "19 5 " -> "20",
    "20 0 " -> "khaki",
    "20 1 " -> "1994-01-01",
    "20 2 " -> "1994-01-01",
    "20 3 " -> "CANADA",
    "21 0 " -> "VIETNAM",
    "22 0 " -> "<<<<function override>>>>",
    "22 1 " -> "13",
    "22 2 " -> "31",
    "22 3 " -> "23",
    "22 4 " -> "29",
    "22 5 " -> "30",
    "22 6 " -> "18",
    "22 7 " -> "17",
    "" -> ""
  )

  def replace(qNum: String, tokens: QUERY_TYPE, queryStr: String, args: String*): String
}


trait DynamicQueryGetter extends TPCHBase {
  self: Adapter =>

  lazy val valAdapter: Adapter = self

  final def deriveFromTokens(qNum: String, queryStr: String,
      tokens: QUERY_TYPE, args: String*): String = {

    val newArgs = if (args.isEmpty) args else {
      tokens.zipWithIndex.sliding(2).flatMap(_.toList
      match {
        case (l, i) :: (r, _) :: Nil
          if l.indexOf("date '[DATE]'") >= 0 && r.indexOf("date '[DATE] '") >= 0 =>
          Seq(args(i), args(i))
        case (_, i) :: _ if i < args.length =>
          Seq(args(i))
        case _ => Nil
      }).toList
    }

    def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
      val maxLeftSize = left.map(_.length).max
      val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))(" ")
      val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))(" ")
      leftPadded.zip(rightPadded).map {
        case (l, r) => l + (" " * ((maxLeftSize - l.length) + 3)) + r
      }
    }

    if (newArgs.nonEmpty && tokens.length != newArgs.length) {
      val errorMessage = s"ERROR: Query $qNum has argument mismatch \n" +
          s" ${sideBySide(tokens, newArgs).mkString("\n")} "
      throw new Exception(errorMessage)
    }

    valAdapter.replace(qNum, tokens, queryStr, newArgs: _*)
  }

  /*
    import scala.language.experimental.macros
    def getFinalQueryString(qNum: Int, args: String*): String = macro TPCH.getQryStr_impl
  */

  def getFinalQueryString(qNum: String, args: String*): String = {
    import scala.reflect.runtime.universe._
    val _this = runtimeMirror(this.getClass.getClassLoader).reflect(this)
    val qry = _this.symbol.typeSignature.member(s"q${qNum}": TermName)
    val toks = _this.symbol.typeSignature.member(s"q${qNum}Tokens": TermName)

    val qstr = _this.reflectMethod(qry.asMethod).apply()
    val tokSeq = _this.reflectMethod(toks.asMethod).apply()

    deriveFromTokens(qNum, qstr.asInstanceOf[String], tokSeq.asInstanceOf[QUERY_TYPE], args: _*)
    /*
        val ret = _this.reflectMethod(fn.asMethod)(qNum,
          qstr.asInstanceOf[String], tokSeq.asInstanceOf[QUERY_TYPE],
          args)

        ret.asInstanceOf[String]
    */
  }

  /*
    def getFinalQueryString(qNum: Int, args: String*): String = {
      import scala.reflect.runtime.universe._
      import scala.tools.reflect.ToolBox

      val tb = runtimeMirror(this.getClass.getClassLoader).mkToolBox()
      val tokenTerm = typeOf[this.type].member(s"q${qNum}Tokens": TermName)
      val queryTerm = typeOf[this.type].member(s"q${qNum}": TermName)
      val deriveFromTokensTerm = typeOf[this.type].member("deriveFromTokens": TermName)
      val self = q"this"

      val ast =
        q"""
           $self.$deriveFromTokensTerm($qNum, $self.$tokenTerm, $self.$queryTerm, ..${args})
         """
      val xx = showCode(ast)

      // scalastyle:off println
      println(xx.toString)
  /*
      val compiled = tb.compile(tb.parse(s"""
           $self.deriveFromTokensTerm($qNum, $self.q${qNum}Tokens, $self.q$qNum, ${args})
         """))
  */

      val compiled = tb.compile(ast)
      compiled().asInstanceOf[String]
  /*
      val dd = tb.compile(
        q"""
           $deriveFromTokensTerm($qNum, $tokenTerm, $queryTerm, ..${args})
         """)
      dd().asInstanceOf[String]
  */
    }
  */

}

/*
object TPCH {
  import scala.reflect.macros.blackbox.Context
  def getQryStr_impl(c: Context)(qNum: c.Expr[Int], args: c.Expr[String]*): c.Expr[String] = {
    import c.universe._
    reify(s
             |      q${qNum}Tokens.zipWithIndex.foldLeft(q${qNum}) { case (src, (tok, i)) =>
             |        replace($qNum, i, src, tok, ${args.tail: _*})
             |      }
     """)
  }
}
*/
