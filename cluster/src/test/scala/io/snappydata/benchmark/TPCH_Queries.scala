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

package io.snappydata.benchmark

import org.apache.spark.Logging

object TPCH_Queries extends Logging {

  private var random = new scala.util.Random(42)

  def setRandomSeed(randomSeed: Integer = 42): Unit = {
    this.random = new scala.util.Random(randomSeed)
  }

  def getQuery(query: String, isDynamic: Boolean, isSnappy: Boolean): String = query match {
    case "1" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery1, TPCH_Queries.getQ1Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery1_Memsql, TPCH_Queries.getQ1Parameter(isDynamic))
      }
    case "2" =>
      createQuery(TPCH_Queries.getQuery2, TPCH_Queries.getQ2Parameter(isDynamic))
    case "3" =>
      createQuery(TPCH_Queries.getQuery3, TPCH_Queries.getQ3Parameter(isDynamic))
    case "4" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery4, TPCH_Queries.getQ4Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery4_Memsql, TPCH_Queries.getQ4Parameter(isDynamic))
      }
    case "5" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery5, TPCH_Queries.getQ5Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery5_Memsql, TPCH_Queries.getQ5Parameter(isDynamic))
      }
    case "6" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery6, TPCH_Queries.getQ6Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery6_Memsql, TPCH_Queries.getQ6Parameter(isDynamic))
      }
    case "7" =>
      createQuery(TPCH_Queries.getQuery7, TPCH_Queries.getQ7Parameter(isDynamic))
    case "8" =>
      createQuery(TPCH_Queries.getQuery8, TPCH_Queries.getQ8Parameter(isDynamic))
    case "9" =>
      createQuery(TPCH_Queries.getQuery9, TPCH_Queries.getQ9Parameter(isDynamic))
    case "10" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery10, TPCH_Queries.getQ10Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery10_Memsql, TPCH_Queries.getQ10Parameter(isDynamic))
      }
    case "11" =>
      createQuery(TPCH_Queries.getQuery11, TPCH_Queries.getQ11Parameter(isDynamic))
    case "12" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery12, TPCH_Queries.getQ12Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery12_Memsql, TPCH_Queries.getQ12Parameter(isDynamic))
      }
    case "13" =>
      createQuery(TPCH_Queries.getQuery13, TPCH_Queries.getQ13Parameter(isDynamic))
    case "14" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery14, TPCH_Queries.getQ14Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery14_Memsql, TPCH_Queries.getQ14Parameter(isDynamic))
      }
    case "15" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery15_Temp, TPCH_Queries.getQ15TempParameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery15_Temp_Memsql,
          TPCH_Queries.getQ15TempParameter(isDynamic))
      }
    case "16" =>
      createQuery(TPCH_Queries.getQuery16, TPCH_Queries.getQ16Parameter(isDynamic))
    case "17" =>
      createQuery(TPCH_Queries.getQuery17, TPCH_Queries.getQ17Parameter(isDynamic))
    case "18" =>
      createQuery(TPCH_Queries.getQuery18, TPCH_Queries.getQ18Parameter(isDynamic))
    case "19" =>
      createQuery(TPCH_Queries.getQuery19, TPCH_Queries.getQ19Parameter(isDynamic))
    case "20" =>
      if (isSnappy) {
        createQuery(TPCH_Queries.getQuery20, TPCH_Queries.getQ20Parameter(isDynamic))
      } else {
        createQuery(TPCH_Queries.getQuery20_Memsql, TPCH_Queries.getQ20Parameter(isDynamic))
      }
    case "21" =>
      createQuery(TPCH_Queries.getQuery21, TPCH_Queries.getQ21Parameter(isDynamic))
    case "22" =>
      createQuery(TPCH_Queries.getQuery22, TPCH_Queries.getQ22Parameter(isDynamic))
  }

  def createQuery(query: String, paramters: Array[String]): String = {
    var generatedQuery = query
    for (s <- paramters) {
      logInfo(s"KBKBKB : createQuery : $s")
      generatedQuery = generatedQuery.replaceFirst("\\?", s)
    }
    logInfo(s"KBKBKB : My query : $generatedQuery")
    generatedQuery
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
        "     l_shipdate <= DATE_SUB('1997-12-31', ? )" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"

  }

  def getQuery1_Memsql: String = {
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
        "     l_shipdate <= '1997-12-31' - interval '?' day" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"
  }


  def getQ1Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      val min = 60
      val max = 120
      Array(s"${min + random.nextInt((max - min) + 1)}")
    } else {
      Array("90")
    }
  }


  def getResultString1: String = {
    "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price" +
        "|avg_disc|count_order"
  }

  def getQuery2: String = {
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
        "     and R_NAME = '?'" +
        "     and S_SUPPKEY = PS_SUPPKEY" +
        "     and P_PARTKEY = PS_PARTKEY" +
        "     and P_SIZE = ?" +
        "     and P_TYPE like '%?'" +
        "     and PS_SUPPLYCOST = (" +
        "         select" +
        "             min(PS_SUPPLYCOST)" +
        "         from" +
        "             SUPPLIER, NATION," +
        "             REGION, PARTSUPP" +
        "         where" +
        "             S_NATIONKEY = N_NATIONKEY" +
        "             and N_REGIONKEY = R_REGIONKEY" +
        "             and R_NAME = '?'" +
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


  def getQ2Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      // size 1 to 50
      // type TIN NICKEL BRASS STEEL COPPER
      // region AFRICA AMERICA ASIA EUROPE MIDDLE EAST
      val min = 1
      val max = 50
      val size = s"${min + random.nextInt((max - min) + 1)}"
      val syllable3 = Array("TIN", "NICKEL", "BRASS", "STEEL", "COPPER")
      val syllableIndex = random.nextInt(syllable3.length)
      val syllableType = syllable3(syllableIndex)
      val regions = Array("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
      val regionIndex = random.nextInt(regions.length)
      val region = regions(regionIndex)
      Array(region, size, syllableType, region)
    }
    else {
      Array("ASIA", "24", "STEEL", "ASIA")
    }
  }

  def getResultString2: String = {
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
        "     C_MKTSEGMENT = '?'" +
        "     and C_CUSTKEY = o_custkey" +
        "     and l_orderkey = o_orderkey" +
        "      and o_orderdate < '?'" +
        "     and l_shipdate > '?' " +
        " group by" +
        "     l_orderkey," +
        "     o_orderdate," +
        "     o_shippriority" +
        " order by" +
        "     l_orderkey" +
        " limit 10"
  }

  def getQ3Parameter(isDynamic: Boolean): Array[String] = {
    // segment AUTOMOBILE BUILDING FURNITURE MACHINERY HOUSEHOLD
    // date1  randomly selected day within [1995-03-01 .. 1995-03-31]
    if (isDynamic) {
      val segments = Array("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD")
      val segmentIndex = random.nextInt(segments.length)
      val segment = segments(segmentIndex)

      val fromDate = java.time.LocalDate.of(1995, 3, 1)
      val toDate = java.time.LocalDate.of(1995, 3, 31)
      val diff = java.time.temporal.ChronoUnit.DAYS.between(fromDate, toDate)
      // val random = new random(System.nanoTime)
      // You may want a different seed
      val selectedDate = fromDate.plusDays(random.nextInt(diff.toInt))
      Array(segment, selectedDate.toString, selectedDate.toString)
    } else {
      Array("BUILDING", "1995-03-15", "1995-03-15")
    }
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
        "     o_orderdate >= '?'" +
        "     and o_orderdate < add_months('?',3)" +
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

  def getQuery4_Memsql: String = {
    // 1.DATE = 1993-07-01.
    " select" +
        "     o_orderpriority," +
        "     count(*) as order_count" +
        " from" +
        "     ORDERS" +
        " where" +
        "     o_orderdate >= '?'" +
        "     and o_orderdate < '?' + interval '3' month" +
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

  def getQ4Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* DATE is the first day of a randomly selected month between the first
    month of 1993 and the 10th month of 1997. */
      val min = 1
      val max = 10
      val month = {
        min + random.nextInt((max - min) + 1)
      }

      val minYear = 1993
      val maxYear = 1997
      val year = {
        minYear + random.nextInt((maxYear - minYear) + 1)
      }
      val date = java.time.LocalDate.of(year, month, 1)
      Array(date.toString, date.toString)
    } else {
      Array("1993-07-01", "1993-07-01")
    }
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
        "        and r_name = '?'" +
        "        and C_CUSTKEY = o_custkey" +
        "        and l_orderkey = o_orderkey" +
        "        and l_suppkey = s_suppkey" +
        "        and C_NATIONKEY = s_nationkey" +
        "        and o_orderdate >= '?'" +
        "        and o_orderdate < add_months('?', 12)" +
        " group by" +
        "        n_name" +
        " order by" +
        "        revenue desc"
  }

  def getQuery5_Memsql: String = {
    // 1. REGION = ASIA;
    // 2. DATE = 1994-01-01.
    " select" +
        "        N_NAME," +
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
        "        and r_name = '?'" +
        "        and C_CUSTKEY = o_custkey" +
        "        and l_orderkey = o_orderkey" +
        "        and l_suppkey = s_suppkey" +
        "        and C_NATIONKEY = s_nationkey" +
        "        and o_orderdate >= '?'" +
        "        and o_orderdate < '?' + interval '1' year" +
        " group by" +
        "        N_NAME" +
        " order by" +
        "        revenue desc"
  }

  def getQ5Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. REGION is randomly selected within the list of values defined for R_NAME
     in C;aise 4.2.3;
    2. DATE is the first of January of a randomly selected year within [1993 .. 1997] */

      val regions = Array("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
      val regionIndex = random.nextInt(regions.length)
      val region = regions(regionIndex)

      val minYear = 1993
      val maxYear = 1997
      val year = {
        minYear + random.nextInt((maxYear - minYear) + 1)
      }

      val date = java.time.LocalDate.of(year, 1, 1)
      Array(region, date.toString, date.toString)
    } else {
      Array("ASIA", "1994-01-01", "1994-01-01")
    }
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
        "        l_shipdate >= '?'" +
        "        and l_shipdate < add_months('?', 12)" +
        "        and l_discount between ? - 0.01 and ? + 0.01" +
        "        and l_quantity < ?"
  }

  def getQuery6_Memsql: String = {
    // 1. DATE = 1994-01-01;
    // 2. DISCOUNT = 0.06;
    // 3. QUANTITY = 24.
    " select" +
        "        sum(l_extendedprice*l_discount) as revenue" +
        " from" +
        "        LINEITEM" +
        " where" +
        "        l_shipdate >= '?'" +
        "        and l_shipdate < '?' + interval '1' year" +
        "        and l_discount between ? - 0.01 and ? + 0.01" +
        "        and l_quantity < ?"
  }

  def getQ6Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. DATE is the first of January of a randomly selected year within [1993 .. 1997];
    2. DISCOUNT is randomly selected within [0.02 .. 0.09];
    3. QUANTITY is randomly selected within [24 .. 25]. */

      val minYear = 1993
      val maxYear = 1997
      val year = {
        minYear + random.nextInt((maxYear - minYear) + 1)
      }
      val date = java.time.LocalDate.of(year, 1, 1)

      val discounts = Array("0.02", "0.03", "0.04", "0.05", "0.06", "0.07", "0.08", "0.09")
      val discountIndex = random.nextInt(discounts.length)
      val discount = discounts(discountIndex)

      val minQuantity = 24
      val maxQuantity = 25
      val quantity = s"${minQuantity + random.nextInt((maxQuantity - minQuantity) + 1)}"

      Array(date.toString, date.toString, discount, discount, quantity)
    } else {
      Array("1994-01-01", "1994-01-01", "0.06", "0.06", "24")
    }
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
        "                         (n1.n_name = '?' and n2.n_name = '?')" +
        "                      or (n1.n_name = '?' and n2.n_name = '?')" +
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

  def getQ7Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. NATION1 is randomly selected within the list of values defined for N_NAME in Clause
         4.2.3;
     2. NATION2 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3 and
     must be different from the value selected for NATION1 in item 1 above. */

      val nations = Array("ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
        "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM",
        "RUSSIA", "UNITED KINGDOM", "UNITED STATES")

      val nation1Index = random.nextInt(nations.length)
      val nation1 = nations(nation1Index)

      val nation2Index = if (nation1Index > (nations.length / 2)) nation1Index - 1
      else nation1Index + 1
      val nation2 = nations(nation2Index)

      Array(nation1, nation2, nation2, nation1)
    } else {
      Array("FRANCE", "GERMANY", "GERMANY", "FRANCE")
    }
  }

  def getResultString7: String = {
    "supp_nation|cust_nation|l_year|revenue"
  }

  def getQuery8: String = {
    //    1. NATION = BRAZIL;
    //    2. REGION = AMERICA;
    //    3. TYPE = ECONOMY ANODIZED STEEL.
    "select" +
        "         o_year," +
        "         sum(case" +
        "                 when nation = '?'" +
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
        "                         and r_name = '?'" +
        "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
        "                         and p_type = '?'" +
        "                         and s_suppkey = l_suppkey" +
        "                         and s_nationkey = n2.n_nationkey" +
        "         ) as all_nations" +
        " group by" +
        "         o_year" +
        " order by" +
        "         o_year"

  }

  def getQ8Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /*
     1. NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
     2. REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to
     N_REGIONKEY for the selected NATION in item 1 above;
     3. TYPE is randomly selected within the list of 3-syllable strings defined for Types in
     Clause 4.2.2.13. */

      val nationsMap: Map[String, Array[String]] =
        Map("AFRICA" -> Array("ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE"),
          "AMERICA" -> Array("ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED STATES"),
          "ASIA" -> Array("INDIA", "INDONESIA", "JAPAN", "CHINA", "VIETNAM"),
          "EUROPE" -> Array("FRANCE", "GERMANY", "ROMANIA", "RUSSIA", "UNITED KINGDOM"),
          "MIDDLE EAST" -> Array("EGYPT", "IRAN", "IRAQ", "JORDAN", "SAUDI ARABIA"))

      val regionKeys = nationsMap.keySet.toArray
      val regionIndex = random.nextInt(regionKeys.length)
      val region = regionKeys(regionIndex)

      val nations: Array[String] = nationsMap(region)
      val nationIndex = random.nextInt(nations.length)
      val nation = nations(nationIndex)

      val syllables1 = Array("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO")
      val syllables2 = Array("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED")
      val syllables3 = Array("TIN", "NICKEL", "BRASS", "STEEL", "COPPER")

      val syllable1Index = random.nextInt(syllables1.length)
      val syllable1 = syllables1(syllable1Index)

      val syllable2Index = random.nextInt(syllables2.length)
      val syllable2 = syllables2(syllable2Index)

      val syllable3Index = random.nextInt(syllables3.length)
      val syllable3 = syllables3(syllable3Index)

      val pType = s"$syllable1 $syllable2 $syllable3"

      Array(nation, region, pType)
    } else {
      Array("BRAZIL", "AMERICA", "ECONOMY ANODIZED STEEL")
    }
  }

  def getResultString8: String = {
    "YEAR|MKT_SHARE"
  }

  def getQuery9: String = {
    // 1. COLOR = green.
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
        "                 and p_name like '%?%'" +
        "         ) as profit" +
        " group by" +
        "         nation," +
        "         o_year" +
        " order by" +
        "         nation," +
        "         o_year desc"

  }

  def getQ9Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* COLOR is randomly selected within the list of values defined for the generation
       of P_NAME in Clause 4.2.3. */

      val pnames = Array("almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
        "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
        "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark",
        "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted",
        "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian",
        "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
        "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin",
        "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru",
        "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle",
        "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring",
        "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow")

      val pnameIndex = random.nextInt(pnames.length)
      val pname = pnames(pnameIndex)

      Array(pname)
    } else {
      Array("green")
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
        "         and o_orderdate >= '?'" +
        "         and o_orderdate < add_months('?', 3)" +
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

  def getQuery10_Memsql: String = {
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
        "         and o_orderdate >= '?'" +
        "         and o_orderdate < '?' + interval '3' month" +
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
        "         revenue desc" +
        " limit 20"

  }

  def getQuery10_ForPrepareStatement: String = {
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

  def getQ10Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* DATE is the first day of a randomly selected month from the second
    month of 1993 to the first month of 1995. */
      val min = 2
      val max = 12
      val month = min + random.nextInt((max - min) + 1)

      val minYear = 1993
      val maxYear = 1994
      val year = minYear + random.nextInt((maxYear - minYear) + 1)


      val date = java.time.LocalDate.of(year, month, 1)
      Array(date.toString, date.toString)
    } else {
      Array("1993-10-01", "1993-10-01")
    }
  }

  def getResultString10: String = {
    "C_CUSTKEY|C_NAME|REVENUE|C_ACCTBAL|N_NAME|C_ADDRESS|C_PHONE|C_COMMENT"
  }

  def getQuery11: String = {
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
        "         and N_NAME = '?'" +
        " group by" +
        "         PS_PARTKEY having" +
        "         sum(PS_SUPPLYCOST * PS_AVAILQTY) > (" +
        "                 select" +
        "                         sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0000001" +
        "                 from" +
        "                         SUPPLIER," +
        "                         NATION," +
        "                         PARTSUPP" +
        "                 where" +
        "                         PS_SUPPKEY = S_SUPPKEY" +
        "                         and S_NATIONKEY = N_NATIONKEY" +
        "                         and N_NAME = '?'" +
        "         )" +
        " order by" +
        "         value desc"
  }

  def getQ11Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. NATION is randomly selected within the list of values defined for N_NAME
      in Clause 4.2.3;
     2. FRACTION is chosen as 0.0001 / SF. */

      val nations = Array("ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
        "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM",
        "RUSSIA", "UNITED KINGDOM", "UNITED STATES")

      val nation1Index = random.nextInt(nations.length)
      val nation = nations(nation1Index)

      Array(nation, nation)
    } else {
      Array("GERMANY", "GERMANY")
    }
  }

  def getResultString11: String = {
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
        "         and l_shipmode in ('?', '?')" +
        "         and l_commitdate < l_receiptdate" +
        "         and l_shipdate < l_commitdate" +
        "         and l_receiptdate >= '?'" +
        "         and l_receiptdate < add_months('?',12)" +
        " group by" +
        "         l_shipmode" +
        " order by" +
        "         l_shipmode"
  }

  def getQuery12_Memsql: String = {
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
        "         and l_shipmode in ('?', '?')" +
        "         and l_commitdate < l_receiptdate" +
        "         and l_shipdate < l_commitdate" +
        "         and l_receiptdate >= '?'" +
        "         and l_receiptdate < '?' + interval '1' year" +
        " group by" +
        "         l_shipmode" +
        " order by" +
        "         l_shipmode"

  }

  def getQ12Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1.SHIPMODE1 is randomly selected within the list of values defined for Modes in
        Clause 4.2.2.13;
        2. SHIPMODE2 is randomly selected within the list of values defined for Modes in
        Clause 4.2.2.13 and must be different from the value selected for SHIPMODE1 in item 1;
        3. DATE is the first of January of a randomly selected year within [1993 .. 1997]. */

      val shipmodes = Array("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB")

      val shipmode1Index = random.nextInt(shipmodes.length)
      val shipmode1 = shipmodes(shipmode1Index)

      val shipmode2Index = if (shipmode1Index > (shipmodes.length / 2)) shipmode1Index - 1
      else shipmode1Index + 1
      val shipmode2 = shipmodes(shipmode2Index)

      val minYear = 1993
      val maxYear = 1997
      val year = {
        minYear + random.nextInt((maxYear - minYear) + 1)
      }
      val date = java.time.LocalDate.of(year, 1, 1)

      Array(shipmode1, shipmode2, date.toString, date.toString)
    } else {
      Array("MAIL", "SHIP", "1994-01-01", "1994-01-01")
    }

  }

  def getResultString12: String = {
    "L_SHIPMODE|HIGH_LINE_COUNT|LOW_LINE_COUNT"
  }

  def getQuery13: String = {
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
        "                 and o_comment not like '%?%?%'" +
        "         group by" +
        "                 C_CUSTKEY" +
        "         ) as c_orders" +
        " group by" +
        "         c_count" +
        " order by" +
        "         custdist desc," +
        "         c_count desc"
  }

  def getQ13Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. WORD1 is randomly selected from 4 possible values: special, pending, unusual, express.
         2. WORD2 is randomly selected from 4 possible values: packages, requests, accounts,
         deposits */

      val words1 = Array("special", "pending", "unusual", "express")
      val words2 = Array("packages", "requests", "accounts", "deposits")

      val word1Index = random.nextInt(words1.length)
      val word1 = words1(word1Index)

      val word2Index = random.nextInt(words2.length)
      val word2 = words2(word2Index)

      Array(word1, word2)
    } else {
      Array("special", "requests")
    }

  }

  def getResultString13: String = {
    "C_COUNT|CUSTDIST"
  }

  def getQuery14: String = {
    // 1.DATE = 1995-09-01.
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
        "         and l_shipdate >= '?'" +
        "         and l_shipdate < add_months ('?', 1)"
  }

  def getQuery14_Memsql: String = {
    // 1.DATE = 1995-09-01.
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
        "         and l_shipdate < '1995-09-01'+ interval '1' month"

  }

  def getQ14Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1.DATE is the first day of a month randomly selected from a random year within
         [1993 .. 1997].. */

      val minYear = 1993
      val maxYear = 1997
      val year = {
        minYear + random.nextInt((maxYear - minYear) + 1)
      }
      val date = java.time.LocalDate.of(year, 1, 1)

      Array(date.toString, date.toString)
    } else {
      Array("1995-09-01", "1995-09-01")
    }
  }

  def getResultString14: String = {
    "PROMO_REVENUE"
  }

  def getTempQuery15_Original: String = {
    "create temporary view " +
        "        revenue as" +
        " select" +
        "      l_suppkey as supplier_no ," +
        "      sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
        " from" +
        "      LINEITEM" +
        " where" +
        "      l_shipdate >= '?'" +
        "      and l_shipdate < '?' + interval '3' month" +
        " group by" +
        "      l_suppkey"
  }

  def getQuery15_Original: String = {
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


  def getQuery15_Temp: String = {
    "select" +
        "         l_suppkey as supplier_no," +
        "         sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
        " from" +
        "         LINEITEM" +
        " where" +
        "         l_shipdate >= '?'" +
        "         and l_shipdate < add_months('?',3) " +
        " group by" +
        "         l_suppkey"
  }

  def getQuery15_Temp_Memsql: String = {
    "create view " +
        "        revenue as" +
        " select" +
        "      l_suppkey as supplier_no ," +
        "      sum(l_extendedprice * (1 - l_discount)) as total_revenue" +
        " from" +
        "      LINEITEM" +
        " where" +
        "      l_shipdate >= '?'" +
        "      and l_shipdate < '?' + interval '3' month" +
        " group by" +
        "      l_suppkey"
  }

  def getQ15TempParameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* DATE is the first day of a randomly selected month between the first month of 1993
         and the 10th month of 1997. */

      val min = 2
      val max = 10
      val month = min + random.nextInt((max - min) + 1)

      val minYear = 1993
      val maxYear = 1997
      val year = minYear + random.nextInt((maxYear - minYear) + 1)


      val date = java.time.LocalDate.of(year, month, 1)
      Array(date.toString, date.toString)
    } else {
      Array("1993-02-01", "1996-01-01")
    }

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
        "         and floor(total_revenue) = (" +
        "                       select" +
        "                             floor(max(total_revenue))" +
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
        "         and p_brand <> 'Brand#??'" +
        "         and p_type not like '?%'" +
        "         and p_size in (?, ?, ?, ?, ?, ?, ?, ?)" +
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

  def getQ16Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. BRAND = Brand#MN where M and N are two single character strings representing
        two numbers randomly and independently selected within [1 .. 5];
        2. TYPE is made of the first 2 syllables of a string randomly selected within the
         list of 3-syllable strings defined for Types in Clause 4.2.2.13;
        3. SIZE1 is randomly selected as a set of eight different values within [1 .. 50];
        4. SIZE2 is randomly selected as a set of eight different values within [1 .. 50];
        5. SIZE3 is randomly selected as a set of eight different values within [1 .. 50];
        6. SIZE4 is randomly selected as a set of eight different values within [1 .. 50];
        7. SIZE5 is randomly selected as a set of eight different values within [1 .. 50];
        8. SIZE6 is randomly selected as a set of eight different values within [1 .. 50];
        9. SIZE7 is randomly selected as a set of eight different values within [1 .. 50];
        10. SIZE8 is randomly selected as a set of eight different values within [1 .. 50] */

      val brands = Array("1", "2", "3", "4", "5")
      val mIndex = random.nextInt(brands.length)
      val nIndex = random.nextInt(brands.length)
      val m = brands(mIndex)
      val n = brands(nIndex)

      val syllables1 = Array("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO")
      val syllables2 = Array("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED")
      val syllable1Index = random.nextInt(syllables1.length)
      val syllable1 = syllables1(syllable1Index)
      val syllable2Index = random.nextInt(syllables2.length)
      val syllable2 = syllables2(syllable2Index)
      val pType = s"$syllable1 $syllable2"

      val min = 1
      val max = 50
      val size1 = (min + random.nextInt(max - min)).toString
      val size2 = (min + random.nextInt(max - min)).toString
      val size3 = (min + random.nextInt(max - min)).toString
      val size4 = (min + random.nextInt(max - min)).toString
      val size5 = (min + random.nextInt(max - min)).toString
      val size6 = (min + random.nextInt(max - min)).toString
      val size7 = (min + random.nextInt(max - min)).toString
      val size8 = (min + random.nextInt(max - min)).toString

      Array(m, n, pType, size1, size2, size3, size4, size5, size6, size7, size8)
    } else {
      Array("4", "5", "MEDIUM POLISHED", "49", "14", "23", "45", "19", "3", "36", "9")
    }
  }

  def getResultString16: String = {
    "P_BRAND|P_TYPE|P_SIZE|SUPPLIER_CNT"
  }

  def getQuery17: String = {
    //    1. BRAND = Brand#23;
    //    2. CONTAINER = MED BOX.
    "select" +
        "         sum(l_extendedprice) / 7.0 as avg_yearly" +
        " from" +
        "         LINEITEM," +
        "         PART" +
        " where" +
        "         P_PARTKEY = l_partkey" +
        "         and P_BRAND = 'Brand#??'" +
        "         and P_CONTAINER = '?'" +
        "         and l_quantity < (" +
        "                 select" +
        "                         0.2 * avg(l_quantity)" +
        "                 from" +
        "                         LINEITEM" +
        "                 where" +
        "                         l_partkey = P_PARTKEY" +
        "         )"
  }

  def getQ17Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. BRAND = 'Brand#MN' where MN is a two character string representing two numbers randomly
       and independently selected within [1 .. 5];
        2. CONTAINER is randomly selected within the list of 2-syllable strings defined for
        Containers in Clause 4.2.2.13. */

      val brands = Array("1", "2", "3", "4", "5")
      val mIndex = random.nextInt(brands.length)
      val nIndex = random.nextInt(brands.length)
      val m = brands(mIndex)
      val n = brands(nIndex)

      val syllables1 = Array("SM", "LG", "MED", "JUMBO", "WRAP")
      val syllables2 = Array("CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM")
      val syllable1Index = random.nextInt(syllables1.length)
      val syllable1 = syllables1(syllable1Index)
      val syllable2Index = random.nextInt(syllables2.length)
      val syllable2 = syllables2(syllable2Index)
      val pContainer = s"$syllable1 $syllable2"
      Array(m, n, pContainer)
    } else {
      Array("2", "3", "SM PACK")
    }
  }

  def getResultString17: String = {
    "AVG_YEARLY"
  }

  /* def getQuery18: String = {
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
        "            l_orderkey having sum(l_quantity) > ?" +
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
  } */

  def getQuery18: String = {
    // 1.QUANTITY = 300
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
        "                         sum(l_quantity) > ?" +
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
        "         o_orderdate" +
        " limit 100"
  }

  def getQ18Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* QUANTITY is randomly selected within [312..315] */

      val min = 312
      val max = 315
      val quantity = min + random.nextInt(max - min)
      Array(quantity.toString)
    } else {
      Array("300")
    }
  }

  def getResultString18: String = {
    "C_NAME|C_CUSTKEY|O_ORDERKEY|O_ORDERDATE|O_TOTALPRICE|Sum(L_QUANTITY)"
  }

  def getQuery19: String = {
    //    1. QUANTITY1 = 1.
    //    2. QUANTITY2 = 10.
    //    3. QUANTITY3 = 20.
    //    4. BRAND1 = Brand#12.
    //    5. BRAND2 = Brand#23.
    //    6. BRAND3 = Brand#34.
    "select" +
        "         sum(l_extendedprice * (1 - l_discount) ) as revenue" +
        " from" +
        "         LINEITEM," +
        "         PART" +
        " where" +
        "         (" +
        "                 p_partkey = l_partkey" +
        "                 and p_brand = 'Brand#?'" +
        "                 and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')" +
        "                 and l_quantity >= ? and l_quantity <= ? + 10" +
        "                 and p_size between 1 and 5" +
        "                 and l_shipmode in ('AIR', 'AIR REG')" +
        "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
        "         )" +
        "         or" +
        "         (" +
        "                 p_partkey = l_partkey" +
        "                 and p_brand = 'Brand#?'" +
        "                 and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')" +
        "                 and l_quantity >= ? and l_quantity <= ? + 10" +
        "                 and p_size between 1 and 10" +
        "                 and l_shipmode in ('AIR', 'AIR REG')" +
        "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
        "         )" +
        "         or" +
        "         (" +
        "                 p_partkey = l_partkey" +
        "                 and p_brand = 'Brand#?'" +
        "                 and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')" +
        "                 and l_quantity >= ? and l_quantity <= ? + 10" +
        "                 and p_size between 1 and 15" +
        "                 and l_shipmode in ('AIR', 'AIR REG')" +
        "                 and l_shipinstruct = 'DELIVER IN PERSON'" +
        "         )"

  }

  def getQ19Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. QUANTITY1 is randomly selected within [1..10].
         2. QUANTITY2 is randomly selected within [10..20].
         3. QUANTITY3 is randomly selected within [20..30].
         4. BRAND1, BRAND2, BRAND3 = 'Brand#MN' where each MN is a two character string
             representing two num- bers randomly and independently selected within [1 .. 5] */

      var min = 1
      var max = 10
      val quantity1 = (min + random.nextInt(max - min)).toString
      min = 10
      max = 20
      val quantity2 = (min + random.nextInt(max - min)).toString
      min = 20
      max = 30
      val quantity3 = (min + random.nextInt(max - min)).toString

      val brands = Array("1", "2", "3", "4", "5")
      var mIndex = random.nextInt(brands.length)
      var nIndex = random.nextInt(brands.length)
      var m = brands(mIndex)
      var n = brands(nIndex)
      val mn1 = m + n

      mIndex = random.nextInt(brands.length)
      nIndex = random.nextInt(brands.length)
      m = brands(mIndex)
      n = brands(nIndex)
      val mn2 = m + n

      mIndex = random.nextInt(brands.length)
      nIndex = random.nextInt(brands.length)
      m = brands(mIndex)
      n = brands(nIndex)
      val mn3 = m + n

      Array(mn1, quantity1, quantity1, mn2, quantity2, quantity2, mn3, quantity3, quantity3)
    }
    else {
      Array("12", "1", "1", "23", "10", "10", "34", "20", "20")
    }
  }


  def getResultString19: String = {
    "REVENUE"
  }

  def getQuery20: String = {
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
        "                                         P_NAME like '?%'" +
        "                         )" +
        "                         and PS_AVAILQTY > (" +
        "                                 select" +
        "                                         0.5 * sum(l_quantity)" +
        "                                 from" +
        "                                         LINEITEM" +
        "                                 where" +
        "                                         l_partkey = PS_PARTKEY" +
        "                                         and l_suppkey = PS_SUPPKEY" +
        "                                         and l_shipdate >= '?'" +
        "                                         and l_shipdate < add_months('?', 12)" +
        "                         )" +
        "         )" +
        "         and S_NATIONKEY = N_NATIONKEY" +
        "         and N_NAME = '?'" +
        " order by" +
        "         S_NAME"
  }

  def getQuery20_Memsql: String = {
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
        "                                         P_NAME like '?%'" +
        "                         )" +
        "                         and PS_AVAILQTY > (" +
        "                                 select" +
        "                                         0.5 * sum(l_quantity)" +
        "                                 from" +
        "                                         LINEITEM" +
        "                                 where" +
        "                                         l_partkey = PS_PARTKEY" +
        "                                         and l_suppkey = PS_SUPPKEY" +
        "                                         and l_shipdate >= '?'" +
        "                                         and l_shipdate < '?' + interval 1 year" +
        "                         )" +
        "         )" +
        "         and S_NATIONKEY = N_NATIONKEY" +
        "         and N_NAME = '?'" +
        " order by" +
        "         S_NAME"

  }

  def getQ20Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* 1. COLOR is randomly selected within the list of values defined for the generation
      of P_NAME.
      2. DATE is the first of January of a randomly selected year within 1993..1997.
      3. NATION is randomly selected within the list of values defined for N_NAME in Clause4.2.3 */
      val pnames = Array("almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
        "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
        "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark",
        "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted",
        "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian",
        "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
        "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin",
        "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru",
        "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle",
        "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring",
        "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow")

      val colorIndex = random.nextInt(pnames.length)
      val color = pnames(colorIndex)

      val minYear = 1993
      val maxYear = 1997
      val year = minYear + random.nextInt((maxYear - minYear) + 1)

      val date = java.time.LocalDate.of(year, 1, 1)

      val nations = Array("ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
        "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM",
        "RUSSIA", "UNITED KINGDOM", "UNITED STATES")

      val nationIndex = random.nextInt(nations.length)
      val nation = nations(nationIndex)

      Array(color, date.toString, date.toString, nation)
    } else {
      Array("khaki", "1994-01-01", "1994-01-01", "CANADA")
    }
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
        "         and n_name = '?'" +
        " group by" +
        "         s_name" +
        " order by" +
        "         numwait desc," +
        "         s_name limit 100"
  }

  def getQ21Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3 */

      val nations = Array("ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
        "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM",
        "RUSSIA", "UNITED KINGDOM", "UNITED STATES")

      val nationIndex = random.nextInt(nations.length)
      val nation = nations(nationIndex)

      Array(nation)
    } else {
      Array("VIETNAM")
    }

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

  def getQuery22: String = {
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
        "                         ('?','?','?','?','?','?','?')" +
        "                 and C_ACCTBAL > (" +
        "                         select" +
        "                                 avg(C_ACCTBAL)" +
        "                         from" +
        "                                 CUSTOMER" +
        "                         where" +
        "                                 C_ACCTBAL > 0.00" +
        "                                 and SUBSTR(C_PHONE,1,2) in" +
        "                                         ('?','?','?','?','?','?','?')" +
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

  def getQ22Parameter(isDynamic: Boolean): Array[String] = {
    if (isDynamic) {
      /* I1 ... I7 are randomly selected without repetition from the possible values for
        Country code as defined in Clause 4.2.2.9. */

      val countryCodes: Array[String] = new Array[String](7)
      var x: Int = 0
      do {
        val min = 1
        val max = 25
        val code = (10 + (min + random.nextInt(max - min))).toString
        if (x == 0) {
          countryCodes(x) = code
          x = x + 1
        } else {
          if (!countryCodes.contains(code)) {
            countryCodes(x) = code
            x = x + 1
          }
        }
      } while (x < 7)

      countryCodes ++ countryCodes
    } else {
      Array("13", "31", "23", "29", "30", "18", "17", "13", "31", "23", "29", "30", "18", "17")
    }
  }

  def getResultString22: String = {
    "CNTRYCODE|NUMCUST|TOTACCTBAL"
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
}
