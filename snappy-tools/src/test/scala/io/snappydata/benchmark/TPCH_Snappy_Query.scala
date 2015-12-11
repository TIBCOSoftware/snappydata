package io.snappydata.benchmark

import java.io.{PrintStream, File, FileOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext

/**
 * Created by kishor on 27/10/15.
 */
object TPCH_Snappy_Query {

  def execute(queryNumber: String, sc: SparkContext): Unit = {
    var snappyContext = SnappyContext(sc)

    var fOutStream = new FileOutputStream(new File(s"$queryNumber.txt"));
    var ps = new PrintStream(fOutStream);

    var query = queryNumber match {
      case "Q" => getQuery()
      case "Q1" => getQuery1()
      case "Q2" => getQuery2()
      case "Q3" => getQuery3()
      case "Q4" => getQuery4()
      case "Q5" => getQuery5()
      case "Q6" => getQuery6()
      case "Q7" => getQuery7()
      case "Q8" => getQuery8()
      case "Q9" => getQuery9()
      case "Q10" => getQuery10()
      case "Q11" => getQuery11()
      case "Q12" => getQuery12()
      case "Q13" => getQuery13()
      case "Q14" => getQuery14()
      case "Q15" => getQuery15()
      case "Q16" => getQuery16()
      case "Q17" => getQuery17()
      case "Q18" => getQuery18()
      case "Q19" => getQuery19()
      case "Q20" => getQuery20()
      case "Q21" => getQuery21()
      case "Q22" => getQuery22()
    }

    var resultFormat = queryNumber match {
      case "Q" => getResultString()
      case "Q1" => getResultString1()
      case "Q2" => getResultString2()
      case "Q3" => getResultString3()
      case "Q4" => getResultString4()
      case "Q5" => getResultString5()
      case "Q6" => getResultString6()
      case "Q7" => getResultString7()
      case "Q8" => getResultString8()
      case "Q9" => getResultString9()
      case "Q10" => getResultString10()
      case "Q11" => getResultString11()
      case "Q12" => getResultString12()
      case "Q13" => getResultString13()
      case "Q14" => getResultString14()
      case "Q15" => getResultString15()
      case "Q16" => getResultString16()
      case "Q17" => getResultString17()
      case "Q18" => getResultString18()
      case "Q19" => getResultString19()
      case "Q20" => getResultString20()
      case "Q21" => getResultString21()
      case "Q22" => getResultString22()
    }

    try {
      var cnts = snappyContext.sql(query).collect()
      println(s"$queryNumber Executed . Result Count : ${cnts.length}")

      ps.println(s"$resultFormat")
      for (s <- cnts) {
        ps.println(s.toString())
      }
      println(s"$queryNumber Result Collected in file $queryNumber.txt")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        e.printStackTrace(ps)
        println(s" Exception while executing $queryNumber in written to file $queryNumber.txt")
      }
    } finally {
      ps.close()
      fOutStream.close()
    }
  }

  def getQuery(): String = {
    //"select N_NAME from NATION where N_REGIONKEY = (select R_REGIONKEY from REGION where R_NAME='ASIA')"
        "select" +
        "     o_orderpriority," +
        "     count(*) as order_count" +
        " from" +
        "     ORDERS" +
        " where" +
        "     o_orderdate >= '1993-07-01'" +
        "     and o_orderdate < DATE_ADD('1998-12-01',90)"+
        //"     and o_orderdate < '1998-12-01' + 3 MONTHS "+
        " group by"+
        "    o_orderpriority"+
        " order by"+
        "    o_orderpriority"
  }

  def getResultString():String = {
    ""
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
        "     l_shipdate <= DATE_SUB('1998-12-01',60)" +
        " group by" +
        "     l_returnflag," +
        "     l_linestatus" +
        " order by" +
        "     l_returnflag," +
        "     l_linestatus"
  }

  def getResultString1():String = {
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
        "     and P_SIZE = 15" +
        "     and P_TYPE like '%BRASS'" +
        "     and S_NATIONKEY = N_NATIONKEY" +
        "     and N_REGIONKEY = R_REGIONKEY" +
        "     and R_NAME = 'EUROPE'" +
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
        "             and R_NAME = 'EUROPE'" +
        "            )"+
        " order by" +
        "     S_ACCTBAL desc," +
        "     N_NAME," +
        "     S_NAME," +
        "     P_PARTKEY"
  }

  def getResultString2():String = {
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
        "     o_orderdate"
  }

  def getResultString3():String = {
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
        //" and o_orderdate < '1993-07-01' + interval '3' month" +
        "     and o_orderdate < DATE_ADD('1993-07-01',90)" +
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
    "o_orderpriority order_count"
  }

  def getQuery5(): String = {
    //1. REGION = ASIA;
    //2. DATE = 1994-01-01.
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
          "        and R_NAME = 'ASIA'" +
          "        and o_orderdate >= '1994-01-01'" +
          //"        and o_orderdate < date '[DATE]' + interval '1' year" +
          "        and o_orderdate < DATE_ADD('1994-01-01', 365)" +
          " group by" +
          "        N_NAME" +
          " order by" +
          "        revenue desc"
  }

  def getResultString5():String = {
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
        "        and l_shipdate < DATE_ADD('1994-01-01', 365)" +
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
        "         supp_nation,"+
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
        "                         (n1.N_NAME = 'FRANCE' and n2.N_NAME = 'GERMANY')" +
        "                         or (n1.N_NAME = 'GERMANY' and n2.N_NAME = 'FRANCE')" +
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
    "supp_nation cust_nation l_year revenue"
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
        "                         and R_NAME = 'AMERICA'" +
        "                         and S_NATIONKEY = n2.N_NATIONKEY" +
        "                         and o_orderdate between '1995-01-01' and '1996-12-31'" +
        "                         and P_TYPE = 'ECONOMY ANODIZED STEEL'" +
        "         ) as all_nations" +
        " group by" +
        "         o_year" +
        " order by" +
        "         o_year"
  }

  def getResultString8():String = {
    "YEAR MKT_SHARE"
  }

  def getQuery9(): String = {
    //1. COLOR = green.
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
  }

  def getResultString9():String = {
    "NATION YEAR SUM_PROFIT"
  }
  def getQuery10(): String = {
    //1.    DATE = 1993-10-01.
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
        "         and o_orderdate < DATE_ADD('1993-10-01', 90)" +
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

  def getResultString10():String = {
    "C_CUSTKEY C_NAME REVENUE C_ACCTBAL N_NAME C_ADDRESS C_PHONE C_COMMENT"
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

  def getResultString11():String = {
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
        "         and l_receiptdate < DATE_ADD('1994-01-01',365)" +
        " group by" +
        "         l_shipmode" +
        " order by" +
        "         l_shipmode"
  }

  def getResultString12():String = {
    "L_SHIPMODE HIGH_LINE_COUNT LOW_LINE_COUNT"
  }

  def getQuery13(): String = {
//    1. WORD1 = special.
//    2. WORD2 = requests.
        "select" +
        "         c_count, " +
        "         count(*) as custdist" +
        " from (" +
        "         select" +
        "                 C_CUSTKEY," +
        "                 count(o_orderkey)" +
        "         from" +
        "                 CUSTOMER left outer join ORDERS on" +
        "                 C_CUSTKEY = o_custkey" +
        "                 and o_comment not like ‘%special%requests%’" +
        "         group by" +
        "                 C_CUSTKEY" +
        "         )as c_orders (C_CUSTKEY, c_count)" +
        " group by" +
        "         c_count" +
        " order by" +
        "         custdist desc," +
        "         c_count desc"
  }

  def getResultString13():String = {
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
        "         and l_shipdate < DATE_ADD ('1995-09-01', 30)"
  }

  def getResultString14():String = {
    "PROMO_REVENUE"
  }

  def getQuery15(): String = {
    ""
  }

  def getResultString15():String = {
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

  def getResultString16():String = {
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
        "         and P_CONTAINER = 'MED BOX'" +
        "         and l_quantity < (" +
        "                 select" +
        "                         0.2 * avg(l_quantity)" +
        "                 from" +
        "                         LINEITEM" +
        "                 where" +
        "                         l_partkey = P_PARTKEY" +
        "         )key" +
        " )"
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
        "                         l_orderkey having sum(l_quantity) > 300" +
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
    "C_NAME C_CUSTKEY O_ORDERKEY O_ORDERDATE O_TOTALPRICE Sum(L_QUANTITY)"
  }
  def getQuery19(): String = {
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
  }

  def getResultString19():String = {
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
        "                                         P_NAME like 'forest%'" +
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
        "                                         and l_shipdate < DATE_ADD('1994-01-01', 365)" +
        "                         )" +
        "         )" +
        "         and S_NATIONKEY = N_NATIONKEY" +
        "         and N_NAME = 'CANADA'" +
        " order by" +
        "         S_NAME"
  }

  def getResultString20():String = {
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
        "         and N_NAME = 'SAUDI ARABIA'" +
        " group by" +
        "         S_NAME" +
        " order by" +
        "         numwait desc," +
        "         S_NAME"
  }

  def getResultString21():String = {
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
        "                 substring(C_PHONE from 1 for 2) as cntrycode," +
        "                 C_ACCTBAL" +
        "         from" +
        "                 CUSTOMER        " +
        "         where" +
        "                 substring(C_PHONE from 1 for 2) in" +
        "                         ('13','31’,'23','29','30','18','17')" +
        "                 and C_ACCTBAL > (" +
        "                         select" +
        "                                 avg(C_ACCTBAL)" +
        "                         from" +
        "                                 CUSTOMER" +
        "                         where" +
        "                                 C_ACCTBAL > 0.00" +
        "                                 and substring (C_PHONE from 1 for 2) in" +
        "                                         ('13','31’,'23','29','30','18','17')" +
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

  def getResultString22():String = {
    "CNTRYCODE NUMCUST TOTACCTBAL"
  }


}

