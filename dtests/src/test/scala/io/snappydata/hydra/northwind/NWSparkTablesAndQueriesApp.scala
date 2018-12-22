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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object NWSparkTablesAndQueriesApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("NWSparkTablesAndQueriesApp Application_" + System.currentTimeMillis()).
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)
    val dataFilesLocation = args(0)
    snc.sql("set spark.sql.shuffle.partitions=6")
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    NWTestUtil.dropTables(snc)
    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File("NWSparkTablesAndQueriesApp.out"),
      true));
    println("Test replicated row tables queries started")
    createAndLoadSparkTables(sqlContext)
    printResults(snc, pw)
    pw.close()
  }

  private def assertJoin(snc: SnappyContext, sqlString: String, tableType: String, queryNum:
  String, pw: PrintWriter): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    pw.println(s"Query ${queryNum} \n df.count for join query is :  ${df.count} \n  TableType " +
        s": ${tableType} \n df.explain() : ${df.explain().toString}")

  }

  private def assertQuery(snc: SnappyContext, sqlString: String, tableType: String, queryNum:
  String, pw: PrintWriter): Any = {
    val df = snc.sql(sqlString)
    pw.println(s"\nQuery ${queryNum} \n df.count is : ${df.count} \n  TableType : ${tableType} \n" +
        s" {df.explain() : ${df.explain().toString}")
  }

  private def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    NWQueries.regions(sqlContext).registerTempTable("regions")
    NWQueries.categories(sqlContext).registerTempTable("categories")
    NWQueries.shippers(sqlContext).registerTempTable("shippers")
    NWQueries.employees(sqlContext).registerTempTable("employees")
    NWQueries.customers(sqlContext).registerTempTable("customers")
    NWQueries.orders(sqlContext).registerTempTable("orders")
    NWQueries.order_details(sqlContext).registerTempTable("order_details")
    NWQueries.products(sqlContext).registerTempTable("products")
    NWQueries.suppliers(sqlContext).registerTempTable("suppliers")
    NWQueries.territories(sqlContext).registerTempTable("territories")
    NWQueries.employee_territories(sqlContext).registerTempTable("employee_territories")
  }

  private def printResults(snc: SnappyContext, pw: PrintWriter): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, "ReplicatedTable", "Q1", pw)
        case "Q2" => assertQuery(snc, NWQueries.Q2, "ReplicatedTable", "Q2", pw)
        case "Q3" => assertQuery(snc, NWQueries.Q3, "ReplicatedTable", "Q3", pw)
        case "Q4" => assertQuery(snc, NWQueries.Q4, "ReplicatedTable", "Q4", pw)
        case "Q5" => assertQuery(snc, NWQueries.Q5, "ReplicatedTable", "Q5", pw)
        case "Q6" => assertQuery(snc, NWQueries.Q6, "ReplicatedTable", "Q6", pw)
        case "Q7" => assertQuery(snc, NWQueries.Q7, "ReplicatedTable", "Q7", pw)
        case "Q8" => assertQuery(snc, NWQueries.Q8, "ReplicatedTable", "Q8", pw)
        case "Q9" => assertQuery(snc, NWQueries.Q9, "ReplicatedTable", "Q9", pw)
        case "Q10" => assertQuery(snc, NWQueries.Q10, "ReplicatedTable", "Q10", pw)
        case "Q11" => assertQuery(snc, NWQueries.Q11, "ReplicatedTable", "Q11", pw)
        case "Q12" => assertQuery(snc, NWQueries.Q12, "ReplicatedTable", "Q12", pw)
        case "Q13" => assertQuery(snc, NWQueries.Q13, "ReplicatedTable", "Q13", pw)
        case "Q14" => assertQuery(snc, NWQueries.Q14, "ReplicatedTable", "Q14", pw)
        case "Q15" => assertQuery(snc, NWQueries.Q15, "ReplicatedTable", "Q15", pw)
        case "Q16" => assertQuery(snc, NWQueries.Q16, "ReplicatedTable", "Q16", pw)
        case "Q17" => assertQuery(snc, NWQueries.Q17, "ReplicatedTable", "Q17", pw)
        case "Q18" => assertQuery(snc, NWQueries.Q18, "ReplicatedTable", "Q18", pw)
        case "Q19" => assertQuery(snc, NWQueries.Q19, "ReplicatedTable", "Q19", pw)
        case "Q20" => assertQuery(snc, NWQueries.Q20, "ReplicatedTable", "Q20", pw)
        case "Q21" => assertQuery(snc, NWQueries.Q21, "ReplicatedTable", "Q21", pw)
        case "Q22" => assertQuery(snc, NWQueries.Q22, "ReplicatedTable", "Q22", pw)
        case "Q23" => assertQuery(snc, NWQueries.Q23, "ReplicatedTable", "Q23", pw)
        case "Q24" => assertQuery(snc, NWQueries.Q24, "ReplicatedTable", "Q24", pw)
        case "Q25" => assertJoin(snc, NWQueries.Q25, "ReplicatedTable", "Q25", pw)
        case "Q26" => assertJoin(snc, NWQueries.Q26, "ReplicatedTable", "Q26", pw)
        case "Q27" => assertJoin(snc, NWQueries.Q27, "ReplicatedTable", "Q27", pw)
        case "Q28" => assertJoin(snc, NWQueries.Q28, "ReplicatedTable", "Q28", pw)
        case "Q29" => assertJoin(snc, NWQueries.Q29, "ReplicatedTable", "Q29", pw)
        case "Q30" => assertJoin(snc, NWQueries.Q30, "ReplicatedTable", "Q30", pw)
        case "Q31" => assertJoin(snc, NWQueries.Q31, "ReplicatedTable", "Q31", pw)
        case "Q32" => assertJoin(snc, NWQueries.Q32, "ReplicatedTable", "Q32", pw)
        case "Q33" => // assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, "ReplicatedTable", "Q34", pw)
        case "Q35" => assertJoin(snc, NWQueries.Q35, "ReplicatedTable", "Q35", pw)
        case "Q36" => assertJoin(snc, NWQueries.Q36, "ReplicatedTable", "Q36", pw)
        case "Q37" => assertJoin(snc, NWQueries.Q37, "ReplicatedTable", "Q37", pw)
        case "Q38" => assertJoin(snc, NWQueries.Q38, "ReplicatedTable", "Q38", pw)
        case "Q39" => assertJoin(snc, NWQueries.Q39, "ReplicatedTable", "Q39", pw)
        case "Q40" => assertJoin(snc, NWQueries.Q40, "ReplicatedTable", "Q40", pw)
        case "Q41" => assertJoin(snc, NWQueries.Q41, "ReplicatedTable", "Q41", pw)
        case "Q42" => assertJoin(snc, NWQueries.Q42, "ReplicatedTable", "Q42", pw)
        case "Q43" => assertJoin(snc, NWQueries.Q43, "ReplicatedTable", "Q43", pw)
        case "Q44" => assertJoin(snc, NWQueries.Q44, "ReplicatedTable", "Q44", pw)
        // LeftSemiJoinHash
        case "Q45" => assertJoin(snc, NWQueries.Q45, "ReplicatedTable", "Q45", pw)
        case "Q46" => assertJoin(snc, NWQueries.Q46, "ReplicatedTable", "Q46", pw)
        case "Q47" => assertJoin(snc, NWQueries.Q47, "ReplicatedTable", "Q47", pw)
        case "Q48" => assertJoin(snc, NWQueries.Q48, "ReplicatedTable", "Q48", pw)
        case "Q49" => assertJoin(snc, NWQueries.Q49, "ReplicatedTable", "Q49", pw)
        case "Q50" => assertJoin(snc, NWQueries.Q50, "ReplicatedTable", "Q50", pw)
        case "Q51" => assertJoin(snc, NWQueries.Q51, "ReplicatedTable", "Q51", pw)
        case "Q52" => assertJoin(snc, NWQueries.Q52, "ReplicatedTable", "Q52", pw)
        case "Q53" => assertJoin(snc, NWQueries.Q53, "ReplicatedTable", "Q53", pw)
        case "Q54" => assertJoin(snc, NWQueries.Q54, "ReplicatedTable", "Q54", pw)
        case "Q55" => assertJoin(snc, NWQueries.Q55, "ReplicatedTable", "Q55", pw)
        case "Q56" => assertJoin(snc, NWQueries.Q56, "ReplicatedTable", "Q56", pw)
        case "Q57" => assertJoin(snc, NWQueries.Q57, "ReplicatedTable", "Q57", pw)
        case "Q58" => assertJoin(snc, NWQueries.Q58, "ReplicatedTable", "Q58", pw)
        case "Q59" => assertJoin(snc, NWQueries.Q59, "ReplicatedTable", "Q59", pw)
        case "Q60" => assertJoin(snc, NWQueries.Q60, "ReplicatedTable", "Q60", pw)
      }
    }
  }

}

