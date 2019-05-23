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

package io.snappydata.hydra.security

import java.io.{PrintWriter}

import scala.collection.immutable.HashMap

import io.snappydata.hydra.northwind.NWQueries

import org.apache.spark.sql.{DataFrame, SnappyContext}

import org.apache.spark.sql.SnappyContext

object SecurityTestUtil {
  // scalastyle:off println
  var expectedExpCnt = 0;
  var unExpectedExpCnt = 0;

  var policyUserMap: HashMap[String, String] = HashMap()
  var policySelectQueryMap: HashMap[Map[String, DataFrame], String] = HashMap()
  var policyFullSelectQueryMap: HashMap[Map[String, DataFrame], String] = HashMap()

  def createColRowTables(snc: SnappyContext) : Unit = {
    println("Inside createColAndRow tables")
    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.employees_table + " using column options(partition_by 'City,Country', " +
        "redundancy '1')")
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table + " using column options(partition_by 'City,Country', " +
        "COLOCATE_WITH 'employees', redundancy '1')")
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets " +
        "'13', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
        "redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options (partition_by 'ProductID,SupplierID', buckets '17', redundancy '1')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1')")
    NWQueries.suppliers(snc).write.insertInto("suppliers")
    println("Finished  createColAndRow tables")
  }

  def createPolicy(snc: SnappyContext, userSchema: Array[String],
      userName: Array[String]): Unit = {
    println("Inside createPolicy() ")
    var filterCond, orderBy = ""
    var cnt = 1
    try {
      for (i <- 0 to userName.length - 1) {
        val policyUser = userName(i)
        for (s <- 0 to userSchema.length - 1) {
          if (userSchema(s).contains("employees")) {
            filterCond = "EMPLOYEEID = 1 AND COUNTRY = 'USA'"
            orderBy = " ORDER BY EMPLOYEEID asc "
          }
          else {
            filterCond = "CATEGORYID = 4"
            orderBy = " ORDER BY CATEGORYID asc "
          }

          val grantSql = " GRANT SELECT on " + userSchema(s) + " TO " + userName(i)
          println("GRANT sql statement is " + grantSql)
          snc.sql(grantSql)

          var policyName = "p" + 0;
          while (policyUserMap.contains(policyName)) {
            println("checking for policy name " + policyName);
            policyName = "p" + cnt
            println("The next policy name is " + policyName)
            cnt = cnt + 1
          }
          var queryMap1: Map[String, DataFrame] = Map()
          var queryMap2: Map[String, DataFrame] = Map()
          val policyStr = "CREATE POLICY " + policyName + " ON " + userSchema(s) +
              " FOR SELECT TO " + policyUser + " USING " + filterCond
          println("The create policy string is " + policyStr)
          val selectQryWF = "SELECT * FROM " + userSchema(s) + " WHERE " + filterCond;
          println("Select Query with filter is " + selectQryWF)
          val selectQry = "SELECT * FROM " + userSchema(s) + orderBy;
          println("Select Query is " + selectQry)
          println("Policy created for " + policyUser + " on table " +
              userSchema(s) + " is " + policyStr)
          snc.sql(policyStr)
          val result1 = snc.sql(selectQry)
          val result2 = snc.sql(selectQryWF)

          queryMap1 += (selectQry -> result1)
          queryMap2 += (selectQryWF -> result2)
          policySelectQueryMap += (queryMap2 -> policyUser)
          policyFullSelectQueryMap += (queryMap1 -> policyUser)
          policyUserMap += (policyName -> userSchema(s))
        }
      }
    }
    catch {
      case ex: Exception => {
        println("Caught exception in createPolicy " + ex.printStackTrace())
      }
    }
    println("Successfully completed createPolicy task")
  }

  def dropPolicy(snc: SnappyContext): Unit = {
    println("Inside dropPolicy()")
    try {
      val policyMap = policyUserMap
      for ((k, v) <- policyMap) {
        val policyNm = k
        val schemaOwner = v
        val dropPolicyStr = " DROP POLICY " + policyNm
        println("Dropping policy p " + policyNm + " with schemaOwner " + schemaOwner)
        snc.sql(dropPolicyStr)
      }
    }
    catch {
      case ex: Exception => {
        println("Caught exception in dropPolicy() " + ex.printStackTrace())
      }
    }
  }

  def enableRLS(snc: SnappyContext, userSchema: Array[String]): Unit = {
    println("Inside enableRLS()")
    val isAltrTableRLS = true
    var alterTabSql = ""
    try {
      for (s <- 0 to userSchema.length - 1) {
        if (isAltrTableRLS) {
          alterTabSql = "ALTER TABLE " + userSchema(s) + " ENABLE ROW LEVEL SECURITY";
          println("The alter table sql is " + alterTabSql);
        } else {
          alterTabSql = "ALTER TABLE " + userSchema(s) + " DISABLE ROW LEVEL SECURITY";
          println("The alter table sql is " + alterTabSql);
        }
        snc.sql(alterTabSql)
      }
    }
    catch {
      case ex: Exception => {
        println("Caught exception in enableRLS " + ex.printStackTrace())
      }
    }
    println("Successfully completed enableRLS task")
  }

  def validateRLS(snc: SnappyContext, isDropPolicy: Boolean, isAltrTableRLS:
  Boolean = true): Unit = {
    println("Inside validateRLS")
    var queryUserMap: HashMap[Map[String, DataFrame], String] = HashMap()
    if (isDropPolicy || !isAltrTableRLS) {
      queryUserMap = policyFullSelectQueryMap; // this map contains rs fro select * from table query
      println("Using policyFullSelectQueryMap");
    } else {
      queryUserMap = policySelectQueryMap; // this map contains rs fro
      // select * from table with condition query
      println("Using policySelectQueryMap");
    }
    try
        for ((k, v) <- queryUserMap) {
          val queryMap = k
          val policyUser = v
          println("The user is " + policyUser);
          for ((k1, v1) <- queryMap) {
            val selectQry = k1
            val finalSelectQ: Array[String] = selectQry.split("WHERE")
            val prevDF = v1
            println("The select Query is " + selectQry);
            println("The Final select Query tobe executed by policy User is  " + finalSelectQ(0));
            val currDF = snc.sql(finalSelectQ(0))
            val currFiltrDF = snc.sql(selectQry)
            val currDFSize = currDF.count()
            val prevDFSize = prevDF.count()
            val currFiltrDFSize = currFiltrDF.count()
            println("Current DF size = " + currDFSize + " Previous DF size = " + prevDFSize +
                " Current DF with filter size = " + currFiltrDFSize)
            if ((currDFSize == prevDFSize) && (currFiltrDFSize == prevDFSize)) {
              println(" The number of rows returned are equal ")
            }
            else {
              println(" The number of rows returned donot match Current DF size = " + currDFSize
                  + " Previous DF size = " + prevDFSize + " and CurrFiltrDF Size = " +
                  currFiltrDFSize)
            }
          }
        }

    catch {
      case ex: Exception => {
        println("Caught exception in validateRLS " + ex.printStackTrace())
      }
    }
  }

  def runQueries(snc: SnappyContext, queryArray: Array[String], expectExcpCnt: Integer,
      unExpectExcpCnt: Integer, isGrant: Boolean, userSchema: Array[String],
       isSelect: Boolean, pw: PrintWriter)
  : Unit = {
    println("Inside run/queries inside SecurityUtil")
    var isAuth = isGrant
    var opAuth: Boolean = false
    var schemaAuth: Boolean = false
    for (j <- 0 to queryArray.length - 1) {
      try {

         for (s <- 0 to userSchema.length - 1) {
           val str = userSchema(s)
           println("Find " + str + " in query " + queryArray(j));
           if (isGrant) {
             if (!queryArray(j).contains(str)) {
               isAuth = false
               println("Execute query   " + queryArray(j) + " with  authorization = " + isAuth)
               snc.sql(queryArray(j)).show
             }
             else {
               if (!isSelect) {
                 if (queryArray(j).contains("INSERT")) {
                   isAuth = true
                 }
                 else {
                   isAuth = false
                 }
               }
               else {
                 isAuth = true
               }
               println("Execute query   " + queryArray(j) + " with  authorization = " + isAuth)
               snc.sql(queryArray(j)).show
               pw.println(s"Query executed successfully is " + queryArray(j))
             }
           }
           else
             {
               isAuth = false
               println("Execute query   " + queryArray(j) + " with  authorization = " + isAuth)
               snc.sql(queryArray(j)).show
               pw.println(s"Query executed successfully is " + queryArray(j))
             }
         }
      }
      catch {
        case ex: Exception => {
          if (isAuth) {
            unExpectedExpCnt = unExpectedExpCnt + 1
            println("Got unExpected Exception " + ex.printStackTrace())
            println("unExpectedExpCnt = " + unExpectedExpCnt)
          }
          else {
            expectedExpCnt = expectedExpCnt + 1
            println("Got Expected exception " + ex.printStackTrace())
            println("expectedCnt = " + expectedExpCnt)
          }
        }
      }
    }
    validate(expectExcpCnt, unExpectExcpCnt)
  }

  def validate(expectedCnt: Integer, unExpectedCnt: Integer): Unit = {
    if (unExpectedCnt == unExpectedExpCnt) {
      println("Validation SUCCESSFUL Got expected cnt of unExpectedException = " + unExpectedCnt)

    }
    else {
      sys.error("Validation failure expected cnt was = " + unExpectedCnt + " but got = "
          + unExpectedCnt)
    }
    if (expectedCnt == expectedExpCnt) {
      println("Validation SUCCESSFUL Got expected cnt of expectedException = " + expectedCnt)

    }
    else {
      sys.error("Validation failure expected cnt was = " + expectedCnt + " but got = "
          + expectedExpCnt)
    }
    unExpectedExpCnt = 0;
    expectedExpCnt = 0;
  }
}
