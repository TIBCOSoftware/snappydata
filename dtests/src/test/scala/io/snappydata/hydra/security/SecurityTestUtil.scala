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

import io.snappydata.hydra.northwind.NWQueries

import org.apache.spark.sql.SnappyContext

object SecurityTestUtil {
  // scalastyle:off println
  var expectedExpCnt = 0;
  var unExpectedExpCnt = 0;

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
