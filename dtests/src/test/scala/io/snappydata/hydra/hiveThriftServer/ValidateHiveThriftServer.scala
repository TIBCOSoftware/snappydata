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
package io.snappydata.hydra.hiveThriftServer

import java.io.PrintWriter
import org.apache.spark.sql.SnappyContext

object ValidateHiveThriftServer {
  // scalastyle:off println

  def validate_ShowSchema_Showdatabases(command : String, hts : HiveThriftServer,
                                        snc : SnappyContext, pw : PrintWriter) : Unit = {
    var result = ""
    var snappyResult = ""
    var counter : Long = 0

    val dfResult = snc.sql(command)
    val dfDBName = dfResult.collectAsList()

    while(hts.rs.next) {
      counter += 1
      result = result + hts.rs.getString("databaseName") + "\t"
    }
    var index = 0
    while(index < 3 ) {
      snappyResult = snappyResult + dfDBName.get(index) + "\t"
      snappyResult = snappyResult.replace("[", "")
      snappyResult = snappyResult.replace("]", "")
      index += 1
    }
    if(command == "ShoW DaTAbasEs") {
      if (hts.printLog) {
        println("Beeline Result : " + result)
        println("snappyResult : " + snappyResult)
      }
      if ((dfResult.count() == counter) && (result.equals(snappyResult))) {
        pw.println("Row counts and Row contents are matched between Snappy and Beeline" +
          " for command : " + command)
      }
      else {
        pw.println("Row counts and Row contents aren't matched for command : " + command)
      }
    }

    if(command == "sHOw SCHemas") {
      if (hts.printLog) {
        println("Beeline Result : " + result)
        println("snappyResult : " + snappyResult)
      }
      if ((dfResult.count() == counter) && (result.equals(snappyResult))) {
        pw.println("Row counts and Row contents are matched between Snappy and Beeline" +
          " for command : " + command)
       }
      else {
        pw.println("Row counts and Row contents aren't matched for command : " + command)
      }
    }
  }

  def validateSelectCountQuery(query : String, snc : SnappyContext,
                               hts : HiveThriftServer, pw: PrintWriter) : Unit = {
    var calculateBeelineCount : String = ""
    var calculateSnappyCount : String = ""

    val snappyDF1 = snc.sql(query).collect()
    calculateSnappyCount = snappyDF1(0).toString()
      .replace("[", "").replace("]", "")

    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(query)
    while (hts.rs.next())
    {
      calculateBeelineCount = hts.rs.getString(1)
    }

    println("calculateSnappyCount : " + calculateSnappyCount)
    println("calculateBeelineCount : " + calculateBeelineCount)

    if(calculateSnappyCount.equals(calculateBeelineCount)) {
      pw.println("Row count for query  >> " + query +
        " << are  equal between Snappy and Beeline. " +
        "Total Row count is : " + calculateBeelineCount )
    }
    else {
      pw.println("Row count for query  >> " + query +
        " << are  not equal between Snappy and Beeline.")
    }
  }

  def validateSelectQuery(query : String, snc : SnappyContext,
                               hts : HiveThriftServer, pw: PrintWriter) : Unit = {

     hts.stmt = hts.connection.createStatement()
     hts.rs = hts.stmt.executeQuery(query)
     var index : Long = 0
     while(hts.rs.next()) {
        index += 1
     }

    if(snc.sql(query).count() == index) {
      println("Counts of query : " + query + " are equal.")
      if (hts.printLog) {
      println("Beeline count : " + index)
      println("Snappy count : " + snc.sql(query).count)
      }
    }
  }
}
