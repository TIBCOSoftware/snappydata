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

import java.io.{FileOutputStream, PrintWriter, File}
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
        pw.println("ROW COUNTS AND ROW contents AREN'T MATCHED FOR COMMAND : " + command)
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
        hts.isFailed = false
       }
      else {
        pw.println("ROW COUNTS AND ROW CONTENTS AREN'T MATCHED FOR COMMAND : " + command)
        hts.isFailed = true
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

    if(hts.printLog) {
      println("calculateSnappyCount : " + calculateSnappyCount)
      println("calculateBeelineCount : " + calculateBeelineCount)
    }

    if(calculateSnappyCount.equals(calculateBeelineCount)) {
      pw.println("Row count for query  -- " + query +
        " -- are  equal between Snappy and Beeline. " +
        "Total Row count is : " + calculateBeelineCount )
      hts.isFailed = false
    }
    else {
      pw.println("ROW COUNT FOR QUERY  -- " + query +
        " -- ARE NOT EQUAL BETWEEN Snappy AND Beeline.")
      hts.isFailed = true
    }
    calculateBeelineCount = null
    calculateSnappyCount = null
  }

  def validateSelectQuery(query : String, snc : SnappyContext,
                               hts : HiveThriftServer, pw: PrintWriter) : Unit = {

     hts.stmt = hts.connection.createStatement()
     hts.rs = hts.stmt.executeQuery(query)
     var index : Long = 0
     var beeLineString : String = ""
     while(hts.rs.next()) {
        index += 1
       beeLineString = beeLineString + "[" + hts.rs.getString("id") + "," +
       hts.rs.getString("name") + "]"
     }
    var snappyString = snc.sql(query).collect().mkString
    if(beeLineString.equals(snappyString)) {
      pw.println("Data between BeeLine and Snappy are matched for " + query)
      hts.isFailed = false
    }
    else {
      pw.println("DATA BETWEEN BeeLine AND Snappy ARE NOT MATCHED FOR " + query)
      hts.isFailed = true
    }

    if(snc.sql(query).count() == index) {
      pw.println("Counts of query : -- " + query + " -- are equal.")
      hts.isFailed = false
    }
    else {
      pw.println("COUNTS OF QUERY : " + query + " are NOT EQUAL.")
      hts.isFailed = true
    }
    if (hts.printLog) {
      println("Beeline count : " + index)
      println("Snappy count : " + snc.sql(query).count)
     }
    beeLineString = null
    snappyString = null
   }
 }
