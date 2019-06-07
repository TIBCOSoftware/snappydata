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

package io.snappydata.hydra.snapshotIsolation

import java.io.{File, PrintWriter}
import java.util

import io.snappydata.hydra.TestUtil

import org.apache.spark.sql.{Row, DataFrame, SnappyContext}

class SnapshotIsolationTestUtils {

  def executeQueries(snc: SnappyContext, pw: PrintWriter): Any = {
    assertQuery(SnapshotIsolationQueries.Q1, "Q1", snc, pw)
  }

  def assertQuery(sqlString: String, queryNum: String, snc: SnappyContext, pw: PrintWriter): Any = {
    val time = System.currentTimeMillis()
    val rowList = snc.sql(sqlString).collect()
    try {
      verifyDuplicateRows(rowList, pw)
      //TestUtil.compareFiles(snappyFile, newDFFile, pw, false)
    } catch {
      case ex: Exception => {
        pw.println(s"Verification failed for ${queryNum} with following exception:\n")
        ex.printStackTrace(pw)
      }
    }
    pw.flush()
  }

  def verifyDuplicateRows(rowList: Array[Row], pw: PrintWriter): Unit = {
    val numRows = rowList.length
    pw.println(s"Num rows in resultSet are ${numRows}.")
    if (numRows == 0)
      pw.println(s"There are no duplicate rows in resultSet. ")
    else {
      pw.println(s"Below duplicate rows found in resultSet: ")
      for (row <- rowList)
        pw.println(row)
    }
    /*
    var dupFound = false
    val dupList: util.List[Row] = new util.ArrayList[Row]()
    for (row <- rowList) {
      pw.println(s"${row}")
      val count = row.getAs("count")
      if (count > 1) {
        dupFound = true
        pw.print("This is a duplicate row");
        dupList.add(row)
      }
    }
    if (dupFound) {
      pw.println(s"Below duplicate rows found in resultSet: ")
      for (i <- dupList)
        pw.println(dupList)
    }
  }
*/
  }

}
