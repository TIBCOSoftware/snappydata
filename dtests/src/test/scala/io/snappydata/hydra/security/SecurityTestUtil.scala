/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SnappyContext

object SecurityTestUtil {
  // scalastyle:off println
  var expectedExpCnt = 0;
  var unExpectedExpCnt = 0;
  def runQueries(snc: SnappyContext, queryArray: Array[String], expectExcpCnt: Integer,
      unExpectExcpCnt: Integer, isGrant: Boolean, userSchema: Array[String], pw: PrintWriter)
  : Unit = {
    println("Inside run/queries inside SecurityUtil")
    var isAuth = isGrant
    for (j <- 0 to queryArray.length - 1) {
      // if(!usr.equals("user1") && !usr.equals(schemaOwner) && isGrant){
        for (s <- 0 to userSchema.length - 1) {
          val str = userSchema(s)
          println("Find " + str + " in query " + queryArray(j));
          if (! queryArray(j).contains(str))
           { isAuth = false;}
          println("Execute query   " + queryArray(j) + " with new authorization = " + isAuth);
        }
     // }
      try {
       /* val actualResult = snc.sql(queryArray(j))
        val result = actualResult.collect() */
        println("Query executed is " + queryArray(j))
       // if()
        snc.sql(queryArray(j)).show

        pw.println(s"Query executed successfully is " + queryArray(j))
        /* result.foreach(rs => {
          pw.println(rs.toString)
        }) */
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
    //  pw.close()
  }

  def validate(expectedCnt: Integer, unExpectedCnt: Integer) : Unit = {
    if (unExpectedCnt == unExpectedExpCnt){
      println("Validation SUCCESSFUL Got expected cnt of unExpectedException = " + unExpectedCnt)

    }
    else {
      sys.error("Validation failure expected cnt was = " + unExpectedCnt + " but got = "
          + unExpectedCnt)
    }
    if(expectedCnt == expectedExpCnt) {
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
