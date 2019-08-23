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
package io.snappydata.hydra.deployPkgUDF

import java.io.PrintWriter
import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}


object AccessUdfSnappyJob extends SnappySQLJob {
  // scalastyle:off println
  // println(" Inside CreateAndLoadSnappyJob")

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    println("Running AccessUdfSnappyJob")
    val snc = snSession.sqlContext
    val udf1: String = jobConfig.getString("udf1")
    val udf2: String = jobConfig.getString("udf2")
    val udf3: String = jobConfig.getString("udf3")
    val argNum: Integer = jobConfig.getInt("argNum")
    val argStr: String = jobConfig.getString("argStr")
    val isException: Boolean = jobConfig.getBoolean("isException")

    val udfs = new Array[String](3)
    udfs(0) = udf1
    udfs(1) = udf2
    udfs(2) = udf3

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter("SnappyJobOutPut.out")
    try {
      for (i <- 0 to udfs.length - 1) {
        val udfName = udfs(i)
        var selectStmt = ""
        println("The udf is " + udfName + " with isException = " + isException)
        udfName match {
          case "MyUDF3" => selectStmt = s"select myudf3($argNum,$argNum)"
            println("The select stmt for " + udfName + " is " + selectStmt)
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
          case "MyUDF4" => selectStmt = s"select myudf4($argNum)"
            println("The select stmt for " + udfName + " is " + selectStmt)
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
          case "MyUDF5" => selectStmt = s"select myudf5('$argStr')"
            println("The select stmt for " + udfName + " is " + selectStmt)
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
        }

      }
    }
    catch {
      case ex: Exception => {
        if (isException) {
          println("Got Expected Exception " + ex.printStackTrace())
          pw.close()
        }
        else {
          println("Got UnExpected exception " + ex.printStackTrace())
          pw.close()
        }
      }
    }

    def validateResults(snc: SnappyContext, resultSet: Row, udfName: String): Unit = {
      var expectedResult = 0
      var actualResult = 0

      udfName match {
        case "MyUDF3" => expectedResult = argNum + argNum
          actualResult = resultSet.getInt(0)
          println("The expected result is =  " + expectedResult)
          println("The actual result is = " + actualResult)
        case "MyUDF4" => {
          val expectedResultFl = argNum / 100.0f
          val actualResultFL = resultSet.getFloat(0)
          println("The expected result is =  " + expectedResultFl)
          println("The actual result is = " + actualResultFL)
          if (expectedResultFl != actualResultFL) {
            println("Result for udf " + udfName + " doesnot match")
          }
        }
        case "MyUDF5" => {
          val expectedStr = argStr.toUpperCase()
          val actualResultStr = resultSet.getString(0)
          println("The expected result is =  " + expectedStr)
          println("The actual result is = " + actualResultStr)
          if (!expectedStr.equals(actualResultStr)) {
            println("Result for udf " + udfName + " doesnot match")
          }
        }
      }
      if (expectedResult != actualResult) {
        pw.println("Result for udf " + udfName + " doesnot match")
      }

    }

  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

