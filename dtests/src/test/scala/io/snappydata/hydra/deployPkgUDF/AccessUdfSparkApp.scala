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
package io.snappydata.hydra.deployPkgUDF

import java.io.{File, FileOutputStream, PrintWriter}
import com.typesafe.config.Config
import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SnappyContext}

object AccessUdfSparkApp {
  def main(args: Array[String]) {
    // scalastyle:off println
    val connectionURL = args(args.length - 1)
    println("The connection url is " + connectionURL)
    val conf = new SparkConf().
        setAppName("AccessUdfSparkApp Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    val pw = new PrintWriter(new FileOutputStream(new File("AccessUdfSparkApp.out"),
      true));
    val udf1 = args(0)
    val udf2 = args(1)
    val udf3 = args(2)
    val argNum = Integer.parseInt(args(3))
    val argStr = args(4)
    val isException = args(5).toBoolean

    val udfs = new Array[String](3)
    udfs(0) = udf1
    udfs(1) = udf2
    udfs(2) = udf3


    for (i <- 0 to udfs.length - 1) {
      val udfName = udfs(i)
      var selectStmt = ""
      try {
        udfName match {
          case "MyUDF3" => selectStmt = s"select myudf3($argNum,$argNum)"
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
          case "MyUDF4" => selectStmt = s"select myudf4($argNum)"
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
          case "MyUDF5" => selectStmt = s"select myudf5('$argStr')"
            val rs = snc.sql(selectStmt).first()
            validateResults(snc, rs, udfName)
        }

      } catch {
        case ex: Exception => {
          if (isException) {
            println("Got Expected Exception " + ex.printStackTrace())
          }
          else {
            println("Got UnExpected exception " + ex.printStackTrace())
          }
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
          val expectedStr = argStr.toUpperCase
          val actualResultStr = resultSet.getString(0)
          println("The expected result is =  " + expectedStr)
          println("The actual result is = " + actualResultStr)
          if (!expectedStr.equals(actualResultStr)) {
            println("Result for udf " + udfName + " doesnot match")
          }
        }
      }
      if (expectedResult != actualResult) {
        println("Result for udf " + udfName + " doesnot match")
      }

    }
  }
}
