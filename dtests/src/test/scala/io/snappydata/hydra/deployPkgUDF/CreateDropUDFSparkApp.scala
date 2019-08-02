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

import java.io.PrintWriter
import com.typesafe.config.Config
import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyContext}


object CreateDropUDFSparkApp {
  def main(args: Array[String]) {
    // scalastyle:off println
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val connectionURL = args(args.length - 1)
    println("The connection url is " + connectionURL)
    val conf = new SparkConf().
        setAppName("AccessUdfSparkApp Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    val pw = new PrintWriter("CreateDropUDFSparkApp.out")
    Try {
      val udf1 = args(0)
      val udf2 = args(1)
      val udf3 = args(2)
      val isDropUDFFunction = args(3).toBoolean
      val jarPath = args(4)
      val udfs = new Array[String](3)
      udfs(0) = udf1
      udfs(1) = udf2
      udfs(2) = udf3

      val returnTyp = new Array[String](3)
      returnTyp(0) = "Integer"
      returnTyp(1) = "Float"
      returnTyp(2) = "String"

      if (isDropUDFFunction) {
        dropFunction(udfs)
      }
      else {
        createFunction(udfs, returnTyp, jarPath)
      }
    }
    match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/CreateDropUDFSparkApp.out"
      case Failure(e) => pw.close();
        throw e;
    }

    def createFunction(udfs: Array[String], returnTyp: Array[String], jarPath: String): Unit = {
      val pkg = "io.snappydata.hydra.deployPkgUDF.udfFiles"
       for (i <- 0 to udfs.length - 1) {
        val udfAlias = udfs(i).toString.toLowerCase
        val createFuncStr = "CREATE FUNCTION " + udfAlias + " as " + pkg + "." +
            udfs(i) + " returns " + returnTyp(i) + " using jar " + "'" + jarPath + "'"
        pw.println("The create statement is " + createFuncStr)
        snc.sql(createFuncStr)
      }

    }

    def dropFunction(udfs: Array[String]): Unit = {
      for (i <- 0 to udfs.length - 1) {
        val udfAlias = udfs(i).toString.toLowerCase
        val dropFuncStr = "DROP FUNCTION IF EXISTS " + udfAlias
        pw.println("The drop function statement is " + dropFuncStr)
        snc.sql(dropFuncStr)
      }
    }
  }

}
