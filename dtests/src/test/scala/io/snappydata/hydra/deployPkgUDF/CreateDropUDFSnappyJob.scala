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
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}


object CreateDropUDFSnappyJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val snc = snSession.sqlContext
    // scalastyle:off println
    val pw = new PrintWriter("SnappyJobOutPut.out")
    Try {
      val udf1: String = jobConfig.getString("udf1")
      val udf2: String = jobConfig.getString("udf2")
      val udf3: String = jobConfig.getString("udf3")
      val isDropUDFFunction: Boolean = jobConfig.getBoolean("isDropUDFFunction")
      val jarPath: String = jobConfig.getString("jarPath")
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
        s"See ${getCurrentDirectory}/SnappyJobOutPut.out"
      case Failure(e) => pw.close();
        throw e;
    }

    def createFunction(udfs: Array[String], returnTyp: Array[String], jarPath: String): Unit = {
      val pkg = "io.snappydata.hydra.deployPkgUDF.udfFiles"
      for (i <- 0 to udfs.length - 1) {
        val udfAlias = udfs(i).toString.toLowerCase
        val createFuncStr = "CREATE FUNCTION " + udfAlias + " as " + pkg + "." +
            udfs(i) + " returns " + returnTyp(i) + " using jar " + "'" + jarPath + "'"
        println("The create statement for " + udfAlias + " is " + createFuncStr)
        snc.sql(createFuncStr)
      }

    }

    def dropFunction(udfs: Array[String]): Unit = {
      for (i <- 0 to udfs.length - 1) {
        val udfAlias = udfs(i).toString.toLowerCase
        val dropFuncStr = "DROP FUNCTION IF EXISTS " + udfAlias
        println("The drop function statement is " + dropFuncStr)
        snc.sql(dropFuncStr)
      }
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
