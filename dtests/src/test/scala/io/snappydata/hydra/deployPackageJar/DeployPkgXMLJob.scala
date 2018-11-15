/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.deployPackageJar

import java.io.{File, FileOutputStream, PrintWriter}
// import com.pivotal.gemfirexd.internal.engine.Misc
import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class DeployPkgXMLJob extends SnappySQLJob{
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {


    val spark : SparkSession = SparkSession.builder().getOrCreate()

    val snc : SnappyContext = snappySession.sqlContext
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val Q1 : String = "SELECT * FROM Books"
//    var tableType : String = jobConfig.getString("tableType")
//    if(tableType == null){
//      tableType = "column"
//    }
    val outputFile = "ValidateBooksXML_" + "column" + "_" + System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)


    val xmlFilePath: String = jobConfig.getString("xmlFileLocation")

    val xmlSchema = snappySession.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog")
      .load(s"${xmlFilePath}")
    val xmlSparkSchema = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog")
      .load(s"${xmlFilePath}")


    val schemaGeneration = xmlSchema.schema
    val sparkSchemaGeneration = xmlSparkSchema.schema

    val xmlData = snappySession.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog").schema(schemaGeneration)
      .load(s"${xmlFilePath}")
    val xmlSparkData = spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "catalog").schema(sparkSchemaGeneration)
        .load(s"${xmlFilePath}")

    xmlData.createOrReplaceTempView("tempXMLTbl")
    xmlSparkData.createOrReplaceTempView("Books")

    // TODO Below statement to be tested, on ROW TABLE AS SELECT has problem and It is know Issue.
    // snappySession.sql("CREATE TABLE Books AS SELECT * FROM tempXMLTbl using column options()")

    snappySession.sql("DROP TABLE IF EXISTS Books")
    snappySession.sql("CREATE TABLE Books using column AS SELECT * FROM tempXMLTbl")

    SnappyTestUtils.assertQueryFullResultSet(snc, Q1, "Q1", "column", pw, sqlContext)


//    Below code is for testing purpose. Please do not remove it.
//    val r = snappySession.sql("SELECT * FROM Books").collect()
//
//    val logger = Misc.getCacheLogWriterNoThrow
//
//    if (logger != null) {
//      logger.info("Result = " + r + " and size = " + r.size +
//        " and first element = " + r(0))
//    }

  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
