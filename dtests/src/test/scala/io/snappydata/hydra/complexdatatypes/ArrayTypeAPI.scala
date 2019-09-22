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
package io.snappydata.hydra.complexdatatypes

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.io.{File, FileOutputStream, PrintWriter}
import io.snappydata.hydra.SnappyTestUtils

class ArrayTypeAPI extends SnappySQLJob{
    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
      // scalastyle:off println
      println("Query ArrayType Via API, job started...")
      val snc : SnappyContext = snappySession.sqlContext
      val spark : SparkSession = SparkSession.builder().getOrCreate()
      val sqlContext = SQLContext.getOrCreate(spark.sparkContext)
      val dataLocation = jobConfig.getString("dataFilesLocation")
      def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
      val outputFile = "ValidateArrayType_Via_API_" +  System.currentTimeMillis()
      val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
      val printDFContent : Boolean = false

      val sncReadDF : DataFrame = snc.read.json(dataLocation)
      val sparkReadDF : DataFrame = spark.read.json(dataLocation)

      println("Started the ArrayType Query 1....")
      val sncSelectDF : DataFrame = sncReadDF.select("*")
      val sparkSelectDF : DataFrame = sparkReadDF.select("*")
      if(printDFContent) {
        println("sncSelectDF count : " +  sncSelectDF.count())
        println("sncSelectDF data : " + sncSelectDF.show())
      }

      println("Starting the ArrayType Query 2....")
      val sncArrQuery2DF : DataFrame = sncReadDF
        .select(col("id"), col("name"), col("marks")(0),
          col("marks").getItem(1), col("marks").getItem(2),
          col("marks")(3), col("marks")(4), col("marks").getItem(5))
        .where(col("id").between(100, 1000))
      val sparkArrQuery2DF : DataFrame = sparkReadDF
        .select(col("id"), col("name"), col("marks")(0),
          col("marks").getItem(1), col("marks").getItem(2),
          col("marks")(3), col("marks")(4), col("marks").getItem(5))
        .where(col("id").between(100, 1000))
      if(printDFContent) {
        println("sncArrQuery2DF count = " +   sncArrQuery2DF.count())
        println("sncArrQuery2DF count= " + sncArrQuery2DF.show())
      }

      println("Starting the ArrayType Query 3....")
      val sncArrQuery3DF : DataFrame = sncReadDF.select(sncReadDF("id"), sncReadDF("name"),
        explode(sncReadDF("marks")).as("marks"))
      val sparkArrQuery3DF : DataFrame = sparkReadDF.select(col("id"), col("name"),
        explode(col("marks")).as("marks"))
      if(printDFContent) {
        println("sncArrQuery3DF count = " + sncArrQuery3DF.count())
        println("sncArrQuery3DF data = " + sncArrQuery3DF.show())
      }

      println("Starting the ArrayType Query 4....")
      val sncArrQuery4DF : DataFrame = sncArrQuery3DF
        .select(sncArrQuery3DF("name"), sncArrQuery3DF("marks"))
        .groupBy(sncArrQuery3DF("name"))
        .agg(sum("marks").as("Total"))
        .orderBy(desc("Total"))
      val sparkArrQuery4DF : DataFrame = sparkArrQuery3DF
        .select(sparkArrQuery3DF("name"), sparkArrQuery3DF("marks"))
        .groupBy(sparkArrQuery3DF("name"))
        .agg(sum("marks").as("Total"))
        .orderBy(desc("Total"))
      if(printDFContent) {
        println("sncArrQuery4DF count : " + sncArrQuery4DF.count())
        println("sncArrQuery4DF data : " + sncArrQuery4DF.show())
      }

      println("Starting the ArrayType Query 5....")
      val sncArrQuery5DF : DataFrame = sncArrQuery3DF
        .select(sncArrQuery3DF("name"), sncArrQuery3DF("marks"))
        .orderBy(sncArrQuery3DF("name"))
        .groupBy(sncArrQuery3DF("name"))
        .agg(max(sncArrQuery3DF("marks")), min(sncArrQuery3DF("marks")))
      val sparkArrQuery5DF : DataFrame = sparkArrQuery3DF
        .select(sparkArrQuery3DF("name"), sparkArrQuery3DF("marks"))
        .orderBy(sparkArrQuery3DF("name"))
        .groupBy(sparkArrQuery3DF("name"))
        .agg(max(sparkArrQuery3DF("marks")), min(sparkArrQuery3DF("marks")))
      if(printDFContent) {
        println("sncArrQuery5DF count = " + sncArrQuery5DF.count())
        println("sncArrQuery5DF data = " + sncArrQuery5DF.show())
      }

      SnappyTestUtils.assertQueryFullResultSet(snc, sncSelectDF, sparkSelectDF,
        "ArrayTypeAPIQuery1", "column" , pw, sqlContext)
      println("Finished the ArrayType Query 1....")
      SnappyTestUtils.assertQueryFullResultSet(snc, sncArrQuery2DF, sparkArrQuery2DF,
        "ArrayTypeAPIQuery2", "column", pw, sqlContext)
      println("Finished the ArrayType Query 2....")
      SnappyTestUtils.assertQueryFullResultSet(snc, sncArrQuery3DF, sparkArrQuery3DF,
        "ArrayTypeAPIQuery3", "column", pw, sqlContext)
      println("Finished the ArrayType Query 3....")
      SnappyTestUtils.assertQueryFullResultSet(snc, sncArrQuery4DF, sparkArrQuery4DF,
        "ArrayTypeAPIQuery4", "column", pw, sqlContext)
      println("Finished the ArrayType Query 4....")
      SnappyTestUtils.assertQueryFullResultSet(snc, sncArrQuery5DF, sparkArrQuery5DF,
        "ArrayTypeAPIQuery5", "column", pw, sqlContext)
      println("Finished the ArrayType Query 5....")

      pw.close()
      println("Query ArrayType Via API, job finished...")
    }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
