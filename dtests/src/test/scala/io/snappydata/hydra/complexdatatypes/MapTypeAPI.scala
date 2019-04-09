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
package io.snappydata.hydra.complexdatatypes

import java.io.{File, FileOutputStream, PrintWriter}
import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

class MapTypeAPI extends SnappySQLJob {
    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
      //  scalastyle:off println
      println("Query MapType Via API, Job Started....")
      val snc : SnappyContext = snappySession.sqlContext
      val spark : SparkSession = SparkSession.builder().getOrCreate()
      val sqlContext : SQLContext = SQLContext.getOrCreate(spark.sparkContext)
      val dataLocation : String = jobConfig.getString("dataFilesLocation")
      def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
      val outputFile = "ValidateMapType_Via_API_" +  System.currentTimeMillis()
      val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
      val printDFContent : Boolean = true

      val sncReadDF : DataFrame = snc.read.json(dataLocation)
      val sparkReadDF : DataFrame = spark.read.json(dataLocation)

//      println("Start the Map Type Query1")
//      val sncSelectDF : DataFrame = sncReadDF.select("*").orderBy("id")
//      val sparkSelectDF : DataFrame = sparkReadDF.select("*").orderBy("id")
//      if(printDFContent) {
//        println("sncSelectDF count : " + sncSelectDF.count())
//        println("sparkSelectDF count : " + sparkSelectDF.count())
//      }

//      println("Start the Map Type Query2")
//      val sncMapQuery2DF : DataFrame = sncReadDF
//          .select(sncReadDF("id"), sncReadDF("name"), sncReadDF("Maths").getItem("maths"),
//            sncReadDF("Science").getField("science"), sncReadDF("English").getItem("english"),
//            sncReadDF("Computer").getItem("computer"), sncReadDF("Music").getField("music"),
//            sncReadDF("History").getItem("history")).filter(sncReadDF("name")==="JxVJBxYlNT")
//      val sparkMapQuery2DF : DataFrame = sparkReadDF
//          .select(sparkReadDF("id"), sparkReadDF("name"), sparkReadDF("Maths").getItem("maths"),
//            sparkReadDF("Science").getField("science"), sparkReadDF("English").getItem("english"),
//            sparkReadDF("Computer").getItem("computer"), sparkReadDF("Music").getField("music"),
//            sparkReadDF("History").getItem("history")).where(sparkReadDF("name")==="JxVJBxYlNT")
//      if(printDFContent) {
//        println("sncMapQuery2DF count : " + sncMapQuery2DF.count())
//        println("sncMapQuery2DF count : " + sncMapQuery2DF.count())
//      }

      println("Start the Map Type Query3")
      val sncMapQuery3DF : DataFrame = sncReadDF
          .select(sncReadDF.agg(sum()))
//      println("Finish the Map Type Query3")
//      println("Start the Map Type Query4")
//      println("Finish the Map Type Query4")
//      println("Start the Map Type Query5")
//      println("Finish the Map Type Query5")
//      println("Start the Map Type Query6")
//      println("Finish the Map Type Query6")

//      SnappyTestUtils.assertQueryFullResultSet(snc, sncSelectDF, sparkSelectDF,
//      "MapTypeQuery1", "column", pw, sqlContext )
//      println("Finish the Map Type Query1")
//      SnappyTestUtils.assertQueryFullResultSet(snc, sncMapQuery2DF, sparkMapQuery2DF,
//      "MapTypeQuery2", "column", pw,sqlContext)
//      println("Finish the Map Type Query2")

      println("Query MapType Via API, Job Completed....")
    }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
