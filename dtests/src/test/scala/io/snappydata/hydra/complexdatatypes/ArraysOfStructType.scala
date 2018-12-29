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
import io.snappydata.hydra.SnappyTestUtils
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class ArraysOfStructType extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("ArraysofStruct Type Job started...")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory : String = new File(".").getCanonicalPath
    val outputFile : String = "ValidateArraysOfStructType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val printContent : Boolean = false

    val ArraysOfStruct_Q1 = "SELECT * FROM TW.TwoWheeler"
    val ArraysOfStruct_Q2 = "SELECT brand FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].type = 'Scooter'"
    val ArraysOfStruct_Q3 = "SELECT brand,BikeInfo[0].cc FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].cc >= 149.0 ORDER BY BikeInfo[0].cc DESC"
    val ArraysOfStruct_Q4 = "SELECT brand,COUNT(BikeInfo[0].type) FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].type = 'Cruiser' GROUP BY brand"
    val ArraysOfStruct_Q5 = "SELECT brand, BikeInfo[0].type AS Style, " +
      "BikeInfo[0].instock AS Available " +
      "FROM TW.TwoWheeler"

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA TW")

    snc.sql("CREATE TABLE IF NOT EXISTS TW.TwoWheeler " +
      "(brand String, " +
      "BikeInfo ARRAY< STRUCT <type:String,cc:Double,noofgears:BigInt,instock:Boolean>>) " +
      "USING column")

    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Honda',ARRAY(STRUCT('Street Bike',149.1,5,false))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'TVS',ARRAY(STRUCT('Scooter',110,0,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Honda',ARRAY(STRUCT('Scooter',109.19,0,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Royal Enfield',ARRAY(STRUCT('Cruiser',346.0,5,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Suzuki', ARRAY(STRUCT('Cruiser',154.9,5,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Yamaha', ARRAY(STRUCT('Street Bike',149,5,false))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Bajaj',ARRAY(STRUCT('Street Bike',220.0,5,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Kawasaki',ARRAY(STRUCT('Sports Bike',296.0,5,false))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Vespa',ARRAY(STRUCT('Scooter',125.0,0,true))")
    snc.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Mahindra',ARRAY(STRUCT('Scooter',109.0,0,false))")

    snc.sql(ArraysOfStruct_Q1)
    snc.sql(ArraysOfStruct_Q2)
    snc.sql(ArraysOfStruct_Q3)
    snc.sql(ArraysOfStruct_Q4)
    snc.sql(ArraysOfStruct_Q5)

    if(printContent) {
      println("snc : ArraysOfStruct_Q2  " + (snc.sql(ArraysOfStruct_Q2).show()))
      println("snc : ArraysOfStruct_Q3  " + (snc.sql(ArraysOfStruct_Q3).show()))
      println("snc : ArraysOfStruct_Q4  " + (snc.sql(ArraysOfStruct_Q4).show()))
      println("snc : ArraysOfStruct_Q5  " + (snc.sql(ArraysOfStruct_Q5).show()))
    }

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA TW")

    spark.sql("CREATE TABLE IF NOT EXISTS TW.TwoWheeler " +
      "(brand String, " +
      "BikeInfo ARRAY< STRUCT <type:String,cc:Double,noofgears:BigInt,instock:Boolean>>) " +
      "USING PARQUET")

    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Honda',ARRAY(STRUCT('Street Bike',149.1,5,false))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'TVS',ARRAY(STRUCT('Scooter',110,0,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Honda',ARRAY(STRUCT('Scooter',109.19,0,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Royal Enfield',ARRAY(STRUCT('Cruiser',346.0,5,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Suzuki', ARRAY(STRUCT('Cruiser',154.9,5,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Yamaha', ARRAY(STRUCT('Street Bike',149,5,false))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Bajaj',ARRAY(STRUCT('Street Bike',220.0,5,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Kawasaki',ARRAY(STRUCT('Sports Bike',296.0,5,false))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Vespa',ARRAY(STRUCT('Scooter',125.0,0,true))")
    spark.sql("INSERT INTO TW.TwoWheeler " +
      "SELECT 'Mahindra',ARRAY(STRUCT('Scooter',109.0,0,false))")

    spark.sql(ArraysOfStruct_Q1)
    spark.sql(ArraysOfStruct_Q2)
    spark.sql(ArraysOfStruct_Q3)
    spark.sql(ArraysOfStruct_Q4)
    spark.sql(ArraysOfStruct_Q5)

    if(printContent) {
      println("spark : ArraysOfStruct_Q2  " + (snc.sql(ArraysOfStruct_Q2).show()))
      println("spark : ArraysOfStruct_Q3  " + (snc.sql(ArraysOfStruct_Q3).show()))
      println("spark : ArraysOfStruct_Q4  " + (snc.sql(ArraysOfStruct_Q4).show()))
      println("spark : ArraysOfStruct_Q5  " + (snc.sql(ArraysOfStruct_Q5).show()))
    }

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, ArraysOfStruct_Q1, "ArraysOfStruct_Q1",
      "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ArraysOfStruct_Q2, "ArraysOfStruct_Q2",
      "column", pw, sqlContext)
    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
//    SnappyTestUtils.assertQueryFullResultSet(snc, ArraysOfStruct_Q3, "ArraysOfStruct_Q3",
//    "column", pw, sqlContext)
//    SnappyTestUtils.assertQueryFullResultSet(snc, ArraysOfStruct_Q4, "ArraysOfStruct_Q4",
//    "column", pw, sqlContext)
//    SnappyTestUtils.assertQueryFullResultSet(snc, ArraysOfStruct_Q5, "ArraysOfStruct_Q5",
//    "column", pw, sqlContext)

    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS TW.TwoWheeler")
    spark.sql("DROP TABLE IF EXISTS TW.TwoWheeler")
    snc.sql("DROP SCHEMA IF EXISTS TW")
    spark.sql("DROP SCHEMA IF EXISTS TW")
  }
}
