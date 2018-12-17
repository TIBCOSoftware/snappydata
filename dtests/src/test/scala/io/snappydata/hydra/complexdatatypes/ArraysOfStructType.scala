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

    val Q1 = "SELECT * FROM TW.TwoWheeler"
    val Q2 = "SELECT brand FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].type = 'Scooter'"
    val Q3 = "SELECT brand,BikeInfo[0].cc FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].cc >= 149.0 "
    val Q4 = "SELECT brand,COUNT(BikeInfo[0].type) FROM TW.TwoWheeler " +
      "WHERE BikeInfo[0].type = 'Cruiser' GROUP BY brand"
    val Q5 = "SELECT brand, BikeInfo[0].type AS Style, BikeInfo[0].instock AS Available " +
      "FROM TW.TwoWheeler"

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA TW")

    snc.sql("CREATE TABLE TW.TwoWheeler " +
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

    snc.sql(Q1)
    snc.sql(Q2)
    // scalastyle:off println
    println("snc : Q2  " + (snc.sql(Q2).show()))
    snc.sql(Q3)
    // scalastyle:off println
    println("snc : Q3  " + (snc.sql(Q3).show()))
    snc.sql(Q4)
    // scalastyle:off println
    println("snc : Q4  " + (snc.sql(Q4).show()))
    snc.sql(Q5)
    // scalastyle:off println
    println("snc : Q5  " + (snc.sql(Q5).show()))

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA TW")

    spark.sql("CREATE TABLE TW.TwoWheeler " +
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

    spark.sql(Q1)
    spark.sql(Q2)
    // scalastyle:off println
    println("spark : Q2  " + (snc.sql(Q2).show()))
    spark.sql(Q3)
    // scalastyle:off println
    println("spark : Q3  " + (snc.sql(Q3).show()))
    spark.sql(Q4)
    // scalastyle:off println
    println("spark : Q4  " + (snc.sql(Q4).show()))
    spark.sql(Q5)
    // scalastyle:off println
    println("spark : Q5  " + (snc.sql(Q5).show()))

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, Q1, "Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q2, "Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q3, "Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q4, "Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q5, "Q5", "column", pw, sqlContext)

    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS TW.TwoWheeler")
    spark.sql("DROP TABLE IF EXISTS TW.TwoWheeler")
    snc.sql("DROP SCHEMA TW")
    spark.sql("DROP SCHEMA TW")
  }
}
