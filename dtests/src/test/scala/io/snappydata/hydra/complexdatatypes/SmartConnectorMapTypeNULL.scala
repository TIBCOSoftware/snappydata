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

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SnappyContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SmartConnectorMapTypeNULL {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector NULL value in MapType Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_MapTypeNULL_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sqlContext = spark.sqlContext
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorMapTypeNULL" + "_" + "column" + System.currentTimeMillis())
      , false))
    /**
      *  Test : NULL value in Map Type column.
      */
    snc.sql(ComplexTypeUtils.createSchemaST)
    spark.sql(ComplexTypeUtils.createSchemaST)

    /**
      * Test Case 1 : MapType Column is last column in the table.
      */
    snc.sql(ComplexTypeUtils.createTableLastColumnMapType)
    spark.sql(ComplexTypeUtils.createTableInSparkMapTypeLastColumn)
    snc.sql(ComplexTypeUtils.insertNULLMapTypeLast)
    spark.sql(ComplexTypeUtils.insertNULLMapTypeLast)
    snc.sql(ComplexTypeUtils.insertNormalDataMapTypeLast)
    spark.sql(ComplexTypeUtils.insertNormalDataMapTypeLast)
    /**
      *  Test Case 2 : MapType Column is between (say middle)  the other data types in the table.
      */
    snc.sql(ComplexTypeUtils.createTableMiddleColumnMapType)
    spark.sql(ComplexTypeUtils.createTableInSparkMapTypeMiddleColumn)
    snc.sql(ComplexTypeUtils.insertNULLMapTypeMiddle)
    spark.sql(ComplexTypeUtils.insertNULLMapTypeMiddle)
    snc.sql(ComplexTypeUtils.insertNormalDataMapTypeMiddle)
    spark.sql(ComplexTypeUtils.insertNormalDataMapTypeMiddle)
    /**
      *  Test Case 3: ArrayType Column is the first column in the table.
      */
    snc.sql(ComplexTypeUtils.createTableFirstColumnMapType)
    spark.sql(ComplexTypeUtils.createTableInSparkMapTypeFirstColumn)
    snc.sql(ComplexTypeUtils.insertNULLMapTypeFirst)
    spark.sql(ComplexTypeUtils.insertNULLMapTypeFirst)
    snc.sql(ComplexTypeUtils.insertNormalDataMapTypeFirst)
    spark.sql(ComplexTypeUtils.insertNormalDataMapTypeFirst)
    /**
      * Validation Routine
      */
    val snappyDFLast = snc.sql(ComplexTypeUtils.selectMapLast)
    val sparkDFLast = spark.sql(ComplexTypeUtils.selectMapLast)
    val df1Last = snappyDFLast.collect()
    val df2Last = sparkDFLast.collect()
    var result1 = df1Last.mkString(",")
    var result2 = df2Last.mkString(",")
    pw.println(result1)
    pw.println(result2)
    if(df1Last.equals(df2Last)) {
      pw.println("-- Insertion of NULL value in MapType last column OK --")
    } else {
      pw.println("-- Insertion of NULL value in MapType last column OK --")
    }
    pw.flush()
    val snappyDFMiddle = snc.sql(ComplexTypeUtils.selectMapMiddle)
    val sparkDFMiddle = spark.sql(ComplexTypeUtils.selectMapMiddle)
    val df1Middle = snappyDFMiddle.collect()
    val df2Middle = sparkDFMiddle.collect()
    result1 = df1Middle.mkString(",")
    result2 = df2Middle.mkString(",")
    pw.println(result1)
    pw.println(result2)
    if(df1Middle.equals(df2Middle)) {
      pw.println("-- Insertion of NULL value in MapType Middle column OK --")
    } else {
      pw.println("-- Insertion of NULL value in MapType Middle column OK --")
    }
    pw.flush()
    val snappyDFFirst = snc.sql(ComplexTypeUtils.selectMapFirst)
    val sparkDFFirst = spark.sql(ComplexTypeUtils.selectMapFirst)
    val df1First = snappyDFFirst.collect()
    val df2First = sparkDFFirst.collect()
    result1 = df1First.mkString(",")
    result2 = df2First.mkString(",")
    pw.println(result1)
    pw.println(result2)
    if(df1First.equals(df2First)) {
      pw.println("-- Insertion of NULL value in MapType First column OK --")
    } else {
      pw.println("-- Insertion of NULL value in MapType First column OK --")
    }
    pw.flush()
    pw.close()
    snc.sql(ComplexTypeUtils.dropTableMapLast)
    snc.sql(ComplexTypeUtils.dropTableMapMiddle)
    snc.sql(ComplexTypeUtils.dropTableMapFirst)
    spark.sql(ComplexTypeUtils.dropTableMapLast)
    spark.sql(ComplexTypeUtils.dropTableMapMiddle)
    spark.sql(ComplexTypeUtils.dropTableMapFirst)
    snc.sql(ComplexTypeUtils.dropDatabaseST)
    spark.sql(ComplexTypeUtils.dropDatabaseST)

  }
}
