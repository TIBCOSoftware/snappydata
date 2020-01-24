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
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SnappyContext, SparkSession}

object SmartConnectorArrayTypeNULL {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector NULL value in ArraysType Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_ArrayTypeNULL_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sqlContext = spark.sqlContext

//    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
//    val dataLocation = args(0)
//    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorArrayTypeNULL" + "_" + "column" + System.currentTimeMillis())
      , false))

    /**
      *  Test : NULL value in Array Type column.
      */
    snc.sql(ComplexTypeUtils.createSchemaST)
    spark.sql(ComplexTypeUtils.createSchemaST)
    /**
      * Test Case 1 : ArrayType Column is last column in the table.
      */
    snc.sql(ComplexTypeUtils.createTableLastColumnArrayType)
    spark.sql(ComplexTypeUtils.createTableInSparkArrTypeLastColumn)
    snc.sql(ComplexTypeUtils.insertNullInLastColumn)
    spark.sql(ComplexTypeUtils.insertNullInLastColumn)
    snc.sql(ComplexTypeUtils.insertNormalDataLastColumn)
    spark.sql(ComplexTypeUtils.insertNormalDataLastColumn)
    /**
      *  Test Case 2 : ArrayType Column is between (say middle)  the other data types in the table.
      */
    snc.sql(ComplexTypeUtils.createTableMiddleColumnArrayType)
    spark.sql(ComplexTypeUtils.createTableInSparkArrayTypeMiddleColumn)
    snc.sql(ComplexTypeUtils.insertNullInMiddleColumn)
    spark.sql(ComplexTypeUtils.insertNullInMiddleColumn)
    snc.sql(ComplexTypeUtils.insertNormalDataMiddleColumn)
    spark.sql(ComplexTypeUtils.insertNormalDataMiddleColumn)
    /**
      *  Test Case 3: ArrayType Column is the first column in the table.
      */
    snc.sql(ComplexTypeUtils.createTableFirstColumnArrayType)
    spark.sql(ComplexTypeUtils.createTableInSparkArrayTypeFirstColumn)
    snc.sql(ComplexTypeUtils.insertNullInFirstColumn)
    spark.sql(ComplexTypeUtils.insertNullInFirstColumn)
    snc.sql(ComplexTypeUtils.insertNormalDataFirstColumn)
    spark.sql(ComplexTypeUtils.insertNormalDataFirstColumn)
    /**
      * Validation Routine
      */
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectLastColumn,
      "smartConnectorAQ1", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType last column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectMiddleColumn,
      "smartConnectorAQ2", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType middle column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectFirstColumn,
      "smartConnectorAQ3", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType first column OK --")
    pw.flush()
    pw.close()

    snc.sql(ComplexTypeUtils.dropTableStudentLast)
    snc.sql(ComplexTypeUtils.dropTableStudentMiddle)
    snc.sql(ComplexTypeUtils.dropTableStudentMiddle)
    spark.sql(ComplexTypeUtils.dropTableStudentLast)
    spark.sql(ComplexTypeUtils.dropTableStudentMiddle)
    spark.sql(ComplexTypeUtils.dropTableStudentLast)
    snc.sql(ComplexTypeUtils.dropDatabaseST)
    spark.sql(ComplexTypeUtils.dropDatabaseST)
  }
}
