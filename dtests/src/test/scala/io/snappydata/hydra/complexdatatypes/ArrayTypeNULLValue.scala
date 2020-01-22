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

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql._

class ArrayTypeNULLValue extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("ArraysTypeNULLValue Job started...")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sqlContext = SQLContext.getOrCreate(spark.sparkContext)
    //  def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateArrayTypeNULLValue" + "_"  +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))

    /**
      *  Test : NULL value in Complex Type column.
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
      "Q1", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType last column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectMiddleColumn,
      "Q2", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType middle column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectFirstColumn,
      "Q3", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in ArrayType first column OK --")
    pw.flush()
    pw.close()

    snc.sql(ComplexTypeUtils.dropTableStudentLast)
    spark.sql(ComplexTypeUtils.dropTableStudentLast)
    snc.sql(ComplexTypeUtils.dropTableStudentMiddle)
    snc.sql(ComplexTypeUtils.dropTableStudentMiddle)
    snc.sql(ComplexTypeUtils.dropDatabaseST)
    spark.sql(ComplexTypeUtils.dropDatabaseST)
  }
}
