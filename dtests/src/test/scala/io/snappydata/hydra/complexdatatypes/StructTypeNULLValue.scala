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

class StructTypeNULLValue extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("Validation for NULL Value in Struct Type column Job started...")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sqlContext : SQLContext = spark.sqlContext
    val outputFile = "ValidateStructTypeNULLValue" + "_"  +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    /**
      *  Test : NULL value in Map Type column.
      */
    snc.sql(ComplexTypeUtils.createSchemaST)
    spark.sql(ComplexTypeUtils.createSchemaST)
    /**
      * Test Case 1 : Struct Type Column is last column in the table.
      */
      snc.sql(ComplexTypeUtils.createTableLastColumnStructType)
      spark.sql(ComplexTypeUtils.createTableInSparkStructTypeLastColumn)
      snc.sql(ComplexTypeUtils.insertNULLStructTypeLast)
      spark.sql(ComplexTypeUtils.insertNULLStructTypeLast)
      snc.sql(ComplexTypeUtils.insertNormalDataStructTypeLast)
      spark.sql(ComplexTypeUtils.insertNormalDataStructTypeLast)
    /**
      *  Test Case 2 : StructType Column is between (say middle)  the other data types in the table.
      */
      snc.sql(ComplexTypeUtils.createTableMiddleColumnStructType)
      spark.sql(ComplexTypeUtils.createTableInSparkStructTypeMiddleColumn)
      snc.sql(ComplexTypeUtils.insertNULLStructTypeMiddle)
      spark.sql(ComplexTypeUtils.insertNULLStructTypeMiddle)
      snc.sql(ComplexTypeUtils.insertNormalDataStructTypeMiddle)
      spark.sql(ComplexTypeUtils.insertNormalDataStructTypeMiddle)
    /**
      *  Test Case 3: StructType Column is the first column in the table.
      */
      snc.sql(ComplexTypeUtils.createTableFirstColumnStructType)
      spark.sql(ComplexTypeUtils.createTableInSparkStructTypeFirstColumn)
      snc.sql(ComplexTypeUtils.insertNULLStructTypeFirst)
      spark.sql(ComplexTypeUtils.insertNULLStructTypeFirst)
      snc.sql(ComplexTypeUtils.insertNormalDataStructTypeFirst)
      spark.sql(ComplexTypeUtils.insertNormalDataStructTypeFirst)
    /**
      * Validation Routine
      *
      */
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectStructLast,
      "SQ1", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in StructType last column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectStructMiddle,
      "SQ2", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in StructType middle column OK --")
    pw.flush()
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.selectStructFirst,
      "SQ3", "column", pw, sqlContext)
    pw.println("-- Insertion of NULL value in StructType first column OK --")
    pw.flush()
    pw.close()
    snc.sql(ComplexTypeUtils.dropTableStructLast)
    snc.sql(ComplexTypeUtils.dropTableStructMiddle)
    snc.sql(ComplexTypeUtils.dropTableStructFirst)
    spark.sql(ComplexTypeUtils.dropTableStructLast)
    spark.sql(ComplexTypeUtils.dropTableStructMiddle)
    spark.sql(ComplexTypeUtils.dropTableStructFirst)
    snc.sql(ComplexTypeUtils.dropDatabaseST)
    spark.sql(ComplexTypeUtils.dropDatabaseST)
  }
}
