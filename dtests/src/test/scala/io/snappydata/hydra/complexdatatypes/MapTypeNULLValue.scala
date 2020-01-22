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
import org.apache.spark.sql._

class MapTypeNULLValue extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("Validation for NULL Value in Map Type column Job started...")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sqlContext : SQLContext = spark.sqlContext
    val outputFile = "ValidateMapTypeNULLValue" + "_"  +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false)))

    /**
      *  Test : NULL value in Map Type column.
      */
    snc.sql(ComplexTypeUtils.createSchemaST)
    spark.sql(ComplexTypeUtils.createSchemaST)

    /**
      * Test Case 1 : MapType Column is last column in the table.
      */

    /**
      *  Test Case 2 : ArrayType Column is between (say middle)  the other data types in the table.
      */

    /**
      *  Test Case 3: ArrayType Column is the first column in the table.
      */

  }
}
