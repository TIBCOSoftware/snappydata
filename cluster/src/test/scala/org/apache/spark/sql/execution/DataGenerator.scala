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
package org.apache.spark.sql.execution

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Utility to generate data given a schema. This is a very primitive version.
  * Can be enhanced to cater to joins/group by etc.
  */
object DataGenerator {

  val numVals = 100
  val numTypes = 100
  val numRoles = 100
  val numGroups = 100
  val numNames = 1000
  val numIds = 5000

  val numIters = 10

  val numElems1 = 12 * numVals * numIds
  val numElems2 = 4 * numIds
  val numElems3 = numTypes * numTypes

  def generateDataFrame(sc: SparkSession, schema: StructType, numRows: Long): DataFrame = {
    val rows = schema.fields.zipWithIndex.map { case (f, i) =>
      randomValue(f.dataType, i)
    }
    sc.range(numRows).selectExpr(rows: _*)
  }

  def randomValue(fieldType: DataType, index: Int): String = {
    fieldType match {
      case IntegerType => s"(id % $numIds) as intval$index"
      case ByteType => s"cast((id % $numTypes) as byte) as byteval$index"
      case LongType => s"cast((id % $numTypes) as long) as longval$index"
      case _: DecimalType => s"cast ((rand() * 100.0) as decimal(28, 10) as decval$index"
      case DoubleType => s"cast((id % $numTypes) as double) as doubleval$index"
      case TimestampType => s"((id % 2) + 2014) as timeval$index"
      case BooleanType => s"((id % 2) == 0) as boolval$index"
      case StringType => s"cast((id % $numNames) as string) as strval$index"
      case other: DataType =>
        throw new UnsupportedOperationException(s"Unexpected data type $other")
    }
  }
}
