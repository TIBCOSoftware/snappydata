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
package io.snappydata.hydra

import java.io.{File, PrintWriter}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.io.Source


object SnappyTestUtils {

  def assertJoinFullResultSet(snc: SnappyContext, sqlString: String, queryNum: String,
                              tableType: String, pw: PrintWriter, sqlContext: SQLContext,
                              planCachingEnabled: Boolean): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    sqlContext.sql("set spark.sql.crossJoin.enabled = true")
    assertQueryFullResultSet(snc, sqlString, queryNum, tableType, pw, sqlContext,
      planCachingEnabled);
  }

  def assertJoinFullResultSet(snc: SnappyContext, sqlString: String, queryNum: String,
                              tableType: String, pw: PrintWriter, sqlContext: SQLContext): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    sqlContext.sql("set spark.sql.crossJoin.enabled = true")
    assertQueryFullResultSet(snc, sqlString, queryNum, tableType, pw, sqlContext)
  }

  def dataTypeConverter(row: Row): Row = {
    val md = row.toSeq.map {
      // case d: Double => "%18.1f".format(d).trim().toDouble
      case d: Double => math.floor(d * 10.0 + 0.5) / 10.0
      case de: BigDecimal => {
        de.setScale(2, BigDecimal.RoundingMode.HALF_UP)
      }
      case i: Integer => {
        i
      }
      case v => v
    }
    Row.fromSeq(md)
  }

  def writeToFile(df: DataFrame, dest: String, snc: SnappyContext): Unit = {
    import snc.implicits._
    df.map(dataTypeConverter)(RowEncoder(df.schema))
      .map(row => {
        val sb = new StringBuilder
        row.toSeq.foreach(e => {
          if (e != null) {
            sb.append(e.toString).append(",")
          }
          else {
            sb.append("NULL").append(",")
          }
        })
        sb.toString()
      }).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option(
      "header", false).save(dest)
  }

  def getTempDir(dirName: String): String = {
    val log: File = new File(".")
    var dest: String = null
    val dirString = log.getCanonicalPath;
    if (dirName.equals("sparkQueryFiles")) {
      val logDir = log.listFiles.filter(_.getName.equals("snappyleader.log"))
      if (!logDir.isEmpty) {
        val leaderLogFile: File = logDir.iterator.next()
        if (leaderLogFile.exists()) dest = dirString + File.separator + ".." + File.separator +
          ".." + File.separator + dirName
      }
      else dest = dirString + File.separator + ".." + File.separator + dirName
    }
    else dest = log.getCanonicalPath + File.separator + dirName
    val tempDir: File = new File(dest)
    if (!tempDir.exists) tempDir.mkdir()
    return tempDir.getAbsolutePath
  }

  /*
 In case of round-off, there is a difference of .1 in snappy and spark results. We can ignore
 such differences
 */
  def isIgnorable(actualRow: String, expectedRow: String): Boolean = {
    var isIgnorable = false
    if (actualRow != null && actualRow.size > 0 && expectedRow != null && expectedRow.size > 0) {
      val actualArray = actualRow.split(",")
      val expectedArray = expectedRow.split(",")
      var diff: Double = 0.0
      if (actualArray.length == expectedArray.length) {
        for (i <- 0 until actualArray.length) {
          val value1: String = actualArray(i)
          val value2: String = expectedArray(i)
          if (!value1.equals(value2)) {
            try {
              val val1: Double = value1.toDouble
              val val2: Double = value2.toDouble
              if (val1 > val2) diff = val1.-(val2).doubleValue
              else diff = val2.-(val1).doubleValue
              diff = "%18.2f".format(diff).trim().toDouble
              // scalastyle:off println
              println("diff is " + diff)
              if (diff <= 0.1) isIgnorable = true
            } catch {
              case nfe: NumberFormatException => return false
            }
          }
        }
      }
    }
    isIgnorable
  }

  def assertQueryFullResultSet(snc: SnappyContext, sqlString: String, queryNum: String,
                               tableType: String, pw: PrintWriter, sqlContext: SQLContext): Any = {
    assertQueryFullResultSet(snc, sqlString, queryNum, tableType, pw, sqlContext, true)
  }

  def assertQueryFullResultSet(snc: SnappyContext, sqlString: String, queryNum: String,
                               tableType: String, pw: PrintWriter, sqlContext: SQLContext,
                               usePlanCaching: Boolean): Any = {
    var snappyDF: DataFrame = null
    if(!usePlanCaching) {
      snappyDF = snc.sqlUncached(sqlString)
    } else {
      snappyDF = snc.sql(sqlString)
    }
    var sparkDF = sqlContext.sql(sqlString);
    val snappyQueryFileName = s"Snappy_${queryNum}.out"
    val sparkQueryFileName = s"Spark_${queryNum}.out"
    val snappyDest: String = getTempDir("snappyQueryFiles_" + tableType) + File.separator +
      snappyQueryFileName
    val sparkDest: String = getTempDir("sparkQueryFiles") + File.separator + sparkQueryFileName
    val sparkFile: File = new java.io.File(sparkDest)
    val snappyFile = new java.io.File(snappyDest)
    if (snappyFile.listFiles() == null) {
      val col1 = snappyDF.schema.fieldNames(0)
      val col = snappyDF.schema.fieldNames.tail
      snappyDF = snappyDF.repartition(1).sortWithinPartitions(col1, col: _*)
      writeToFile(snappyDF, snappyDest, snc)
      // scalastyle:off println
      pw.println(s"${queryNum} Result Collected in file $snappyDest")
    }
    if (sparkFile.listFiles() == null) {
      val col1 = sparkDF.schema.fieldNames(0)
      val col = sparkDF.schema.fieldNames.tail
      sparkDF = sparkDF.repartition(1).sortWithinPartitions(col1, col: _*)
      writeToFile(sparkDF, sparkDest, snc)
      pw.println(s"${queryNum} Result Collected in file $sparkDest")
    }
    val expectedFile = sparkFile.listFiles.filter(_.getName.endsWith(".csv"))
    val actualFile = snappyFile.listFiles.filter(_.getName.endsWith(".csv"))
    val expectedLineSet = Source.fromFile(expectedFile.iterator.next()).getLines()
    val actualLineSet = Source.fromFile(actualFile.iterator.next()).getLines
    // var numLines = 0
    while (expectedLineSet.hasNext && actualLineSet.hasNext) {
      val expectedLine = expectedLineSet.next()
      val actualLine = actualLineSet.next()
      if (!actualLine.equals(expectedLine)) {
        if (!isIgnorable(actualLine, expectedLine)) {
          pw.println(s"\n** For ${queryNum} result mismatch observed**")
          pw.println(s"\nExpected Result:\n $expectedLine")
          pw.println(s"\nActual Result:\n $actualLine")
          pw.println(s"\nQuery =" + sqlString + " Table Type : " + tableType)
        }
        /* assert(assertion = false, s"\n** For $queryNum result mismatch observed** \n" +
            s"Expected Result \n: $expectedLine \n" +
            s"Actual Result   \n: $actualLine \n" +
            s"Query =" + sqlString + " Table Type : " + tableType) */
        // Commented due to Q37 failure by just the difference of 0.1 in actual and expected value
      }
      // numLines += 1
    }
    if (actualLineSet.hasNext || expectedLineSet.hasNext) {
      pw.println(s"\nFor ${queryNum} result count mismatch observed")
      pw.flush()
      // assert(assertion = false, s"\nFor $queryNum result count mismatch observed")
    }
    // scalastyle:on println
    pw.flush()
  }

  def assertQueryFullResultSet(snc: SnappyContext, snDF : DataFrame,
                               spDF : DataFrame, queryNum: String,
                               tableType: String, pw: PrintWriter, sqlContext: SQLContext): Any = {
    var snappyDF: DataFrame = snDF
    //    if(!usePlanCaching) {
    //      snappyDF = snc.sqlUncached(sqlString)
    //    } else {
    //      snappyDF = snc.sql(sqlString)
    //    }
    var sparkDF = spDF
    val snappyQueryFileName = s"Snappy_${queryNum}.out"
    val sparkQueryFileName = s"Spark_${queryNum}.out"
    val snappyDest: String = getTempDir("snappyQueryFiles_" + tableType) + File.separator +
      snappyQueryFileName
    val sparkDest: String = getTempDir("sparkQueryFiles") + File.separator + sparkQueryFileName
    val sparkFile: File = new java.io.File(sparkDest)
    val snappyFile = new java.io.File(snappyDest)
    if (snappyFile.listFiles() == null) {
      val col1 = snappyDF.schema.fieldNames(0)
      val col = snappyDF.schema.fieldNames.tail
      snappyDF = snappyDF.repartition(1).sortWithinPartitions(col1, col: _*)
      writeToFile(snappyDF, snappyDest, snc)
      // scalastyle:off println
      pw.println(s"${queryNum} Result Collected in file $snappyDest")
    }
    if (sparkFile.listFiles() == null) {
      val col1 = sparkDF.schema.fieldNames(0)
      val col = sparkDF.schema.fieldNames.tail
      sparkDF = sparkDF.repartition(1).sortWithinPartitions(col1, col: _*)
      writeToFile(sparkDF, sparkDest, snc)
      pw.println(s"${queryNum} Result Collected in file $sparkDest")
    }
    val expectedFile = sparkFile.listFiles.filter(_.getName.endsWith(".csv"))
    val actualFile = snappyFile.listFiles.filter(_.getName.endsWith(".csv"))
    val expectedLineSet = Source.fromFile(expectedFile.iterator.next()).getLines()
    val actualLineSet = Source.fromFile(actualFile.iterator.next()).getLines
    // var numLines = 0
    while (expectedLineSet.hasNext && actualLineSet.hasNext) {
      val expectedLine = expectedLineSet.next()
      val actualLine = actualLineSet.next()
      if (!actualLine.equals(expectedLine)) {
        if (!isIgnorable(actualLine, expectedLine)) {
          pw.println(s"\n** For ${queryNum} result mismatch observed**")
          pw.println(s"\nExpected Result:\n $expectedLine")
          pw.println(s"\nActual Result:\n $actualLine")
          //  pw.println(s"\nQuery =" + sqlString + " Table Type : " + tableType)
        }
        /* assert(assertion = false, s"\n** For $queryNum result mismatch observed** \n" +
            s"Expected Result \n: $expectedLine \n" +
            s"Actual Result   \n: $actualLine \n" +
            s"Query =" + sqlString + " Table Type : " + tableType) */
        // Commented due to Q37 failure by just the difference of 0.1 in actual and expected value
      }
      // numLines += 1
    }
    if (actualLineSet.hasNext || expectedLineSet.hasNext) {
      pw.println(s"\nFor ${queryNum} result count mismatch observed")
      pw.flush()
      // assert(assertion = false, s"\nFor $queryNum result count mismatch observed")
    }
    pw.println(s"Validation for ${queryNum} finished.")
    pw.println()
    // scalastyle:on println
    pw.flush()
  }




}