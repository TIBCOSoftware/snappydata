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

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object TestUtil {
  var validateFullResultSet: Boolean = false;
  var tableType: String = null

  /*
  Executes the join query, matches the result with expected result, returns false if the query
  validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext, skipNumRowsValidation: Boolean): Boolean = {
    var hasValidationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    if (!skipNumRowsValidation) {
      val df = snc.sql(sqlString)
      pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.count} for ${tableType} table")
      if (df.count() != numRows) {
        pw.println(s"Result mismatch for join query ${queryNum} : found ${df.count} rows but expected ${numRows} rows.")
        hasValidationFailed = true
      }
      pw.flush()
    }
    if (validateFullResultSet)
      hasValidationFailed = assertValidateFullResultSet(snc, sqlString, queryNum, pw, sqlContext,
        hasValidationFailed)

    if (hasValidationFailed)
      pw.println(s"Failed Query =" + sqlString + " Table Type : " + tableType + "\n")
    pw.flush()
    return hasValidationFailed
  }

  /*
   Executes the query, matches the result with expected result, returns false if the query
   validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext, skipNumRowsValidation: Boolean): Boolean = {

    var hasValidationFailed = false
    if (!skipNumRowsValidation) {
      val df = snc.sql(sqlString)
      pw.println(s"No. rows in resultset for query ${queryNum} is : ${df.count} for ${tableType} table")
      if (df.count() != numRows) {
        pw.println(s"Result mismatch for query ${queryNum} : found ${df.count} rows but expected ${numRows} rows.")
        hasValidationFailed = true
      }
      pw.flush()
    }
    if (validateFullResultSet)
      hasValidationFailed = assertValidateFullResultSet(snc, sqlString, queryNum, pw, sqlContext,
        hasValidationFailed)

    if (hasValidationFailed)
      pw.println(s"Failed Query : " + sqlString + "\n Table Type : " + tableType + "\n")
    pw.flush()
    return hasValidationFailed
  }

  /*
  Performs full resultSet validation for snappy results for the query against snappy spark
  resultset.
   */
  def assertValidateFullResultSet(snc: SnappyContext, sqlString: String, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext, validationFailed: Boolean): Boolean = {
    var hasValidationFailed = validationFailed

    val snappyQueryFileName = s"Snappy_${queryNum}"
    val snappyDest: String = SnappyTestUtils.getTempDir("snappyQueryFiles") + File.separator +
        snappyQueryFileName
    pw.println(snappyDest)
    val snappyFile: File = new java.io.File(snappyDest)
    var snappyDF = snc.sql(sqlString)

    val sparkQueryFileName = s"Spark_${queryNum}"
    val sparkDest: String = SnappyTestUtils.getTempDir("sparkQueryFiles") + File.separator + sparkQueryFileName
    pw.println(sparkDest)
    val sparkFile: File = new java.io.File(sparkDest)
    var sparkDF = sqlContext.sql(sqlString)

    try {
      if (!snappyFile.exists()) {
        val snap_col1 = snappyDF.schema.fieldNames(0)
        val snap_col = snappyDF.schema.fieldNames.filter(!_.equals(snap_col1)).toSeq
        snappyDF = snappyDF.repartition(1).sortWithinPartitions(snap_col1, snap_col: _*)
        SnappyTestUtils.writeToFile(snappyDF, snappyDest, snc)
        //writeResultSetToCsv(snappyDF, snappyFile)
        pw.println(s"${queryNum} Result Collected in file ${snappyDest}")
      }
      if (!sparkFile.exists()) {
        val col1 = sparkDF.schema.fieldNames(0)
        val col = sparkDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
        sparkDF = sparkDF.repartition(1).sortWithinPartitions(col1, col: _*)
        SnappyTestUtils.writeToFile(sparkDF, sparkDest, snc)
        //writeResultSetToCsv(sparkDF, sparkFile)
        pw.println(s"${queryNum} Result Collected in file ${sparkDest}")
      }
      hasValidationFailed = compareFiles(snappyFile, sparkFile, pw, hasValidationFailed)
    } catch {
      case ex: Exception => {
        hasValidationFailed = true
        pw.println(s"Full resultSet Validation failed for ${queryNum} with following exception:\n")
        ex.printStackTrace(pw)
      }
    }
    pw.flush()
    return hasValidationFailed
  }

  /*
  Writes the query resultset to a csv file.
   */
  def writeResultSetToCsv(df: DataFrame, destFile: File): Unit = {
    val parent = destFile.getParentFile
    if (!parent.exists()) {
      parent.mkdirs()
    }
    implicit val encoder = RowEncoder(df.schema)
    df.mapPartitions(rows => {
      val sb: StringBuilder = new StringBuilder()
      val pw = new PrintWriter(destFile)
      try {
        rows.foreach { row =>
          row.toSeq.foreach {
            case d: Double =>
              // round to one decimal digit
              sb.append(math.floor(d * 10.0 + 0.5) / 10.0).append(',')
            case bd: java.math.BigDecimal =>
              sb.append(bd.setScale(2, java.math.RoundingMode.HALF_UP)).append(',')
            case v => sb.append(v).append(',')
          }
          val len = sb.length
          if (len > 0) sb.setLength(len - 1)
          sb.append('\n')
          if (sb.length >= 1048576) {
            pw.append(sb)
            pw.flush()
            sb.clear()
          }
        }
        if (sb.nonEmpty) {
          pw.append(sb)
          pw.flush()
        }
      }
      finally {
        pw.close()
      }
      Iterator.empty
    }).collect()
  }

  /*
  Returns the path for the directory where the output of resultset of queries have been saved.
  Creates a new directory, if not already existing
   */
  def getQueryResultDir(dirName: String): String = {
    val log: File = new File(".")
    var dest: String = null
    val dirString = log.getCanonicalPath;
    if (dirName.equals("sparkQueryFiles")) {
      val logDir = log.listFiles.filter(_.getName.equals("snappyleader.log"))
      if (!logDir.isEmpty) {
        val leaderLogFile: File = logDir.iterator.next()
        if (leaderLogFile.exists())
          dest = dirString + File.separator + ".." + File.separator + ".." + File.separator + dirName
      }
      else dest = dirString + File.separator + ".." + File.separator + dirName
    }
    else dest = log.getCanonicalPath + File.separator + dirName
    val queryResultDir: File = new File(dest)
    if (!queryResultDir.exists)
      queryResultDir.mkdir()
    return queryResultDir.getAbsolutePath
  }

  /*
  Performs full resultSet validation from snappy for a select query against results in a goldenFile.
   */
  def assertValidateFullResultSetFromGoldenFile(sqlString: String, queryNum: String, tableType:
  String, snc: SnappyContext, pw: PrintWriter, validationFailed: Boolean, goldenFileDest: String):
  Boolean = {
    var hasValidationFailed = validationFailed

    val snappyQueryFileName = s"Snappy_${queryNum}"
    val snappyDest: String = SnappyTestUtils.getTempDir("snappyQueryFiles") + File.separator + snappyQueryFileName
    pw.println(snappyDest)
    val snappyFile: File = new java.io.File(snappyDest)
    var snappyDF = snc.sql(sqlString)

    pw.println(goldenFileDest)
    val goldenFileName = goldenFileDest + File.separator + s"Spark_$queryNum"
    val sortedGoldenDest = goldenFileDest + File.separator + s"Sorted_$queryNum"
    val sortedGoldenFile: File = new java.io.File(sortedGoldenDest)
    val goldenFile: File = new java.io.File(goldenFileName)

    try {
      if (!snappyFile.exists()) {
        val snap_col1 = snappyDF.schema.fieldNames(0)
        val snap_col = snappyDF.schema.fieldNames.filter(!_.equals(snap_col1)).toSeq
        snappyDF = snappyDF.repartition(1).sortWithinPartitions(snap_col1, snap_col: _*)
        SnappyTestUtils.writeToFile(snappyDF, snappyDest, snc)
        //writeResultSetToCsv(snappyDF, snappyFile)
        pw.println(s"${queryNum} Result Collected in file $snappyDest")
      }
      if (!goldenFile.exists()) {
        pw.println(s"Did not find any golden file for query $queryNum")
        throw new Exception(s"Did not find any golden file for query $queryNum")
      } else if (goldenFile.length() > 0) {
        // sort the contents of golden file before comparing results
        var goldenDF = snc.read.format("com.databricks.spark.csv")
            .option("header", "false").option("inferSchema", "true").option("nullValue", "NULL")
            .load(goldenFileName)
        val col1 = goldenDF.schema.fieldNames(0)
        val col = goldenDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
        goldenDF = goldenDF.repartition(1).sortWithinPartitions(col1, col: _*)
        SnappyTestUtils.writeToFile(goldenDF, sortedGoldenDest, snc)
        //writeResultSetToCsv(goldenDF, sortedGoldenFile)
        pw.println(s"${queryNum} Result Collected in file ${sortedGoldenDest}")
      } else {
        pw.println(s"zero results in query $queryNum.")
      }
      hasValidationFailed = compareFiles(snappyFile, sortedGoldenFile, pw, hasValidationFailed)

    } catch {
      case ex: Exception => {
        hasValidationFailed = true
        pw.println(s"Full resultSet Validation failed for ${queryNum} with following exception:\n")
        ex.printStackTrace(pw)
      }
    }
    pw.flush()
    return hasValidationFailed
  }

  def compareFiles(snappyFile: File, sparkFile: File, pw: PrintWriter, validationFailed: Boolean):
  Boolean = {
    var hasValidationFailed = validationFailed
    val expectedFile = sparkFile.listFiles.filter(_.getName.endsWith(".csv"))
    val actualFile = snappyFile.listFiles.filter(_.getName.endsWith(".csv"))
    val expectedLineSet = Source.fromFile(expectedFile.iterator.next()).getLines()
    val actualLineSet = Source.fromFile(actualFile.iterator.next()).getLines()

    while (expectedLineSet.hasNext && actualLineSet.hasNext) {
      val expectedLine = expectedLineSet.next()
      val actualLine = actualLineSet.next()
      if (!actualLine.equals(expectedLine)) {
        hasValidationFailed = true
        pw.println(s"Expected Result : $expectedLine")
        pw.println(s"Actual Result   : $actualLine")
      }
    }
    if (actualLineSet.hasNext || expectedLineSet.hasNext) {
      hasValidationFailed = true
      if (actualLineSet.hasNext)
        pw.println(s"Following ${actualLineSet.size} rows are unexpected in Snappy:")
      while (actualLineSet.hasNext)
        pw.println(actualLineSet.next())
      if (expectedLineSet.hasNext)
        pw.println(s"Following ${expectedLineSet.size} rows are missing in Snappy:")
      while (expectedLineSet.hasNext)
        pw.println(expectedLineSet.next())
    }
    hasValidationFailed
  }

  /*
    If validation has failed for a query, add the query number to failedQueries String
   */
  def addToFailedQueryList(failedQueries: String, queryNum: String): String = {
    var str = failedQueries
    if (str.isEmpty)
      str = queryNum
    else
      str = str + "," + queryNum
    return str
  }

}
