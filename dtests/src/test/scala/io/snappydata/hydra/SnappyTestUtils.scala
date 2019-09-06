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

package io.snappydata.hydra

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.sql.Timestamp
import java.util
import java.util.Collections

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object SnappyTestUtils {

  var validateFullResultSet: Boolean = false;
  var numRowsValidation: Boolean = false;
  var tableType: String = null

  def logTime: String = {
    "[" + new Timestamp(System.currentTimeMillis()).toString + "] "
  }

  /*
  Executes the join query, matches only the full result with expected result, returns false if the
  query validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, queryNum: String, pw: PrintWriter,
      sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    numRowsValidation = false
    validationFailed = assertJoin(snc, sqlString, 0, queryNum, pw, sqlContext)
    return validationFailed
  }

  /*
  Executes the join query, matches the result with expected result, returns false if the query
  validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    if (validateFullResultSet) {
      sqlContext.sql("set spark.sql.crossJoin.enabled = true")
    }
    validationFailed = assertQuery(snc, sqlString, numRows, queryNum, pw, sqlContext)
    return validationFailed
  }

  /*
   Executes the query, matches only the full resultSet with expected result, returns false if the
   query validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext): Boolean = {
    numRowsValidation = false
    assertQuery(snc, sqlString, 0, queryNum, pw, sqlContext)
  }

  /*
   Executes the query, matches the result with expected result, returns false if the query
   validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    var snappyDF: DataFrame = null
    snappyDF = snc.sql(sqlString)
    val snappyDFCount = snappyDF.count
    // scalastyle:off println
    pw.println(s"\n${logTime} Executing Query $queryNum ...")
    println(s"Query $queryNum")
    snappyDF.explain(true)
    if (numRowsValidation) {
      pw.println(s"${logTime} Query ${queryNum} returned ${snappyDFCount} rows for ${tableType}")
      if (snappyDFCount != numRows) {
        pw.println(s"${logTime} Result mismatch for query ${queryNum} found ${snappyDFCount} rows" +
            s"but expected ${numRows} rows.")
        validationFailed = true
      }
      pw.flush()
    }
    var fullRSValidationFailed: Boolean = false
    if (validateFullResultSet) {
      val sparkDF = sqlContext.sql(sqlString)
      val sparkDFCount = sparkDF.count()
      if(snappyDFCount != sparkDFCount) {
        pw.println(s"Count difference observed in snappy and spark resultset for query " +
            s"${queryNum}. Snappy returned ${snappyDFCount} and spark returned ${sparkDFCount}.")
        fullRSValidationFailed = true
      }
      fullRSValidationFailed = assertQuery(snc, snappyDF, sparkDF, queryNum, pw)
    }
    if (validationFailed) {
      pw.println(s"\n${logTime} NumRows validation failed for query ${queryNum} on ${tableType} " +
          s"table.")
    }
    if (fullRSValidationFailed) {
      pw.println(s"\n${logTime} Full resultset validation failed for query ${queryNum} on " +
          s"${tableType} table.")
      validationFailed = true
    }
    pw.println(s"${logTime} Execution completed for query ${queryNum}")
    pw.flush()
    return validationFailed
  }

  def assertQuery(snc: SnappyContext, snappyDF: DataFrame, sparkDF: DataFrame, queryNum: String,
      pw: PrintWriter): Boolean = {
    var fullRSValidationFailed = false
    val snappyQueryFileName = s"Snappy_${queryNum}"
    val snappyDest: String = getQueryResultDir("snappyResults") +
        File.separator + snappyQueryFileName
    // scalastyle:off println
    // pw.println(s"Snappy query results are at : ${snappyDest}")
    val snappyFile: File = new java.io.File(snappyDest)

    val sparkQueryFileName = s"Spark_${queryNum}"
    val sparkDest: String = getQueryResultDir("sparkResults") + File.separator +
        sparkQueryFileName
    // pw.println(s"Spark query results are at : ${sparkDest}")
    val sparkFile: File = new java.io.File(sparkDest)
    try {
      if (!snappyFile.exists()) {
        // val snap_col1 = snappyDF.schema.fieldNames(0)
        // val snap_col = snappyDF.schema.fieldNames.filter(!_.equals(snap_col1)).toSeq
        // snappyDF.repartition(1).sortWithinPartitions(snap_col1, snap_col: _*)
        writeToFile(snappyDF.repartition((1)), snappyDest, snc)
        pw.println(s"${logTime} Snappy result collected in : ${snappyDest}")
      }
      if (!sparkFile.exists()) {
        // val col1 = sparkDF.schema.fieldNames(0)
        // val col = sparkDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
        // sparkDF.repartition(1).sortWithinPartitions(col1, col: _*)
        writeToFile(sparkDF.repartition(1), sparkDest, snc)
        pw.println(s"${logTime} Spark result collected in : ${sparkDest}")
      }
      val missingDF = sparkDF.except(snappyDF).collectAsList()
      val unexpectedDF = snappyDF.except(sparkDF).collectAsList()
      if(missingDF.size() > 0 || unexpectedDF.size() > 0) {
        fullRSValidationFailed = true
        pw.println("Found mismatch in resultset")
        if(missingDF.size() > 0) {
          pw.println(s"The following ${missingDF.size} rows were missing in snappyDF:\n " +
              missingDF.forEach(println))
        }
        if(unexpectedDF.size() > 0) {
          pw.println(s"The following ${unexpectedDF.size} rows were unexpected in snappyDF:\n" +
              missingDF.forEach(println))
        }
      }
      // fullRSValidationFailed
      //    = compareFiles(snappyFile, sparkFile, pw, queryNum, fullRSValidationFailed)
    } catch {
      case ex: Exception => {
        fullRSValidationFailed = true
        pw.println(s"${logTime} Full resultSet validation for ${queryNum} got the following " +
            s"exception:\n")
        ex.printStackTrace(pw)
      }
    }
    pw.flush()
    fullRSValidationFailed
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

  /*
   Writes the query resultset to a csv file.
   */
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
        if (leaderLogFile.exists()) {
          dest = dirString + File.separator + ".." + File.separator + ".." + File.separator +
              dirName
        }
      }
      else dest = dirString + File.separator + ".." + File.separator + dirName
    }
    else dest = log.getCanonicalPath + File.separator + dirName
    val queryResultDir: File = new File(dest)
    if (!queryResultDir.exists) {
      queryResultDir.mkdir()
    }
    return queryResultDir.getAbsolutePath
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

  def executeProcess(pb: ProcessBuilder, logFile: File, pw: PrintWriter): Int = {
    var p: Process = null
    try {
      if (logFile != null) {
        pb.redirectErrorStream(true)
        pb.redirectError(ProcessBuilder.Redirect.PIPE)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
      }
      p = pb.start
      if (logFile != null) {
        assert(pb.redirectInput eq ProcessBuilder.Redirect.PIPE)
        assert(pb.redirectOutput.file eq logFile)
        assert(p.getInputStream.read == -1)
      }
      val rc = p.waitFor
      val pbCmd = util.Arrays.toString(pb.command.toArray)
      if ((rc == 0) || (pbCmd.contains("grep -v -F") && rc == 1)) {
        println("Process executed successfully")
        0
      }
      else {
        println("Process execution failed with exit code: " + rc)
        1
      }
    } catch {
      case e: IOException =>
        println("Exception occurred while starting the process:" + pb + "\nError Message:" + e
            .getMessage)
        1
      case e: InterruptedException =>
        println("Exception occurred while waiting for the process execution:" + p + "\nError " +
            "Message:" + e.getMessage)
        1
    }
  }

  def compareFiles(snappyFile: File, sparkFile: File, pw: PrintWriter, queryNum:
  String, validationFailed: Boolean)
  : Boolean = {
    var hasValidationFailed = validationFailed
    val expectedFile = sparkFile.listFiles.filter(_.getName.endsWith(".csv"))
    val actualFile = snappyFile.listFiles.filter(_.getName.endsWith(".csv"))

    hasValidationFailed = compareFiles(getQueryResultDir("snappyQueryFiles"), actualFile.iterator
        .next().getAbsolutePath, expectedFile.iterator.next().getAbsolutePath, pw, queryNum)

    /*
    val expectedLineSet = Source.fromFile(expectedFile.iterator.next()).getLines()
    val actualLineSet = Source.fromFile(actualFile.iterator.next()).getLines()

    while (expectedLineSet.hasNext && actualLineSet.hasNext) {
      val expectedLine = expectedLineSet.next()
      val actualLine = actualLineSet.next()
      if (!actualLine.equals(expectedLine)) {
        if (!isIgnorable(actualLine, expectedLine)) {
          hasValidationFailed = true
          pw.println(s"Expected Result : $expectedLine")
          pw.println(s"Actual Result   : $actualLine")
        } else {
          hasValidationFailed = false
        }

      }
    }
    // scalastyle:off println
    if (actualLineSet.hasNext || expectedLineSet.hasNext) {
      hasValidationFailed = true
      if (actualLineSet.hasNext) {
        pw.println(s"Following ${actualLineSet.size} rows are unexpected in Snappy:")
        while (actualLineSet.hasNext) {
          pw.println(s"${actualLineSet.next()}")
        }
      }
      if (expectedLineSet.hasNext) {
        pw.println(s"Following ${expectedLineSet.size} rows are missing in Snappy:")
        while (expectedLineSet.hasNext) {
          pw.println(s"${expectedLineSet.next()}")
        }
      }
    }
    */
    hasValidationFailed
  }

  def compareFiles(dir: String, snappyResultsFile: String, sparkResultsFile: String,
      pw: PrintWriter, queryNum: String): Boolean = {
    val aStr = new StringBuilder
    var pb: ProcessBuilder = null
    var command: String = null
    val missingFileName = dir + File.separator + "missing_" + queryNum + ".txt"
    val unexpectedFileName = dir + File.separator + "unexpected_" + queryNum + ".txt"
    try {
      var writer = new PrintWriter(missingFileName)
      writer.print("")
      writer.close()
      writer = new PrintWriter(unexpectedFileName)
      writer.print("")
      writer.close()
    } catch {
      case fe: FileNotFoundException =>
        pw.println(s"${logTime} Exception while overwriting the result mismatch files", fe)
        return true
    }
    val unexpectedResultsFile = new File(unexpectedFileName)
    val missingResultsFile = new File(missingFileName)
    command = "grep -v -F -x -f " + sparkResultsFile + " " + snappyResultsFile
    pb = new ProcessBuilder("/bin/bash", "-c", command)
    println("Executing command : " + command)
    // get the unexpected rows in snappy
    executeProcess(pb, unexpectedResultsFile, pw)
    command = "grep -v -F -x -f " + snappyResultsFile + " " + sparkResultsFile
    pb = new ProcessBuilder("/bin/bash", "-c", command)
    println("Executing command : " + command)
    // get the missing rows in snappy
    executeProcess(pb, missingResultsFile, pw)
    var unexpectedRsReader: Iterator[String] = null
    var missingRsReader: Iterator[String] = null
    try {
      unexpectedRsReader = Source.fromFile(unexpectedResultsFile).getLines()
      missingRsReader = Source.fromFile(missingResultsFile).getLines()
    } catch {
      case fe: FileNotFoundException =>
        pw.println(s"${logTime} Could not find file to compare results.", fe)
        return true
    }
    val unexpected = new util.ArrayList[String]
    val missing = new util.ArrayList[String]
    try {
      while (unexpectedRsReader.hasNext)
        unexpected.add("\n  " + unexpectedRsReader.next())
      while (missingRsReader.hasNext)
        missing.add("\n  " + missingRsReader.next())
    } catch {
      case ie: IOException =>
        pw.println(s"${logTime} Got exception while reading resultset" +
            s" files", ie)
    }

    if (missing.size > 0) {
      if (missing.size < 20) {
        aStr.append(s"\nThe following ${missing.size} rows are missing from snappy resultset: \n")
        aStr.append(missing.toString)
      }
      else {
        aStr.append(s"There are ${missing.size} rows missing from snappy for $queryNum. Please " +
            s"check $missingFileName")
      }
      aStr.append("\n")
    }
    if (unexpected.size > 0) {
      if (unexpected.size < 20) {
        aStr.append(s"\nThe following ${unexpected.size} rows are unexpected in snappy " +
            s"resultset:\n")
        aStr.append(unexpected.toString)
      }
      else {
        aStr.append(s"There are ${unexpected.size} rows unexpected in snappy for $queryNum. " +
            s"Please check $unexpectedFileName")
      }
      aStr.append("\n")
    }

    // check if the mismatch is due to decimal, and can be ignored
    if ((missing.size() > 0) && missing.size() == unexpected.size()) {
      Collections.sort(missing)
      Collections.sort(unexpected)
      for (i <- 0 until missing.size()) {
        if (!isIgnorable(missing.get(i), unexpected.get(i))) true
      }
      // pw.println("This mismatch can be ignored.")
      aStr.setLength(0) // data mismatch can be ignored
    }

    if (aStr.length() > 0) {
      pw.println(s"${logTime} ${aStr.toString}")
      true
    } else {
      false
    }
  }

  /*
   If validation has failed for a query, add the query number to failedQueries String
   */
  def addToFailedQueryList(failedQueries: String, queryNum: String): String = {
    var str = failedQueries
    if (str.isEmpty) {
      str = queryNum
    }
    else {
      str = str + "," + queryNum
    }
    return str
  }


  /*
   Performs full resultSet validation from snappy for a select query against results in a
   goldenFile.
   */
  def assertValidateFullResultSetFromGoldenFile(sqlString: String, queryNum: String, tableType:
  String, snc: SnappyContext, pw: PrintWriter, validationFailed: Boolean, goldenFileDest: String):
  Boolean = {
    var hasValidationFailed = validationFailed

    val snappyQueryFileName = s"Snappy_${queryNum}"
    val snappyDest: String = getQueryResultDir("snappyQueryFiles") +
        File.separator + snappyQueryFileName
    pw.println(s" ${logTime} ${snappyDest}")
    val snappyFile: File = new java.io.File(snappyDest)
    var snappyDF = snc.sql(sqlString)

    pw.println(s"${logTime} ${goldenFileDest}")
    val goldenFileName = goldenFileDest + File.separator + s"Spark_$queryNum"
    val sortedGoldenDest = goldenFileDest + File.separator + s"Sorted_$queryNum"
    val sortedGoldenFile: File = new java.io.File(sortedGoldenDest)
    val goldenFile: File = new java.io.File(goldenFileName)

    try {
      if (!snappyFile.exists()) {
        val snap_col1 = snappyDF.schema.fieldNames(0)
        val snap_col = snappyDF.schema.fieldNames.filter(!_.equals(snap_col1)).toSeq
        snappyDF = snappyDF.repartition(1).sortWithinPartitions(snap_col1, snap_col: _*)
        writeToFile(snappyDF, snappyDest, snc)
        pw.println(s"${logTime} ${queryNum} Result Collected in file $snappyDest")
      }
      if (!goldenFile.exists()) {
        pw.println(s"${logTime} Did not find any golden file for query $queryNum")
        throw new Exception(s"Did not find any golden file for query $queryNum")
      } else if (goldenFile.length() > 0) {
        // sort the contents of golden file before comparing results
        var goldenDF = snc.read.format("com.databricks.spark.csv")
            .option("header", "false").option("inferSchema", "true").option("nullValue", "NULL")
            .load(goldenFileName)
        val col1 = goldenDF.schema.fieldNames(0)
        val col = goldenDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
        goldenDF = goldenDF.repartition(1).sortWithinPartitions(col1, col: _*)
        writeToFile(goldenDF, sortedGoldenDest, snc)
        pw.println(s"${logTime} ${queryNum} Result Collected in file ${sortedGoldenDest}")
      } else {
        pw.println(s"${logTime} No results in query result file for $queryNum.")
      }
      hasValidationFailed = compareFiles(snappyFile, sortedGoldenFile, pw, queryNum,
        hasValidationFailed)

    } catch {
      case ex: Exception => {
        hasValidationFailed = true
        pw.println(s"${logTime} Full resultSet Validation failed for ${queryNum} with following " +
            s"exception:\n")
        ex.printStackTrace(pw)
      }
    }
    pw.flush()
    return hasValidationFailed
  }

}
