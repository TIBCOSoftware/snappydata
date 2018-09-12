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

package io.snappydata.hydra

import java.io.{BufferedReader, File, FileNotFoundException, FileReader, IOException, PrintWriter}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.util

object SnappyTestUtils {

  var validateFullResultSet: Boolean = false;
  var numRowsValidation: Boolean = false;
  var tableType: String = null

  /*
  Executes the join query, matches only the full result with expected result, returns false if the
  query validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, queryNum: String, pw: PrintWriter,
      sqlContext: SQLContext, usePlanCaching: Boolean): Boolean = {
    var validationFailed = false
    numRowsValidation = false
    validationFailed = assertJoin(snc, sqlString, 0, queryNum, pw, sqlContext, usePlanCaching)
    return validationFailed
  }

  def assertJoin(snc: SnappyContext, sqlString: String, queryNum: String, pw: PrintWriter,
      sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    numRowsValidation = false
    validationFailed = assertJoin(snc, sqlString, 0, queryNum, pw, sqlContext, true)
    return validationFailed
  }


  /*
  Executes the join query, matches the result with expected result, returns false if the query
  validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    numRowsValidation = true
    validationFailed = assertJoin(snc, sqlString, numRows, queryNum, pw, sqlContext, true)
    return validationFailed
  }

  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext, usePlanCaching: Boolean): Boolean = {
    var validationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    if (validateFullResultSet) {
      sqlContext.sql("set spark.sql.crossJoin.enabled = true")
    }
    validationFailed = assertQuery(snc, sqlString, numRows, queryNum, pw, sqlContext,
      usePlanCaching)
    return validationFailed
  }

  /*
   Executes the query, matches only the full resultSet with expected result, returns false if the
   query validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext, usePlanCaching: Boolean): Boolean = {
    numRowsValidation = false
    assertQuery(snc, sqlString, 0, queryNum, pw, sqlContext, usePlanCaching)
  }

  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext): Boolean = {
    numRowsValidation = false
    assertQuery(snc, sqlString, 0, queryNum, pw, sqlContext, true)
  }

    def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String,
        pw: PrintWriter, sqlContext: SQLContext): Boolean = {
      numRowsValidation = true
      assertQuery(snc, sqlString, numRows, queryNum, pw, sqlContext, true)
    }

    /*
   Executes the query, matches the result with expected result, returns false if the query
   validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String,
      pw: PrintWriter, sqlContext: SQLContext, usePlanCaching: Boolean): Boolean = {
    var validationFailed = false
    var snappyDF: DataFrame = null
    if(!usePlanCaching) {
      snappyDF = snc.sqlUncached(sqlString)
    } else {
      snappyDF = snc.sql(sqlString)
    }
    val count = snappyDF.count
    // scalastyle:off println
    println(s"Query $queryNum")
    snappyDF.explain(true)
    if (numRowsValidation) {
      pw.println(s"Query ${queryNum} returned ${count} rows for ${tableType} table")
      if (count != numRows) {
        pw.println(s"Result mismatch for query ${queryNum} : found ${count} rows but expected " +
            s" ${numRows} rows.")
        validationFailed = true
      }
      pw.flush()
    }
    var fullRSValidationFailed: Boolean = false
    if (validateFullResultSet) {
      val snappyQueryFileName = s"Snappy_${queryNum}"
      val snappyDest: String = getQueryResultDir("snappyQueryFiles") +
          File.separator + snappyQueryFileName
      // scalastyle:off println
      // pw.println(s"Snappy query results are at : ${snappyDest}")
      val snappyFile: File = new java.io.File(snappyDest)

      val sparkQueryFileName = s"Spark_${queryNum}"
      val sparkDest: String = getQueryResultDir("sparkQueryFiles") + File.separator +
          sparkQueryFileName
      // pw.println(s"Spark query results are at : ${sparkDest}")
      val sparkFile: File = new java.io.File(sparkDest)
      var sparkDF = sqlContext.sql(sqlString)

      try {
        if (!snappyFile.exists()) {
          // val snap_col1 = snappyDF.schema.fieldNames(0)
          // val snap_col = snappyDF.schema.fieldNames.filter(!_.equals(snap_col1)).toSeq
          snappyDF = snappyDF.repartition(1) // .sortWithinPartitions(snap_col1, snap_col: _*)
          writeToFile(snappyDF, snappyDest, snc)
          // writeResultSetToCsv(snappyDF, snappyFile)
          pw.println(s"${queryNum} Result Collected in file ${snappyDest}")
        }
        if (!sparkFile.exists()) {
          // val col1 = sparkDF.schema.fieldNames(0)
          // val col = sparkDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
          sparkDF = sparkDF.repartition(1) // .sortWithinPartitions(col1, col: _*)
          writeToFile(sparkDF, sparkDest, snc)
          // writeResultSetToCsv(sparkDF, sparkFile)
          pw.println(s"${queryNum} Result Collected in file ${sparkDest}")
        }
        fullRSValidationFailed = compareFiles(snappyFile, sparkFile, pw, queryNum,
           fullRSValidationFailed)
      } catch {
        case ex: Exception => {
          fullRSValidationFailed = true
          pw.println(s"Full resultSet validation for ${queryNum} got the following exception:\n")
          ex.printStackTrace(pw)
        }
      }
      pw.flush()
    }
    if (validationFailed) {
      pw.println(s"\nNumRows validation failed for query ${queryNum} on ${tableType} table.")
    }
    if(fullRSValidationFailed){
      pw.println(s"\nFull resultset validation failed for query ${queryNum} on ${tableType} table.")
      validationFailed = true
    }
    pw.flush()
    return validationFailed
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
  def isIgnorable(actualLine: String, expectedLine: String): Boolean = {
    var canBeIgnored = false
    if ((actualLine != null && actualLine.size > 0) && (expectedLine != null && expectedLine.size
     > 0)) {
      val actualArray = actualLine.split(",")
      val expectedArray = expectedLine.split(",")
      var diff: Double = 0.0
      if(actualArray.length != expectedArray.length){
        canBeIgnored = false
      } else {
        for (i <- 0 to actualArray.length) {
          val value1 = actualArray(i).toDouble
          val value2 = expectedArray(i).toDouble
          if(value1 > value2) diff = value1.-(value2).doubleValue
          else diff = value2.-(value1).doubleValue
          println("diff is " + diff)
          if (diff <= 0.01) canBeIgnored = true
        }
      }
    }
    return canBeIgnored
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
      if ((rc == 0) || (pb.command.contains("grep") && rc == 1)) {
        pw.println("Process executed successfully")
        0
      }
      else {
        pw.println("Process execution failed with exit code: " + rc)
        1
      }
    } catch {
      case e: IOException =>
        pw.println("Exception occurred while starting the process:" + pb + "\nError Message:" + e
            .getMessage)
        1
      case e: InterruptedException =>
        pw.println("Exception occurred while waiting for the process execution:" + p + "\nError " +
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
        .next().getAbsolutePath, expectedFile.iterator.next().getAbsolutePath,
      pw, queryNum, hasValidationFailed)

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
      pw: PrintWriter, queryNum: String, hasValidationFailed: Boolean): Boolean = {
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
        pw.println("Log exception while overwirting the result mismatch files", fe)
        false
    }
    val unexpectedResultsFile = new File(unexpectedFileName)
    val missingResultsFile = new File(missingFileName)
    command = "grep -v -F -x -f " + sparkResultsFile + " " + snappyResultsFile
    pb = new ProcessBuilder("/bin/bash", "-c", command)
    pw.println("Executing command : " + command)
    // get the unexpected rows in snappy
    executeProcess(pb, unexpectedResultsFile, pw)
    command = "grep -v -F -x -f " + snappyResultsFile + " " + sparkResultsFile
    pb = new ProcessBuilder("/bin/bash", "-c", command)
    pw.println("Executing command : " + command)
    // get the missing rows in snappy
    executeProcess(pb, missingResultsFile, pw)
    var unexpectedRsReader: BufferedReader = null
    var missingRsReader: BufferedReader = null
    try {
      unexpectedRsReader = new BufferedReader(new FileReader(unexpectedResultsFile))
      missingRsReader = new BufferedReader(new FileReader(missingResultsFile))
    } catch {
      case fe: FileNotFoundException =>
        pw.println("Could not find file to compare results.", fe)
        false
    }
    var line: String = null
    val unexpected = new util.ArrayList[String]
    val missing = new util.ArrayList[String]
    try {
      while ( {
        (line = unexpectedRsReader.readLine) != null
      }) unexpected.add("\n  " + line)
      while ( {
        (line = missingRsReader.readLine) != null
      }) missing.add("\n  " + line)
      unexpectedRsReader.close()
      missingRsReader.close()
    } catch {
      case ie: IOException =>
        pw.println("Got exception while reading resultset files", ie)
    }
    if (missing.size > 0) {
      if (missing.size < 20) {
        aStr.append("\nThe following " + missing.size + " rows are missing from snappy resultset:")
        aStr.append(missing.toString)
      }
      else {
        aStr.append("There are " + missing.size + " rows missing in snappy for " + queryNum + "." +
            " " + "Please check " + missingFileName)
      }
      aStr.append("\n")
    }
    if (unexpected.size > 0) {
      if (unexpected.size < 20) {
        aStr.append("\nThe following " + unexpected.size +
            " rows from snappy resultset are unexpected: ")
        aStr.append(unexpected.toString)
      }
      else {
        aStr.append("There are " + unexpected.size + " rows unexpected in snappy for " + queryNum +
            ". Please check " + unexpectedFileName)
      }
      aStr.append("\n")
    }
    pw.println(aStr.toString)
    hasValidationFailed
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
        writeToFile(snappyDF, snappyDest, snc)
        // writeResultSetToCsv(snappyDF, snappyFile)
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
        writeToFile(goldenDF, sortedGoldenDest, snc)
        // writeResultSetToCsv(goldenDF, sortedGoldenFile)
        pw.println(s"${queryNum} Result Collected in file ${sortedGoldenDest}")
      } else {
        pw.println(s"zero results in query $queryNum.")
      }
      hasValidationFailed = compareFiles(snappyFile, sortedGoldenFile, pw, queryNum,
        hasValidationFailed)

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
}
