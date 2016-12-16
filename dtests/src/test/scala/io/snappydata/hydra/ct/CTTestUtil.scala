/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package io.snappydata.hydra.ct

import java.io.{File, PrintWriter}

import scala.io.Source

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SnappyContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object CTTestUtil {
  var validateFullResultSet: Boolean = false;
  var tableType: String = null

  def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  /*
 Executes the join query, matches the result with expected result, returns false if the query has
 failed.
 */
  def assertJoin(sqlString: String, numRows: Int, queryNum: String, snc: SnappyContext, sqlContext: SQLContext, pw: PrintWriter): Boolean = {
    var hasValidationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    if (queryNum == "Q23")
      pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.show} for ${CTTestUtil.tableType} table")
    else {
      pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.count} for ${CTTestUtil.tableType} table")
      if (df.count() != numRows) {
        pw.println(s"Result mismatch for join query ${queryNum} : found ${df.count} rows but expected ${numRows} rows.")
        hasValidationFailed = true
      }
    }
    pw.flush()
    if (CTTestUtil.validateFullResultSet)
      hasValidationFailed = assertValidateFullResultSet(sqlString, queryNum, CTTestUtil.tableType, snc, sqlContext, pw, hasValidationFailed)
    if (hasValidationFailed)
      pw.println(s"Failed Query =" + sqlString + " Table Type : " + tableType + "\n")
    pw.flush()
    return hasValidationFailed
  }

  /*
   Executes the query, matches the result with expected result, returns false if the query has
   failed.
   */
  def assertQuery(sqlString: String, numRows: Int, queryNum: String, snc: SnappyContext, sqlContext: SQLContext, pw: PrintWriter): Boolean = {
    var hasValidationFailed = false
    val df = snc.sql(sqlString)
    pw.println(s"No. rows in resultset for query ${queryNum} is : ${df.count} for ${CTTestUtil.tableType} table")
    if (df.count() != numRows) {
      pw.println(s"Result mismatch for query ${queryNum} : found ${df.count} rows but expected ${numRows} rows.")
      hasValidationFailed = true
    }
    pw.flush()
    if (CTTestUtil.validateFullResultSet)
      hasValidationFailed = assertValidateFullResultSet(sqlString, queryNum, CTTestUtil.tableType, snc, sqlContext, pw, hasValidationFailed)
    if (hasValidationFailed)
      pw.println(s"Failed Query : " + sqlString + "\n Table Type : " + tableType + "\n")
    pw.flush()
    return hasValidationFailed
  }

  /*
  Performs full resultSet validation for the query against snappy spark resultset.
   */

  def assertValidateFullResultSet(sqlString: String, queryNum: String, tableType: String,
      snc: SnappyContext, sqlContext: SQLContext, pw: PrintWriter, validationFailed: Boolean): Boolean = {
    var hasValidationFailed = validationFailed
    val snappyDF = snc.sql(sqlString)
    val snappyQueryFileName = s"Snappy_${queryNum}"
    val sparkQueryFileName = s"Spark_${queryNum}"

    val sparkDest: String = getTempDir("sparkQueryFiles") + File.separator + sparkQueryFileName
    val snappyDest = getTempDir("snappyQueryFiles") + File.separator + snappyQueryFileName

    pw.println(sparkDest)
    pw.println(snappyDest)

    val sparkFile: File = new java.io.File(sparkDest)
    val snappyFile: File = new java.io.File(snappyDest)

    val sparkDF = sqlContext.sql(sqlString)
    val col1 = sparkDF.schema.fieldNames(0)
    val col = sparkDF.schema.fieldNames.filter(!_.equals(col1)).toSeq
    try {

      if (snappyFile.listFiles() == null) {
        writeResultSetToCsv(snc, snappyDF, snappyDest, col1, col)
        pw.println(s"${queryNum} Result Collected in file $snappyQueryFileName")
      }
      if (sparkFile.listFiles() == null) {
        writeResultSetToCsv(snc, sparkDF, sparkDest, col1, col)
        pw.println(s"${queryNum} Result Collected in file $sparkQueryFileName")
      }
      val expectedFile = sparkFile.listFiles.filter(_.getName.endsWith(".csv"))
      if (!(expectedFile.length > 0)) {
        pw.println(s"File not found for csv")
        throw new Exception(s"File not found for csv for query ${queryNum}")
      }
      val actualFile = snappyFile.listFiles.filter(_.getName.endsWith(".csv"))
      if (!(actualFile.length > 0)) {
        pw.println(s"File not found for csv")
        throw new Exception(s"File not found for csv for query ${queryNum}")
      }

      val expectedLineSet = Source.fromFile(expectedFile.iterator.next()).getLines()
      val actualLineSet = Source.fromFile(actualFile.iterator.next()).getLines

      while (expectedLineSet.hasNext && actualLineSet.hasNext) {
        val expectedLine = expectedLineSet.next()
        val actualLine = actualLineSet.next()
        if (!actualLine.equals(expectedLine)) {
          hasValidationFailed = true
          pw.println(s"Expected Result \n: $expectedLine")
          pw.println(s"Actual Result   \n: $actualLine")
        }
      }
      if (actualLineSet.hasNext || expectedLineSet.hasNext) {
        hasValidationFailed = true
        if (actualLineSet.hasNext)
          pw.println("Following rows unexpected in Snappy:")
        while (actualLineSet.hasNext)
          pw.println(actualLineSet.next())
        if (expectedLineSet.hasNext)
          pw.println("Following rows missing in Snappy:")
        while (expectedLineSet.hasNext)
          pw.println(expectedLineSet.next())
      }
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
  def writeResultSetToCsv(snc: SnappyContext, df: DataFrame, destFile: String, col1: String, col: Seq[String]): Unit = {
    import snc.implicits._
    df.coalesce(1).orderBy(col1, col: _*)
        .map(row => {
          val md = row.toSeq.map {
            case d: Double => "%18.1f".format(d).trim().toDouble
            case de: BigDecimal => de.setScale(2, BigDecimal.RoundingMode.HALF_UP)
            case i: Integer => i
            case v => v
          }
          Row.fromSeq(md)
        })(RowEncoder(df.schema))
        .map(row => {
          var str = ""
          row.toSeq.foreach(e => {
            if (e != null)
              str = str + e.toString + ","
            else
              str = str + "NULL" + ","
          })
          str
        }).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", false).save(destFile)
  }

  /*
  Returns the path for the directory where the output of resultset of queries have been saved.
   Creates a new directory, if not already existing
   */
  protected def getTempDir(dirName: String): String = {
    val log: File = new File(".")
    var dest: String = null
    val dirString = log.getCanonicalPath;
    if (dirName.equals("sparkQueryFiles")) {
      val logDir = log.listFiles.filter(_.getName.equals("snappyleader.log"))
      if (!logDir.isEmpty) {
        val leaderLogFile: File = logDir.iterator.next()
        if (leaderLogFile.exists()) dest = dirString + File.separator + ".." + File.separator + ".." + File.separator + dirName
      }
      else dest = dirString + File.separator + ".." + File.separator + dirName
    }
    else dest = log.getCanonicalPath + File.separator + dirName
    val tempDir: File = new File(dest)
    if (!tempDir.exists) tempDir.mkdir()
    return tempDir.getAbsolutePath
  }

  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String, tableType: String, pw: PrintWriter):
  Any = {
    pw.println(s"Query execution for $queryNum")
    val df = snc.sql(sqlString)
    pw.println("Number of Rows for  : " + sqlString + " is :" + df.count())
  }

  def createReplicatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl)
    snc.sql(CTQueries.exec_details_create_ddl)
  }

  def createPersistReplicatedRowTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " persistent")
  }

  def createPartitionedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "'")
  }

  def createPersistPartitionedRowTables(snc: SnappyContext, persistenceMode: String, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "' PERSISTENT")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "' PERSISTENT")
  }

  def createColocatedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11'")
  }

  def createPersistColocatedTables(snc: SnappyContext, redundancy: String, persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11' persistent")
  }

  // to add evition attributes
  def createRowTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "'")
  }

  //to add eviction attributes
  def createColocatedRowTablesWithEviction(snc: SnappyContext, redundancy: String, persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11' persistent ")
  }

  def createColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column options(redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(redundancy '" + redundancy + "')")
  }

  def createPersistColumnTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column options(PERSISTENT '" + persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT '" + persistenceMode + "')")
  }

  def createColocatedColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  def createPersistColocatedColumnTables(snc: SnappyContext, redundancy: String, persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "') ")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "',  COLOCATE_WITH 'ORDER_DETAILS')")
  }

  // to add eviction attributes
  def createColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "')")
  }

  //to add eviction attributes
  def createColocatedColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  /*
     Load data to already created tables.
   */
  def loadTables(snc: SnappyContext): Unit = {
    CTQueries.order_details_data(snc).write.insertInto("order_details")
    CTQueries.exec_details_data(snc).write.insertInto("exec_details")
  }

  /*
   Create and load tables in Spark
   */
  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    CTQueries.order_details_data(sqlContext).createOrReplaceTempView("order_details")
    println(s"order_details Table created successfully in spark")
    CTQueries.exec_details_data(sqlContext).createOrReplaceTempView("exec_details")
    println(s"exec_details Table created successfully in spark")
  }

  /*
    If validation has failed for a query, add the query number to failedQueries String
   */
  def addFailedQuery(failedQueries: String, queryNum: String): String = {
    var str = failedQueries
    if (str.isEmpty)
      str = queryNum
    else
      str = str + "," + queryNum
    return str
  }

  /*
  Performs validation for tables with the queries. Returns failed queries in a string.
   */
  def executeQueries(snc: SnappyContext, tblType: String, pw: PrintWriter,
      fullResultSetValidation: Boolean, sqlContext: SQLContext): String = {
    validateFullResultSet = fullResultSetValidation
    tableType = tblType
    var failedQueries = ""
    if (CTTestUtil.validateFullResultSet)
      CTTestUtil.createAndLoadSparkTables(sqlContext)

    for (q <- CTQueries.queries) {
      var hasValidationFailed = false;
      q._1 match {
        case "Q1" => hasValidationFailed = assertQuery(CTQueries.query1, 1, "Q1", snc, sqlContext, pw)
        case "Q2" => hasValidationFailed = assertQuery(CTQueries.query2, 1, "Q2", snc, sqlContext, pw)
        case "Q3" => hasValidationFailed = assertQuery(CTQueries.query3, 1, "Q3", snc, sqlContext, pw)
        case "Q4" => hasValidationFailed = assertQuery(CTQueries.query4, 1, "Q4", snc, sqlContext, pw)
        case "Q5" => hasValidationFailed = assertQuery(CTQueries.query5, 1, "Q5", snc, sqlContext, pw)
        case "Q6" => hasValidationFailed = assertQuery(CTQueries.query6, 5, "Q6", snc, sqlContext, pw)
        case "Q7" => hasValidationFailed = assertQuery(CTQueries.query7, 5, "Q7", snc, sqlContext, pw)
        case "Q8" => hasValidationFailed = assertQuery(CTQueries.query8, 5, "Q8", snc, sqlContext, pw)
        case "Q9" => hasValidationFailed = assertQuery(CTQueries.query9, 1, "Q9", snc, sqlContext, pw)
        case "Q10" => hasValidationFailed = assertQuery(CTQueries.query10, 1, "Q10", snc, sqlContext, pw)
        case "Q11" => hasValidationFailed = assertJoin(CTQueries.query11, 2706, "Q11", snc, sqlContext, pw)
        case "Q12" => hasValidationFailed = assertJoin(CTQueries.query12, 150, "Q12", snc, sqlContext, pw)
        case "Q13" => hasValidationFailed = assertQuery(CTQueries.query13, 149, "Q13", snc, sqlContext, pw)
        case "Q14" => hasValidationFailed = assertQuery(CTQueries.query14, 149, "Q14", snc, sqlContext, pw)
        case "Q15" => hasValidationFailed = assertJoin(CTQueries.query15, 2620, "Q15", snc, sqlContext, pw)
        case "Q16" => hasValidationFailed = assertJoin(CTQueries.query16, 150, "Q16", snc, sqlContext, pw)
        case "Q17" => hasValidationFailed = assertQuery(CTQueries.query17, 2, "Q17", snc, sqlContext, pw)
        case "Q18" => hasValidationFailed = assertQuery(CTQueries.query18, 0, "Q18", snc, sqlContext, pw)
        case "Q19" => hasValidationFailed = assertQuery(CTQueries.query19, 47, "Q19", snc, sqlContext, pw)
        case "Q20" => hasValidationFailed = assertQuery(CTQueries.query20, 100, "Q20", snc, sqlContext, pw)
        case "Q21" => hasValidationFailed = assertQuery(CTQueries.query21, 0, "Q21", snc, sqlContext, pw)
        case "Q22" => hasValidationFailed = assertJoin(CTQueries.query22, 1, "Q22", snc, sqlContext, pw)
        //case "Q23" => hasValidationFailed = assertJoin(CTQueries.query23,0,"Q23",snc,sqlContext,pw)
        case "Q24" => hasValidationFailed = assertQuery(CTQueries.query24, 999, "Q24", snc, sqlContext, pw)
        case _ => pw.println(s"Query not be executed ${q._1}")
      }
      if (hasValidationFailed)
        failedQueries = addFailedQuery(failedQueries, q._1)
    }
    return failedQueries;
  }

  def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists exec_details")
    snc.sql("drop table if exists order_details")
  }

}

