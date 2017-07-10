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

import java.io.PrintWriter

import io.snappydata.hydra.{SnappyTestUtils}

import org.apache.spark.sql.{SQLContext, SnappyContext}

object CTTestUtil {

  def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath

  def createReplicatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl)
    snc.sql(CTQueries.exec_details_create_ddl)
  }

  def createPersistReplicatedRowTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " persistent")
  }

  def createPartitionedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' " +
        "redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl +  " partition by (EXEC_DID) buckets '11' " +
        "redundancy '" + redundancy + "'")
  }

  def createPersistPartitionedRowTables(snc: SnappyContext, persistenceMode: String, redundancy:
  String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11'" +
        "redundancy '" + redundancy + "' PERSISTENT")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy" +
        " '"  + redundancy + "' PERSISTENT")
  }

  def createColocatedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '"
        + redundancy + "' buckets '11'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with " +
        "(orders_details) redundancy '" + redundancy + "' buckets '11'")
  }

  def createPersistColocatedTables(snc: SnappyContext, redundancy: String, persistenceMode:
  String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '"
        + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with " +
        "(orders_details) redundancy '" + redundancy + "' buckets '11' persistent")
  }

  // to add evition attributes
  def createRowTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' " +
        "redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy" +
        " '"  + redundancy + "'")
  }

  // to add eviction attributes
  def createColocatedRowTablesWithEviction(snc: SnappyContext, redundancy: String,
      persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '"
        + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with " +
        "(orders_details) redundancy '" + redundancy + "' buckets '11' persistent ")
  }

  def createColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(redundancy '" +
        redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(redundancy '" +
        redundancy + "')")
  }

  def createPersistColumnTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "')")
  }

  def createColocatedColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS')")
  }

  def createPersistColocatedColumnTables(snc: SnappyContext, redundancy: String, persistenceMode:
  String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" +
        redundancy + "') ")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "',  " +
        "COLOCATE_WITH 'ORDERS_DETAILS')")
  }

  // to add eviction attributes
  def createColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID'," +
        " buckets '11', redundancy '" + redundancy + "')")
  }

  // to add eviction attributes
  def createColocatedColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS')")
  }

  /*
  Load data to already created tables.
   */
  def loadTables(snc: SnappyContext): Unit = {
    CTQueries.orders_details_df(snc).write.insertInto("orders_details")
    CTQueries.exec_details_df(snc).write.insertInto("exec_details")
  }

  /*
   Create and load tables in Spark
   */
  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    CTQueries.orders_details_df(sqlContext).createOrReplaceTempView("orders_details")
    // scalastyle:off println
    println(s"orders_details Table created successfully in spark")
    CTQueries.exec_details_df(sqlContext).createOrReplaceTempView("exec_details")
    println(s"exec_details Table created successfully in spark")
  }

  /*
  Performs validation for tables with the queries. Returns failed queries in a string.
   */
  def executeQueries(snc: SnappyContext, tblType: String, pw: PrintWriter,
      fullResultSetValidation: Boolean, sqlContext: SQLContext, numRowsValidation: Boolean): String
  = {
    SnappyTestUtils.validateFullResultSet = fullResultSetValidation
    SnappyTestUtils.tableType = tblType
    SnappyTestUtils.numRowsValidation = numRowsValidation
    var failedQueries = ""
    if (SnappyTestUtils.validateFullResultSet) {
      CTTestUtil.createAndLoadSparkTables(sqlContext)
    }

    for (q <- CTQueries.queries) {
      var queryExecuted = true;
      var hasValidationFailed = false;
      q._1 match {
        case "Q1" => hasValidationFailed = assertQuery(snc, CTQueries.query1, 1, "Q1",
          pw, sqlContext)
        case "Q2" => hasValidationFailed = assertQuery(snc, CTQueries.query2, 1, "Q2",
          pw, sqlContext)
        case "Q3" => hasValidationFailed = assertQuery(snc, CTQueries.query3, 1, "Q3",
          pw, sqlContext)
        case "Q4" => hasValidationFailed = assertQuery(snc, CTQueries.query4, 1, "Q4",
          pw, sqlContext)
        case "Q5" => hasValidationFailed = assertQuery(snc, CTQueries.query5, 1, "Q5",
          pw, sqlContext)
        case "Q6" => hasValidationFailed = assertQuery(snc, CTQueries.query6, 5, "Q6",
          pw, sqlContext)
        case "Q7" => hasValidationFailed = assertQuery(snc, CTQueries.query7, 5, "Q7",
          pw, sqlContext)
        case "Q8" => hasValidationFailed = assertQuery(snc, CTQueries.query8, 5, "Q8",
          pw, sqlContext)
        case "Q9" => hasValidationFailed = assertQuery(snc, CTQueries.query9, 1, "Q9",
          pw, sqlContext)
        case "Q10" => hasValidationFailed = assertQuery(snc, CTQueries.query10, 1, "Q10",
          pw, sqlContext)
        case "Q11" => hasValidationFailed = assertJoin(snc, CTQueries.query11, 2706, "Q11",
          pw, sqlContext)
        case "Q12" => hasValidationFailed = assertJoin(snc, CTQueries.query12, 150, "Q12",
          pw, sqlContext)
        case "Q13" => hasValidationFailed = assertQuery(snc, CTQueries.query13, 149, "Q13",
          pw, sqlContext)
        case "Q14" => hasValidationFailed = assertQuery(snc, CTQueries.query14, 149, "Q14",
          pw, sqlContext)
        case "Q15" => hasValidationFailed = assertJoin(snc, CTQueries.query15, 2620, "Q15",
          pw, sqlContext)
        case "Q16" => hasValidationFailed = assertJoin(snc, CTQueries.query16, 150, "Q16",
          pw, sqlContext)
        case "Q17" => hasValidationFailed = assertQuery(snc, CTQueries.query17, 2, "Q17",
          pw, sqlContext)
        case "Q18" => hasValidationFailed = assertQuery(snc, CTQueries.query18, 0, "Q18",
          pw, sqlContext)
        case "Q19" => hasValidationFailed = assertQuery(snc, CTQueries.query19, 47, "Q19",
          pw, sqlContext)
        case "Q20" => hasValidationFailed = assertQuery(snc, CTQueries.query20, 100, "Q20",
          pw, sqlContext)
        case "Q21" => hasValidationFailed = assertQuery(snc, CTQueries.query21, 2, "Q21",
          pw, sqlContext)
        case "Q22" => hasValidationFailed = assertJoin(snc, CTQueries.query22, 1, "Q22",
          pw, sqlContext)
        // case "Q23" => hasValidationFailed = assertJoin(snc, CTQueries.query23,0,"Q23",
        //   pw,sqlContext)
        case "Q24" => hasValidationFailed = assertQuery(snc, CTQueries.query24, 999, "Q24",
          pw, sqlContext)
        case _ =>
          // scalastyle:off println
          pw.println(s"Query ${q._1} will not  be executed.")
          queryExecuted = false
      }
      if(queryExecuted){
        pw.println(s"Execution completed for query ${q._1}")
      }
      if (hasValidationFailed) {
        failedQueries = SnappyTestUtils.addToFailedQueryList(failedQueries, q._1)
      }
    }
    return failedQueries;
  }

  def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists exec_details")
    snc.sql("drop table if exists orders_details")
  }

  /*
  Executes the join query, matches the result with expected result, returns false if the query
  validation has failed.
  */
  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    validationFailed = assertQuery(snc, sqlString, numRows, queryNum, pw, sqlContext)
    return validationFailed
  }

  /*
   Executes the query, matches the result with expected result, returns false if the query
   validation has failed.
   */
  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, pw:
  PrintWriter, sqlContext: SQLContext): Boolean = {
    var validationFailed = false
    val df = snc.sql(sqlString)
    val count = df.count
    println(s"Query $queryNum")
    df.explain(true)
    if (SnappyTestUtils.numRowsValidation) {
      pw.println(s"No. rows in resultset for query ${queryNum} is : ${count} for " +
          s"${SnappyTestUtils.tableType} table")
      if (df.count() != numRows) {
        pw.println(s"Result mismatch for query ${queryNum} : found ${count} rows but expected " +
            s" ${numRows} rows.")
        validationFailed = true
      }
      pw.flush()
    }
    if (SnappyTestUtils.validateFullResultSet) {
      validationFailed = SnappyTestUtils.assertQueryFullResultSet(snc, sqlString, queryNum,
        pw, sqlContext)
    }

    if (validationFailed) {
      pw.println(s"Failed Query : " + sqlString + "\n Table Type : " + SnappyTestUtils.tableType
          + "\n")
    }
    pw.flush()
    return validationFailed
  }
}

