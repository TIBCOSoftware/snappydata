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

package io.snappydata.hydra.ct

import java.io.PrintWriter

import io.snappydata.hydra.SnappyTestUtils

import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{SQLContext, SnappyContext}

object CTTestUtil {

  var insertUniqueRecords: Boolean = false;

  def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath

  def createReplicatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl)
    snc.sql(CTQueries.exec_details_create_ddl)
  }

  def createPersistReplicatedRowTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (PERSISTENT '" +
        persistenceMode + "')")
  }

  def createPartitionedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "')")
  }

  def createPersistPartitionedRowTables(snc: SnappyContext,
                                        persistenceMode: String, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS(partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "', PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + "  USING row OPTIONS(partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', PERSISTENT '" + persistenceMode + "')")
  }

  def createColocatedRowTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', redundancy '" + redundancy + "', buckets '11')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (partition_by 'EXEC_DID', " +
        "COLOCATE_WITH 'orders_details', redundancy '" + redundancy + "', buckets '11')")
  }

  def createPersistColocatedTables(snc: SnappyContext, redundancy: String, persistenceMode:
  String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', REDUNDANCY '" + redundancy + "', buckets '11', PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (partition_by 'EXEC_DID' " +
        "colocate_with 'orders_details', redundancy '" + redundancy + "', buckets '11', " +
        "PERSISTENT '" + persistenceMode + "')")
  }

  // to add evition attributes
  def createRowTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "')")
  }

  // to add eviction attributes
  def createColocatedRowTablesWithEviction(snc: SnappyContext, redundancy: String,
                                           persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING row OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', redundancy '" + redundancy + "', buckets '11', PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING row OPTIONS (partition_by 'EXEC_DID' " +
        "colocate_with 'orders_details', redundancy '" + redundancy + "', buckets '11', " +
        "PERSISTENT '" + persistenceMode + "')")
  }

  def createColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(redundancy '" +
        redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(redundancy '" + redundancy
        + "')")
  }

  def createColumnTablesWithKeyColumns(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(redundancy '" +
        redundancy + "' , key_columns 'SINGLE_ORDER_DID')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(redundancy '" + redundancy
        + "', key_columns 'EXEC_DID')")
  }

  def createPersistColumnTables(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "')")
  }

  def createPersistColumnTablesWithKeyColumns(snc: SnappyContext, persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "' , key_columns 'SINGLE_ORDER_DID')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT '" +
        persistenceMode + "', key_columns 'EXEC_DID')")
  }

  def createColocatedColumnTables(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS')")
  }

  def createColocatedColumnTablesWithKeyColumns(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "' , key_columns " +
        "'SINGLE_ORDER_DID')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS' , " +
        "key_columns 'EXEC_DID')")
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


  def createPersistColocatedColumnTablesWithKeyColumns(snc: SnappyContext, redundancy: String,
                                                       persistenceMode: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" +
        redundancy + "', key_columns 'SINGLE_ORDER_DID') ")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "',  " +
        "COLOCATE_WITH 'ORDERS_DETAILS' , key_columns 'EXEC_DID')")
  }

  // to add eviction attributes
  def createColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "')")
  }


  def createColumnTablesWithEvictionAndKeyColumns(snc: SnappyContext, redundancy: String): Unit
  = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "' , key_columns " +
        "'SINGLE_ORDER_DID')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "' , key_columns 'EXEC_DID')")
  }


  // to add eviction attributes
  def createColocatedColumnTablesWithEviction(snc: SnappyContext, redundancy: String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS')")
  }

  def createColocatedColumnTablesWithEvictionAndKeyColumns(snc: SnappyContext, redundancy:
  String): Unit = {
    snc.sql(CTQueries.orders_details_create_ddl + " USING column OPTIONS (partition_by " +
        "'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "' , key_columns " +
        "'SINGLE_ORDER_DID')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', " +
        "buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDERS_DETAILS' , " +
        "key_columns 'EXEC_DID')")
  }

  /*
  Load data to already created tables.
   */
  def loadTables(snc: SnappyContext): Unit = {
    CTQueries.orders_details_df(snc).write.insertInto("orders_details")
    CTQueries.exec_details_df(snc).write.insertInto("exec_details")
  }

  /*
  Load data to already created tables by removing duplicate records.
 */
  def loadTablesByRemovingDuplicateRecords(snc: SnappyContext): Unit = {
    CTQueries.orders_details_df(snc).dropDuplicates("SINGLE_ORDER_DID").write.insertInto("orders_details")
    CTQueries.exec_details_df(snc).dropDuplicates("EXEC_DID").write.insertInto("exec_details")
  }

  /*
  Load data to existing tables using putInto API.
 */

  def addDataUsingPutInto(snc: SnappyContext): Unit = {
    CTQueries.orders_details_df(snc).dropDuplicates("SINGLE_ORDER_DID").write.putInto("orders_details")
    CTQueries.exec_details_df(snc).dropDuplicates("EXEC_DID").write.putInto("exec_details")
  }

  /*
   Create and load tables in Spark
   */

  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    // scalastyle:off println
    if (insertUniqueRecords) {
      CTQueries.orders_details_df(sqlContext).
          dropDuplicates("SINGLE_ORDER_DID").createOrReplaceTempView("orders_details")
      println(s"orders_details Table created successfully in spark")
      CTQueries.exec_details_df(sqlContext).
          dropDuplicates("EXEC_DID").createOrReplaceTempView("exec_details")
      println(s"exec_details Table created successfully in spark")
    }
    else {
      CTQueries.orders_details_df(sqlContext, true).createOrReplaceTempView("orders_details")
      println(s"orders_details Table created successfully in spark")
      CTQueries.exec_details_df(sqlContext, true).createOrReplaceTempView("exec_details")
      println(s"exec_details Table created successfully in spark")
    }
  }

  /*
  Method for validating queires results for ct schema for 1.5GB data.
  For any other data size, this method can be used for comparing full resultsets by setting
  numRowsValidation to false.
  Returns failed queries in a string.
   */
  def executeQueries(snc: SnappyContext, tblType: String, pw: PrintWriter, sqlContext:
  SQLContext): String = {
    SnappyTestUtils.tableType = tblType
    var failedQueries = ""
    if (SnappyTestUtils.validateFullResultSet) {
      // scalastyle:off println
      pw.println(s"createAndLoadSparkTables started ...")

      val startTime = System.currentTimeMillis
      CTTestUtil.createAndLoadSparkTables(sqlContext)
      val finishTime = System.currentTimeMillis()
      pw.println(s"createAndLoadSparkTables completed successfully in : " + ((finishTime -
          startTime)/1000) + " seconds")
    }
    for (q <- CTQueries.queries) {
      var queryExecuted = true;
      var hasValidationFailed = false;
      q._1 match {
        case "Q1" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query1, 1,
          "Q1", pw, sqlContext)
        case "Q2" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query2, 1,
          "Q2", pw, sqlContext)
        case "Q3" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query3, 1,
          "Q3", pw, sqlContext)
        case "Q4" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query4, 1,
          "Q4", pw, sqlContext)
        case "Q5" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query5, 1,
          "Q5", pw, sqlContext)
        case "Q6" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query6, 5,
          "Q6", pw, sqlContext)
        case "Q7" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query7, 5,
          "Q7", pw, sqlContext)
        case "Q8" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query8, 5,
          "Q8", pw, sqlContext)
        case "Q9" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query9, 1,
          "Q9", pw, sqlContext)
        case "Q10" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query10, 1,
          "Q10", pw, sqlContext)
        case "Q11" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query11, 2706,
          "Q11", pw, sqlContext)
        case "Q12" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query12, 150,
          "Q12", pw, sqlContext)
        case "Q13" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query13, 149,
          "Q13", pw, sqlContext)
        case "Q14" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query14, 149,
          "Q14", pw, sqlContext)
        case "Q15" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query15, 2620,
          "Q15", pw, sqlContext)
        case "Q16" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query16, 150,
          "Q16", pw, sqlContext)
        case "Q17" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query17, 2,
          "Q17", pw, sqlContext)
        case "Q18" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query18, 0,
          "Q18", pw, sqlContext)
        case "Q19" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query19, 47,
          "Q19", pw, sqlContext)
        case "Q20" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query20, 100,
          "Q20", pw, sqlContext)
        case "Q21" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query21, 2,
          "Q21", pw, sqlContext)
        case "Q22" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query22, 1,
          "Q22", pw, sqlContext)
        // case "Q23" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, CTQueries.query23,0,
        // "Q23", pw,sqlContext)
        case "Q24" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, CTQueries.query24, 999,
          "Q24", pw, sqlContext)
        case _ =>
          // scalastyle:off println
          pw.println(s"Query ${q._1} will not  be executed.")
          queryExecuted = false
      }
      if (queryExecuted) {
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
}


