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
  var tableType: String = null

  def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  def assertJoin(sqlString: String, numRows: Int, queryNum: String,snc: SnappyContext,sqlContext: SQLContext, pw: PrintWriter): Boolean = {
    var hasValidationFailed = false
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    if (queryNum == "Q23")
      pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.show} for ${CTTestUtil.tableType} table")
    else {
      pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.count} for ${CTTestUtil.tableType} table")
      if (df.count() != numRows) {
        pw.println(s"Result mismatch for join query ${queryNum} : found ${df.count} rows but " +
            s"expected ${numRows} rows. Query is :${sqlString} for ${CTTestUtil.tableType} table.")
        hasValidationFailed = true
      }
    }
    pw.flush()
    return hasValidationFailed
  }

  def assertQuery(sqlString: String, numRows: Int, queryNum: String,snc: SnappyContext, sqlContext: SQLContext, pw: PrintWriter): Boolean = {
    var hasValidationFailed = false
    val df = snc.sql(sqlString)
    pw.println(s"No. rows in resultset for query ${queryNum} is : ${df.count} for ${CTTestUtil.tableType} table")
    if (df.count() != numRows) {
      pw.println(s"Result mismatch for query ${queryNum} : found ${df.count} rows but " +
          s"expected ${numRows} rows. Query is :${sqlString} for ${CTTestUtil.tableType} table.")
      hasValidationFailed = true
    }
    pw.flush()
    return hasValidationFailed
  }

  def createReplicatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl)
    snc.sql(CTQueries.exec_details_create_ddl)
  }

  def createPersistReplicatedRowTables(snc: SnappyContext,persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " persistent")
  }

  def createPartitionedRowTables(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "'")
  }

  def createPersistPartitionedRowTables(snc: SnappyContext,persistenceMode: String,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "' PERSISTENT")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "' PERSISTENT")
  }

  def createColocatedRowTables(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl +" partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11'")
  }

  def createPersistColocatedTables(snc: SnappyContext,redundancy: String,persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11' persistent")
  }

  // to add evition attributes
  def createRowTablesWithEviction(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '" + redundancy + "'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '" + redundancy + "'")
  }

  //to add eviction attributes
  def createColocatedRowTablesWithEviction(snc: SnappyContext,redundancy: String,persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl +" partition by (SINGLE_ORDER_DID) redundancy '" + redundancy + "' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '" + redundancy + "' buckets '11' persistent ")
  }

  def createColumnTables(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column options(redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(redundancy '" + redundancy + "')")
  }

  def createPersistColumnTables(snc: SnappyContext,persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column options(PERSISTENT '" + persistenceMode + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT '" + persistenceMode + "')")
  }

  def createColocatedColumnTables(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  def createPersistColocatedColumnTables(snc: SnappyContext,redundancy: String,persistenceMode: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "') ")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', PERSISTENT '" + persistenceMode + "', redundancy '" + redundancy + "',  COLOCATE_WITH 'ORDER_DETAILS')")
  }

  // to add eviction attributes
  def createColumnTablesWithEviction(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "')")
  }

  //to add eviction attributes
  def createColocatedColumnTablesWithEviction(snc: SnappyContext,redundancy: String): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '" + redundancy + "')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '" + redundancy + "', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  def loadTables(snc: SnappyContext): Unit ={
    CTQueries.order_details_data(snc).write.insertInto("order_details")
    CTQueries.exec_details_data(snc).write.insertInto("exec_details")
  }

  def executeQueries(snc: SnappyContext, tblType: String, pw: PrintWriter,
      fullResultSetValidation: Boolean,sqlContext: SQLContext): Boolean = {
    var hasValidationFailed = false;
    tableType = tblType

    for (q <- CTQueries.queries) {
      q._1 match {
        case "Q1" => hasValidationFailed = assertQuery(CTQueries.query1,1,"Q1",snc,sqlContext,pw)
        case "Q2" => hasValidationFailed = assertQuery(CTQueries.query2,1,"Q2",snc,sqlContext,pw)
        case "Q3" => hasValidationFailed = assertQuery(CTQueries.query3,1,"Q3",snc,sqlContext,pw)
        case "Q4" => hasValidationFailed = assertQuery(CTQueries.query4,1,"Q4",snc,sqlContext,pw)
        case "Q5" => hasValidationFailed = assertQuery(CTQueries.query5,1,"Q5",snc,sqlContext,pw)
        case "Q6" => hasValidationFailed = assertQuery(CTQueries.query6,5,"Q6",snc,sqlContext,pw)
        case "Q7" => hasValidationFailed = assertQuery(CTQueries.query7,5,"Q7",snc,sqlContext,pw)
        case "Q8" => hasValidationFailed = assertQuery(CTQueries.query8,5,"Q8",snc,sqlContext,pw)
        case "Q9" => hasValidationFailed = assertQuery(CTQueries.query9,1,"Q9",snc,sqlContext,pw)
        case "Q10" => hasValidationFailed = assertQuery(CTQueries.query10,1,"Q10",snc,sqlContext,pw)
        case "Q11" => hasValidationFailed = assertJoin(CTQueries.query11,2706,"Q11",snc,sqlContext,pw)
        case "Q12" => hasValidationFailed = assertJoin(CTQueries.query12,150,"Q12",snc,sqlContext,pw)
        case "Q13" => hasValidationFailed = assertQuery(CTQueries.query13,149,"Q13",snc,sqlContext,pw)
        case "Q14" => hasValidationFailed = assertQuery(CTQueries.query14,149,"Q14",snc,sqlContext,pw)
        case "Q15" => hasValidationFailed = assertJoin(CTQueries.query15,2620,"Q15",snc,sqlContext,pw)
        case "Q16" => hasValidationFailed = assertJoin(CTQueries.query16,150,"Q16",snc,sqlContext,pw)
        case "Q17" => hasValidationFailed = assertQuery(CTQueries.query17,2,"Q17",snc,sqlContext,pw)
        case "Q18" => hasValidationFailed = assertQuery(CTQueries.query18,0,"Q18",snc,sqlContext,pw)
        case "Q19" => hasValidationFailed = assertQuery(CTQueries.query19,47,"Q19",snc,sqlContext,pw)
        case "Q20" => hasValidationFailed = assertQuery(CTQueries.query20,100,"Q20",snc,sqlContext,pw)
        case "Q21" => hasValidationFailed = assertQuery(CTQueries.query21,0,"Q21",snc,sqlContext,pw)
        case "Q22" => hasValidationFailed = assertJoin(CTQueries.query22,1,"Q22",snc,sqlContext,pw)
        //case "Q23" => hasValidationFailed = assertJoin(CTQueries.query23,0,"Q23",snc,sqlContext,pw)
        case "Q24" => hasValidationFailed = assertQuery(CTQueries.query24,999,"Q24",snc,sqlContext,pw)

        case _ => pw.println(s"Query not be executed ${q._1}")
      }
    }
    return hasValidationFailed;
  }

  def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists exec_details")
    snc.sql("drop table if exists order_details")
  }

}
