package io.snappydata.hydra.ct

import java.io.PrintWriter

import org.apache.spark.sql.SnappyContext

object CTTestUtil {

  def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    pw.println(s"No. rows in resultset for join query ${queryNum} is : ${df.count} for ${tableType} table")
    assert(df.count() == numRows,s"Result mismatch for join query ${queryNum} : got " + df.count
    () + "" +
        " rows but expected " + numRows + " rows. Query is :" + sqlString + " for " +
        tableType + " table.")
  }

  /*
  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    val df = snc.sql(sqlString)
    pw.println(s"No. rows in resultset for query ${queryNum} is : ${df.count} for ${tableType} table")
    assert(df.count() == numRows,s"Result mismatch for query ${queryNum} : got " + df.count() + "" +
        " rows but expected " + numRows + " rows. Query is :" + sqlString + " for " +
        tableType + " table.")
  }
*/

  def assertQuery(snc: SnappyContext, sqlString: String,queryNum: String, tableType: String, pw:
      PrintWriter):
  Any = {
    pw.println(s"Query execution for $queryNum")
    val df = snc.sql(sqlString)
    pw.println("Number of Rows for  : " + sqlString +" is :" +  df.count())
  }

  def createReplicatedRowTables(pw: PrintWriter, snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl)
    snc.sql(CTQueries.exec_details_create_ddl)
  }

  def createPersistReplicatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " persistent")
    CTQueries.order_details_data.write.insertInto("order_details")
    snc.sql(CTQueries.exec_details_create_ddl + " persistent")
    CTQueries.exec_details_data.write.insertInto("exec_details")
  }

  def createPartitionedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '1'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '1'")
  }

  def createPersistPartitionedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '1' PERSISTENT")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '1' PERSISTENT")
  }

  def createColocatedRowTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl +" partition by (SINGLE_ORDER_DID) redundancy '1' buckets '11'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '1' buckets '11'")
  }

  def createPersistColocatedTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) redundancy '1' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '1' buckets '11' persistent")
  }

  // to add evition attributes
  def createRowTablesWithEviction(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " partition by (SINGLE_ORDER_DID) buckets '11' redundancy '1'")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) buckets '11' redundancy '1'")
  }

  //to add eviction attributes
  def createColocatedRowTablesWithEviction(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl +" partition by (SINGLE_ORDER_DID) redundancy '1' buckets '11' persistent")
    snc.sql(CTQueries.exec_details_create_ddl + " partition by (EXEC_DID) colocate with (order_details) redundancy '1' buckets '11'")
  }

  def createColumnTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column")
    snc.sql(CTQueries.exec_details_create_ddl + " using column")
  }

  def createPersistColumnTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " using column options(PERSISTENT 'async')")
    snc.sql(CTQueries.exec_details_create_ddl + " using column options(PERSISTENT 'async')")
  }

  def createColocatedColumnTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '1')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '1', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  def createPersistColocatedColumnTables(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', PERSISTENT 'async', redundancy '1') ")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', PERSISTENT 'async', redundancy '1',  COLOCATE_WITH 'ORDER_DETAILS')")
  }

  // to add eviction attributes
  def createColumnTablesWithEviction(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '1')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '1')")
  }

  //to add eviction attributes
  def createColocatedColumnTablesWithEviction(snc: SnappyContext): Unit = {
    snc.sql(CTQueries.order_details_create_ddl + " USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '1')")
    snc.sql(CTQueries.exec_details_create_ddl + " USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '1', COLOCATE_WITH 'ORDER_DETAILS')")
  }

  def loadTables(snc: SnappyContext): Unit ={
    CTQueries.order_details_data.write.insertInto("order_details")
    CTQueries.exec_details_data.write.insertInto("exec_details")
  }

  def executeQueries(snc: SnappyContext, tableType: String, pw: PrintWriter): Unit = {
    for (q <- CTQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, CTQueries.query1,"Q1",tableType,pw)
        case "Q2" => assertQuery(snc, CTQueries.query2,"Q2",tableType,pw)
        case "Q3" => assertQuery(snc, CTQueries.query3,"Q3",tableType,pw)
        case "Q4" => assertQuery(snc, CTQueries.query4,"Q4",tableType,pw)
        case "Q5" => assertQuery(snc, CTQueries.query5,"Q5",tableType,pw)
        case "Q6" => assertQuery(snc, CTQueries.query6,"Q6",tableType,pw)
        case "Q7" => assertQuery(snc, CTQueries.query7,"Q7",tableType,pw)
        case "Q8" => assertQuery(snc, CTQueries.query8,"Q8",tableType,pw)
        case "Q9" => assertQuery(snc, CTQueries.query9,"Q9",tableType,pw)
        case "Q10" => assertQuery(snc, CTQueries.query10,"Q10",tableType,pw)
        case "Q11" => assertQuery(snc, CTQueries.query11,"Q11",tableType,pw)
        case "Q12" => assertQuery(snc, CTQueries.query12,"Q12",tableType,pw)
        case "Q13" => assertQuery(snc, CTQueries.query13,"Q13",tableType,pw)
        case "Q14" => assertQuery(snc, CTQueries.query14,"Q14",tableType,pw)
        case "Q15" => assertQuery(snc, CTQueries.query15,"Q15",tableType,pw)
        case "Q16" => assertQuery(snc, CTQueries.query16,"Q16",tableType,pw)
        case "Q17" => assertQuery(snc, CTQueries.query17,"Q17",tableType,pw)
        case "Q18" => assertQuery(snc, CTQueries.query18,"Q18",tableType,pw)
        case "Q19" => assertQuery(snc, CTQueries.query19,"Q19",tableType,pw)
        case "Q20" => assertQuery(snc, CTQueries.query20,"Q20",tableType,pw)
        case "Q21" => assertQuery(snc, CTQueries.query21,"Q21",tableType,pw)
        case "Q22" => assertQuery(snc, CTQueries.query22,"Q22",tableType,pw)
        case "Q23" => assertQuery(snc, CTQueries.query23,"Q23",tableType,pw)
      }
    }
  }

  def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists exec_details")
    snc.sql("drop table if exists order_details")
  }

}
