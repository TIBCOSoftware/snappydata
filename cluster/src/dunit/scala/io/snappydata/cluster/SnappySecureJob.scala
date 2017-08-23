package io.snappydata.cluster

import java.io.{FileOutputStream, PrintWriter}

import com.pivotal.gemfirexd.Attribute
import com.typesafe.config.{Config, ConfigException}
import io.snappydata.Constant
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql._

class SnappySecureJob extends SnappySQLJob {
  private val colTable = "JOB_COLTABLE"
  private val rowTable = "JOB_ROWTABLE"
  private var pw: PrintWriter = _

  // Job config names
  val outputFile = "output.file"
  val opCode = "op.code"
  val otherColTabName = "other.columntable"
  val otherRowTabName = "other.rowtable"

  case class Data(PS_PARTKEY: Int, PS_SUPPKEY: Int,
      PS_AVAILQTY: Int, PS_SUPPLYCOST: BigDecimal)

  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath

  private def verifySessionAndConfig(snSession: SnappySession, jobConfig: Config): Unit = {
    assert(snSession.conf.getOption(Attribute.USERNAME_ATTR).isDefined, "Username not set in conf")
    assert(snSession.conf.getOption(Attribute.PASSWORD_ATTR).isDefined, "Password not set in conf")
    try {
      jobConfig.getString(Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd
          .Attribute.USERNAME_ATTR)
      assert(false, "Boot credentials set in job config")
    } catch {
      case _: ConfigException.Missing => // expected
    }
    try {
      jobConfig.getString(Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd
          .Attribute.PASSWORD_ATTR)
      assert(false, "Boot credentials set in job config")
    } catch {
      case _: ConfigException.Missing => // expected
    }
  }

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val file = jobConfig.getString(outputFile)
    val msg = s"\nCheck ${getCurrentDirectory}/$file file for output of this job"
    pw = new PrintWriter(new FileOutputStream(file), true)
    try {
      verifySessionAndConfig(snSession, jobConfig)
      if (jobConfig.getString(opCode).equalsIgnoreCase("sqlOps")) {
        createPartitionedRowTableUsingSQL(snSession)
        createPartitionedRowTableUsingAPI(snSession)
      } else {
        accessAndModifyTablesOwnedByOthers(snSession, jobConfig)
      }
      pw.println(msg)
    } finally {
      pw.close()
    }
    msg
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    verifySessionAndConfig(sc, config)
    SnappyJobValid()
  }


  def createPartitionedRowTableUsingAPI(snSession: SnappySession): Unit = {
    pw.println()
    pw.println(s"Creating tables $colTable and $rowTable using API")

    // drop the table if it exists
    snSession.dropTable(colTable, ifExists = true)
    snSession.dropTable(rowTable, ifExists = true)

    val schema = StructType(Array(StructField("PS_PARTKEY", IntegerType, false),
      StructField("S_SUPPKEY", IntegerType, false),
      StructField("PS_AVAILQTY", IntegerType, false),
      StructField("PS_SUPPLYCOST", DecimalType(15, 2), false)
    ))

    val props1 = Map("PARTITION_BY" -> "PS_PARTKEY")
    snSession.createTable(colTable, "column", schema, props1)
    snSession.createTable(rowTable, "row", schema, props1)

    val data = Seq(Seq(100, 1, 5000, BigDecimal(100)),
      Seq(200, 2, 50, BigDecimal(10)),
      Seq(300, 3, 1000, BigDecimal(20)),
      Seq(400, 4, 200, BigDecimal(30))
    )
    val rdd = snSession.sparkContext.parallelize(data,
      data.length).map(s => new Data(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int],
      s(2).asInstanceOf[Int],
      s(3).asInstanceOf[BigDecimal]))

    val dataDF = snSession.createDataFrame(rdd)
    pw.println(s"Inserting data in $colTable table")
    dataDF.write.insertInto(colTable)
    pw.println(s"Inserting data in $rowTable table")
    dataDF.write.insertInto(rowTable)

    pw.println(s"Printing the contents of the $colTable table")
    var tableData = snSession.sql(s"SELECT * FROM $colTable").collect()
    tableData.foreach(pw.println)
    pw.println(s"Printing the contents of the $rowTable table")
    tableData = snSession.sql(s"SELECT * FROM $rowTable").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Update the available quantity for PARTKEY 100")
    snSession.update(rowTable, "PS_PARTKEY = 100", Row(50000), "PS_AVAILQTY")

    pw.println(s"Printing the contents of the $rowTable table after update")
    tableData = snSession.sql(s"SELECT * FROM $rowTable").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Delete the records for PARTKEY 400")
    snSession.delete(rowTable, "PS_PARTKEY = 400")

    pw.println(s"Printing the contents of the $rowTable table after delete")
    tableData = snSession.sql(s"SELECT * FROM $rowTable").collect()
    tableData.foreach(pw.println)

    pw.println("****Done****")
  }

  def createPartitionedRowTableUsingSQL(snSession: SnappySession): Unit = {
    Map(colTable -> "column", rowTable -> "row").foreach(e => createPartitionedTableUsingSQL
    (snSession, e._1, e._2))
  }

  def createPartitionedTableUsingSQL(snSession: SnappySession, table: String, tableType: String):
  Unit = {
    pw.println()
    pw.println(s"****Creating a partitioned table($table) using SQL****")

    snSession.sql(s"DROP TABLE IF EXISTS $table")

    val pk = if (tableType.equalsIgnoreCase("row")) "PRIMARY KEY" else ""
    snSession.sql(s"CREATE TABLE $table ( " +
        s"PS_PARTKEY    INTEGER NOT NULL $pk," +
        "PS_SUPPKEY     INTEGER NOT NULL," +
        "PS_AVAILQTY    INTEGER NOT NULL," +
        "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
        s"USING $tableType OPTIONS (PARTITION_BY 'PS_PARTKEY' )")

    // insert some data in it
    pw.println()
    pw.println(s"Inserting data in $table table")
    snSession.sql(s"INSERT INTO $table VALUES(100, 1, 5000, 100)")
    snSession.sql(s"INSERT INTO $table VALUES(200, 2, 50, 10)")
    snSession.sql(s"INSERT INTO $table VALUES(300, 3, 1000, 20)")
    snSession.sql(s"INSERT INTO $table VALUES(400, 4, 200, 30)")

    pw.println(s"Printing the contents of the $table table")
    var tableData = snSession.sql(s"SELECT * FROM $table").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Update the available quantity for PARTKEY 100")
    snSession.sql(s"UPDATE $table SET PS_AVAILQTY = 50000 WHERE PS_PARTKEY = 100")

    pw.println(s"Printing the contents of the $table table after update")
    tableData = snSession.sql(s"SELECT * FROM $table").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Delete the records for PARTKEY 400")
    snSession.sql(s"DELETE FROM $table WHERE PS_PARTKEY = 400")

    pw.println(s"Printing the contents of the $table table after delete")
    tableData = snSession.sql(s"SELECT * FROM $table").collect()
    tableData.foreach(pw.println)

    pw.println("****Done****")
  }

  def accessAndModifyTablesOwnedByOthers(sns: SnappySession, config: Config): Unit = {
    pw.println()
    pw.println("****Accessing other user's tables****")

    val otherColTab = config.getString(otherColTabName)
    val otherRowTab = config.getString(otherRowTabName)
    val grantedOp = config.getString(opCode)

    val selects = Seq(s"SELECT * FROM $otherColTab", s"SELECT * FROM $otherRowTab")
    val inserts = Seq(s"INSERT INTO $otherColTab VALUES (-100, '-ONE HUNDRED', 100.1)",
      s"INSERT INTO $otherRowTab VALUES (-100, '-ONE HUNDRED', 100.1)")
    val updates = Seq(s"UPDATE $otherColTab SET COL1 = 100 WHERE COL1 = 1",
      s"UPDATE $otherRowTab SET COL1 = 100 WHERE COL1 = 1")
    val deletes = Seq(s"DELETE FROM $otherColTab WHERE COL1 = -100",
      s"DELETE FROM $otherRowTab WHERE COL1 = -100")

    // If dml is SELECT or UPDATE on any table or DELETE on column table, we check for below
    // error message.
    // s"User 'GEMFIRE1' does not have SELECT permission on column 'COL1' of table 'GEMFIRE2'"

    // If dml is DELETE on row table or INSERT on any table, we check for below error message.
    // s"User 'GEMFIRE1' does not have <DML> permission on table 'GEMFIRE2'"

    val columnErrorMsgAffix = "column 'COL1' of table 'GEMFIRE2'"
    val tableErrorMsgAffix = "table 'GEMFIRE2'"

    selects.foreach(s => assertGrantRevoke(s, grantedOp, dml = "select", columnErrorMsgAffix))
    inserts.foreach(s => assertGrantRevoke(s, grantedOp, dml = "insert", tableErrorMsgAffix))
    updates.foreach(s => assertGrantRevoke(s, grantedOp, dml = "update", columnErrorMsgAffix))
    deletes.foreach(s => assertGrantRevoke(s, grantedOp, dml = "delete", tableErrorMsgAffix))
    pw.println("****Done****")

    def assertGrantRevoke(s: String, grantedOp: String, dml: String, affix: String): Unit = {
      pw.println()
      if (grantedOp.equalsIgnoreCase(dml) || (dml.equalsIgnoreCase("select") && (grantedOp
          .equalsIgnoreCase("update") || grantedOp.equalsIgnoreCase("delete")))) {
        pw.println(s"Executing: $s")
        sns.sql(s).collect()
        pw.println(s"Success for $s")
      } else {
        try {
          sns.sql(s).collect()
          assert(false, s"Should have failed $s")
        } catch {
          case t: Throwable =>
            if (t.getMessage.contains(getErrorMessageToCheck(s, grantedOp, dml, affix))) {
              val msg = if (t.getMessage.contains("at ")) t.getMessage.substring(0, t.getMessage
                  .indexOf("at ")) else t.getMessage
              pw.println(s"Expected exception for $s: [$msg]")
              // t.getStackTrace.foreach(s => pw.println(s"  ${s.toString}"))
            } else {
              pw.println(s"UNEXPECTED ERROR FOR $s:\n[${t.getMessage}]")
              // t.getStackTrace.foreach(s => pw.println(s"  ${s.toString}"))
              throw t
            }
        }
      }
    }

    def getErrorMessageToCheck(sql: String, grantedOp: String, dml: String, affix: String): String
    = {
      var missingOp = dml.toUpperCase
      var append = affix
      if (grantedOp.equalsIgnoreCase("nogrant") || grantedOp.equalsIgnoreCase("insert")) {
        if (dml.equalsIgnoreCase("update") || (dml.equalsIgnoreCase("delete") && sql.contains
        (otherColTab))) {
          missingOp = "SELECT"
          if (dml.equalsIgnoreCase("delete") && sql.contains(otherColTab)) {
            append = columnErrorMsgAffix
          }
        }
      }
      s"User 'GEMFIRE1' does not have $missingOp permission on $append"
    }
  }

}
