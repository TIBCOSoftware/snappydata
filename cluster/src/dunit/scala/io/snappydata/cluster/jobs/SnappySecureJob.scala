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

package io.snappydata.cluster.jobs

import java.io.{FileOutputStream, PrintWriter}

import com.pivotal.gemfirexd.Attribute
import com.typesafe.config.{Config, ConfigException}
import io.snappydata.{Constant, ServiceManager}
import io.snappydata.impl.LeadImpl
import org.apache.spark.SparkCallbacks
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.ui.SnappyBasicAuthenticator

// scalastyle:off println
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

  def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val file = jobConfig.getString(outputFile)
    val msg = s"\nCheck ${getCurrentDirectory}/$file file for output of this job"
    pw = new PrintWriter(new FileOutputStream(file), true)
    try {
      SnappyStreamingSecureJob.verifySessionAndConfig(snSession, jobConfig)
      if (jobConfig.getString(opCode).equalsIgnoreCase("sqlOps")) {
        createPartitionedRowTableUsingSQL(snSession)
        createPartitionedRowTableUsingAPI(snSession)
      } else {
        accessAndModifyTablesOwnedByOthers(snSession, jobConfig)
      }
      // Confirm that our zeppelin interpreter is not initialized.
      assert(ServiceManager.getLeadInstance.asInstanceOf[LeadImpl].getInterpreterServerClass ==
          null, "Zeppelin interpreter must not be initialized in secure cluster")
      // Check SnappyData Pulse UI is secured by our custom authenticator.
      assert(SparkCallbacks.getAuthenticatorForJettyServer().get
          .isInstanceOf[SnappyBasicAuthenticator], "SnappyData Pulse UI not secured")
      pw.println(msg)
    } finally {
      pw.close()
    }
    msg
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyStreamingSecureJob.verifySessionAndConfig(sc, config)
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

    val opCodeToSQLs = Map("select" -> Seq(s"SELECT * FROM $otherColTab",
      s"SELECT * FROM $otherRowTab"),
      "insert" -> Seq(s"INSERT INTO $otherColTab VALUES (1, 'ONE', 1.1)",
        s"INSERT INTO $otherRowTab VALUES (1, 'ONE', 1.1)"),
      "update" -> Seq(s"UPDATE $otherColTab SET COL1 = 100 WHERE COL1 = 1",
        s"UPDATE $otherRowTab SET COL1 = 100 WHERE COL1 = 1"),
      "delete" -> Seq(s"DELETE FROM $otherColTab WHERE COL1 = 100",
        s"DELETE FROM $otherRowTab WHERE COL1 = 100"))

    grantedOp match {
      case "nogrant" => opCodeToSQLs.keys.foreach(k => opCodeToSQLs(k).foreach(s =>
        assertGrantRevoke(s, k, granted = false)))
      case op: String => opCodeToSQLs(op).foreach(s => assertGrantRevoke(s, op, granted = true))
    }

    def assertGrantRevoke(s: String, op: String, granted: Boolean = false): Unit = {
      if (granted) {
        sns.sql(s).collect()
        pw.println(s"Success for $s")
      } else {
        try {
          sns.sql(s).collect()
          assert(false, s"Should have failed $s")
        } catch {
          case t: Throwable if (t.getMessage.contains(s"does not have ${op.toUpperCase} " +
              s"permission on") ||
              t.getMessage.contains(s"does not have SELECT permission on") ||
              t.getMessage.contains("42502")) =>
            pw.println(s"Found expected exception for $s")
          // t.getStackTrace.foreach(s => pw.println(s"${t.getMessage}\n  ${s.toString}"))
          case t: Throwable => pw.println(s"UNEXPECTED ERROR FOR $s:\n[${t.getMessage}]")
            t.getStackTrace.foreach(s => pw.println(s"  ${s.toString}"))
            throw t
        }
      }
    }

    pw.println("****Done****")
  }

}

object SnappyStreamingSecureJob extends SnappyStreamingJob {

  def verifySessionAndConfig(snSession: SnappySession, jobConfig: Config): Unit = {
    assert(snSession.conf.getOption(Attribute.USERNAME_ATTR).isDefined, "Username not set in conf")
    assert(snSession.conf.getOption(Attribute.PASSWORD_ATTR).isDefined, "Password not set in conf")

    def checkConfig(configKey: String): Unit = {
      try {
        jobConfig.getString(configKey)
        assert(false, s"Sensitive config $configKey found in job config!")
      } catch {
        case _: ConfigException.Missing => // expected
      }
    }

    Seq(Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd.Attribute.USERNAME_ATTR,
      Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR,
      com.pivotal.gemfirexd.Attribute.USERNAME_ATTR,
      com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR,
      "gemfire.sys.security-password",
      "javax.jdo.option.ConnectionURL").foreach(checkConfig(_))
  }

  override def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    verifySessionAndConfig(sc.snappySession, config)
    new SnappyJobValid
  }

  override def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any = {
    verifySessionAndConfig(sc.snappySession, jobConfig)
  }
}