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
package io.snappydata.hydra.hiveThriftServer

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql._

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

class HiveThriftServer extends SnappySQLJob {

  var connection : Connection = null
  var result : String = ""
  var snappyResult : String = ""
  var rs : ResultSet = null
  var rsMetaData : ResultSetMetaData = null
  var stmt : Statement = null
  var printLog : Boolean = false

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println

    val queryDropEmpBeeline : String = "drop table if exists EmpBeeline"


    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateHiveThriftServer" + "_" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext : SQLContext = spark.sqlContext

    println("Starting the Hive Thrift Server testing job.....")
    val hiveThriftServer = new HiveThriftServer
    connectToBeeline(hiveThriftServer)

    executeShowSchemas(hiveThriftServer, "sHOw SCHemas", snc, pw, sqlContext)
    executeShowSchemas(hiveThriftServer, "ShoW DaTAbasEs", snc, pw, sqlContext)

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 1 : Create table from beeline, insert,update,deleter from beeline...")
    executeUseSchema(hiveThriftServer, "default", snc, spark)
    createRowOrColumnTableFromBeeline(hiveThriftServer,
      "create TablE if not exists EmpBeeline(id long, name String) using row", pw)
    insertIntoRowOrColumnTableFromBeeline(hiveThriftServer, "insert into EmpBeeline select " +
      "id, concat(id, '_Snappy_TIBCO') from range(500000)", snc, spark, pw, sqlContext)
    executeShowTables(hiveThriftServer, "show TabLES in default", pw)
    updateRowOrColumnTableFromBeeline(hiveThriftServer,
      "update EmpBeeline set id = " +
        "id+5 where id < 100", snc, spark, pw, sqlContext)
    deleteFromRowOrColumnTableFromBeeline(hiveThriftServer, "delete from EmpBeeline",
      snc, spark, pw, sqlContext)
    executeDropTables(hiveThriftServer, "drop table if exists EmpBeeline")
    pw.println()

    executeShowTables(hiveThriftServer, "ShoW tAbLES iN DEFAULT", pw)

    pw.flush()
    pw.close()
    disconnectToBeeline(hiveThriftServer)
    println("Finished the Hive Thrift Server testing.....")
  }

  def connectToBeeline(hts : HiveThriftServer) : Unit = {
    hts.connection = DriverManager
      .getConnection("jdbc:hive2://localhost:10000", "app", "app")
  }

  def disconnectToBeeline(hts : HiveThriftServer) : Unit = {
      hts.connection.close()
      if(hts.connection.isClosed()) {
      println("Connection with Beeline client closed....")
    }
  }

  def executeShowSchemas(hts : HiveThriftServer, command : String,
                         snc : SnappyContext, pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    hts.rsMetaData = hts.rs.getMetaData
    ValidateHiveThriftServer.validate_ShowSchema_Showdatabases(command, hts, snc, pw)
  }

  def executeUseSchema(hts : HiveThriftServer, command : String, snc : SnappyContext,
                       spark : SparkSession) : Unit = {
    var whichSchema = false
    hts.stmt = hts.connection.createStatement()
    snc.sql("use " + command)
    spark.sql("use " + command)
    whichSchema = hts.stmt.execute("use " + command)
    if(whichSchema) {
      println("Current Schema is : " + command)
    }
  }

  def createRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String,
                                        pw : PrintWriter) : Unit = {
    pw.println("Creating the table...")
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }

  def insertIntoRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String,
                                            snc : SnappyContext, spark : SparkSession,
                                            pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    pw.println("Inserting into the table...")
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
    var insertChk1 : String = "select count(*) as Total from EmpBeeline"
    var insertChk2 : String = "select * from EmpBeeline where id > 495000 order by id DESC"
    ValidateHiveThriftServer.validateSelectCountQuery(insertChk1, snc, hts, pw)
    ValidateHiveThriftServer.validateSelectQuery(insertChk2, snc, hts, pw)
    insertChk1 = null
    insertChk2 = null
//  Below code produce,
//  com.google.common.util.concurrent.UncheckedExecutionException:
//  java.lang.ClassCastException:
//  org.apache.spark.sql.SparkSession cannot be cast to org.apache.spark.sql.SnappySession
//  SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk1, "insertCheck1",
//  "Row", pw, spark, false)
//  SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk2, "insertCheck2",
//  "Row", pw, spark, false)
  }

  def updateRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String,
                                        snc : SnappyContext, spark : SparkSession,
                                        pw : PrintWriter, sqlContext : SQLContext) : Unit = {
      pw.println("Update the table...")
      hts.stmt = hts.connection.createStatement()
      hts.stmt.executeQuery(command)
      var insertChk1 : String = "select count(*) as Total from EmpBeeline"
      var insertChk2 : String = "select * from EmpBeeline where id < 500 order by id ASC"
      ValidateHiveThriftServer.validateSelectCountQuery(insertChk1, snc, hts, pw)
      ValidateHiveThriftServer.validateSelectQuery(insertChk2, snc, hts, pw)
      insertChk1 = null
      insertChk2 = null
 }

  def deleteFromRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String,
                                            snc : SnappyContext, spark : SparkSession,
                                            pw : PrintWriter, sqlContext : SQLContext) : Unit = {
      pw.println("Deleter from table...")
      hts.stmt = hts.connection.createStatement()
      hts.stmt.executeQuery(command)
      var insertChk1 : String = "select count(*) as Total from EmpBeeline"
      var insertChk2 : String = "select * from EmpBeeline"
      ValidateHiveThriftServer.validateSelectCountQuery(insertChk1, snc, hts, pw)
      ValidateHiveThriftServer.validateSelectQuery(insertChk2, snc, hts, pw)
      insertChk1 = null
      insertChk2 = null
  }

  def executeShowTables(hts : HiveThriftServer, command : String, pw : PrintWriter) : Unit = {
    result = ""
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    hts.rsMetaData = hts.rs.getMetaData
    result = hts.rsMetaData.getColumnLabel(1) + "\t" +
      hts.rsMetaData.getColumnName(2) + "\t" + hts.rsMetaData.getColumnName(3) + "\n"
    while(hts.rs.next()) {
        result = result + hts.rs.getString("schemaName") + "\t"  +
        hts.rs.getString("tableName") + "\t" + hts.rs.getString("isTemporary") + "\n"
    }
    pw.println("show tables : " + "\n" + result)
  }

  def executeDropTables(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }
}
