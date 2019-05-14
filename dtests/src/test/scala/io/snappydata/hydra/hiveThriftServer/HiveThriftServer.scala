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
import org.apache.spark.sql._

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

    val Query1 : String = "create TablE if not exists EmpBeeline(id long, name String)"
    val Query2 : String = "insert into EmpBeeline select " +
      "id, concat(id, '_Snappy_TIBCO') from range(20000)"

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateHiveThriftServer" + "_" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)

    println("Starting the Hive Thrift Server testing job.....")
    val hiveThriftServer = new HiveThriftServer
    connectToBeeline(hiveThriftServer)

    executeShowSchemasAndValidate(hiveThriftServer, "ShoW DaTAbasEs", snc)
    executeShowSchemasAndValidate(hiveThriftServer, "sHOw SCHemas", snc)

    executeUseSchema(hiveThriftServer, "default", snc, spark)
    createRowOrColumnTableFromBeeline(hiveThriftServer, Query1)
    insertIntoRowOrColumnTableFromBeeline(hiveThriftServer, Query2, snc, spark, pw, sqlContext)


    executeShowTables(hiveThriftServer, "show TabLES in default")
    executeDropTables(hiveThriftServer, "drop table if exists EmpBeeline")
    executeShowTables(hiveThriftServer, "ShoW tAbLES iN DEFAULT")

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

  def executeShowSchemasAndValidate(hts : HiveThriftServer, command : String,
                                    snc : SnappyContext) : Unit = {
    result = ""
    snappyResult = ""
    var counter : Long = 0
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    hts.rsMetaData = hts.rs.getMetaData
    val dfResult = snc.sql(command)
    val dfDBName = dfResult.collectAsList()
    /*  -- Validataion Code --  */
    while(hts.rs.next) {
      counter += 1
      result = result + hts.rs.getString("databaseName") + "\t"
    }
    var index = 0
    while(index < 3 ) {
      snappyResult = snappyResult + dfDBName.get(index) + "\t"
      snappyResult = snappyResult.replace("[", "")
      snappyResult = snappyResult.replace("]", "")
      index += 1
    }
    if(command == "ShoW DaTAbasEs") {
      if (printLog) {
        println("Beeline Result : " + result)
        println("snappyResult : " + snappyResult)
      }
      if ((dfResult.count() == counter) && (result.equals(snappyResult))) {
          println("Row counts and Row contents are matched between snappy and beeline" +
            " for command : " + command)
     }
    }

    if(command == "sHOw SCHemas") {
      if (printLog) {
        println("Beeline Result : " + result)
        println("snappyResult : " + snappyResult)
      }
      if ((dfResult.count() == counter) && (result.equals(snappyResult))) {
        println("Row counts and Row contents are matched between snappy and beeline" +
          " for command : " + command)
      }
    }
  }

  def executeUseSchema(hts : HiveThriftServer, command : String, snc : SnappyContext,
                       spark : SparkSession) : Unit = {
    var whichSchema = false
    hts.stmt = hts.connection.createStatement()
    snc.sql("use default")

    whichSchema = hts.stmt.execute("use " + command)
    if(whichSchema) {
      println("Current Schema is : " + command)
    }
  }

  def createRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }

  def insertIntoRowOrColumnTableFromBeeline(hts : HiveThriftServer, command : String,
                                            snc : SnappyContext, spark : SparkSession,
                                            pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
    /*  -- Validataion Code --  */
    val insertChk1 : String = "select count(*) as Total from EmpBeeline"
    val insertChk2 : String = "select * from EmpBeeline where id > 19050"
    snc.sql(insertChk1)
    snc.sql(insertChk2)
    spark.sql(insertChk1)
    spark.sql(insertChk2)
    SnappyTestUtils.assertQueryFullResultSet(snc, insertChk1, "InsertCheck1", "Row", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, insertChk2, "InsertCheck2", "Row", pw, sqlContext)
  }

  def executeShowTables(hts : HiveThriftServer, command : String) : Unit = {
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
    println("show tables : " + "\n" + result)
  }

  def executeDropTables(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }
}
