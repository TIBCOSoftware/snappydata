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

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.typesafe.config.Config
import org.apache.spark.sql._

class HiveThriftServer extends SnappySQLJob {

  var connection : Connection = null
  var result : String = ""
  var rs : ResultSet = null
  var stmt : Statement = null

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    val snc : SnappyContext = snappySession.sqlContext
    println("Starting the Hive Thrift Server testing job.....")
    val hiveThriftServer = new HiveThriftServer
    connectToBeeline(hiveThriftServer)
    executeShowSchemasAndValidate(hiveThriftServer, "ShoW DaTAbasEs")
    executeShowSchemasAndValidate(hiveThriftServer, "sHOw SCHemas")
    executeCreateTable(hiveThriftServer, "create TablE if not exists " +
      "Emp(id int, name String)")
    executeShowTables(hiveThriftServer, "show TabLES in default")
    executeDropTables(hiveThriftServer, "drop table if exists Emp")
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

  def executeShowSchemasAndValidate(hts : HiveThriftServer, command : String) : Unit = {
    result = ""
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    while(hts.rs.next) {
      result = result + hts.rs.getString("databaseName") + "\n"
    }

    if(command == "ShoW DaTAbasEs") {
      println("show databases :" + "\n" + result)
    }

    if(command == "sHOw SCHemas") {
      println("show schemas :" + "\n" + result)
    }
  }

  def executeCreateTable(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }

  def executeShowTables(hts : HiveThriftServer, command : String) : Unit = {
    result = ""
    println("Command :" + command)
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    while(hts.rs.next()) {
        result = hts.rs.getString("schemaName") + " " +
        hts.rs.getString("tableName") + " "  + hts.rs.getString("isTemporary") + "\n"
    }
    println("show tables : " + result)
  }

  def executeDropTables(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }
}
