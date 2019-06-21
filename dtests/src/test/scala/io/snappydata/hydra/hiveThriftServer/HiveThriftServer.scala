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
  var isFailed : Boolean = false

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println


    val snc : SnappyContext = snappySession.sqlContext
    snc.sql("set snappydata.hiveServer.enabled=true")
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

    /*     ------------------------------------------------------------------------   */

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Use Default Schema")
    pw.println("Case 1 : Create row table from beeline, insert,update,delete from beeline...")
    executeUseSchema(hiveThriftServer, "default", snc, spark)
    createRowOrColumnTableFromBeeline(hiveThriftServer,
      "create TablE if not exists Emp1(id long, name String) using row", pw)
    insertIntoTableFromBeeline(hiveThriftServer, "insert into Emp1 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(4000000)", snc,
      spark, pw, sqlContext, "Emp1", "Row")
    executeShowTables(hiveThriftServer, "show TabLES in default", pw)
    updateTableFromBeeline(hiveThriftServer,
      "update Emp1 set id = " +
        "id+5 where id < 100", snc, spark, pw, sqlContext, "Emp1", "Row")
    deleteFromTableFromBeeline(hiveThriftServer, "delete from Emp1",
      snc, spark, pw, sqlContext, "Emp1", "Row")
    executeDropTableFromBeeLine(hiveThriftServer, "drop table if exists Emp1")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 2 : Create column table from beeline, insert,update,delete from Snappy...")
    createRowOrColumnTableFromBeeline(hiveThriftServer,
      "create TablE if not exists Emp2(id long, name String) using column", pw)
    insertIntoTableFromSnappy(hiveThriftServer, "insert into Emp2 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(50000000)", snc,
      spark, pw, sqlContext, "Emp2", "column")
    executeShowTables(hiveThriftServer, "show TabLES in default", pw)
    updateTableFromSnappy(hiveThriftServer,
      "update Emp2 set name = " +
        "'_TIBCO' where id > 1000 and id < 2000", snc,
      spark, pw, sqlContext, "Emp2", "column")
    deleteFromTableFromSnappy(hiveThriftServer, "delete from Emp2",
      snc, spark, pw, sqlContext, "Emp2", "column")
    executeDropTableFromSnappy(snc, "drop table if exists Emp2")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 3 : Create row table from Snappy, insert,update,delete from BeeLine...")
    createRowOrColumnTableFromSnappy(snc,
      "create TablE if not exists Emp3(id long, name String) using row", pw)
    insertIntoTableFromBeeline(hiveThriftServer, "insert into Emp3 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(4000000)", snc,
      spark, pw, sqlContext, "Emp3", "Row")
    executeShowTables(hiveThriftServer, "show TabLES in default", pw)
    updateTableFromBeeline(hiveThriftServer,
      "update Emp3 set id = " +
        "id+5 where id < 100", snc, spark, pw, sqlContext, "Emp3", "Row")
    deleteFromTableFromBeeline(hiveThriftServer, "delete from Emp3",
      snc, spark, pw, sqlContext, "Emp3", "Row")
    executeDropTableFromBeeLine(hiveThriftServer, "drop table if exists Emp3")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 4 : Create column table from Snappy, insert,update,delete from Snappy...")
    createRowOrColumnTableFromSnappy(snc,
      "create TablE if not exists Emp4(id long, name String) using column", pw)
    insertIntoTableFromSnappy(hiveThriftServer, "insert into Emp4 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(50000000)", snc,
      spark, pw, sqlContext, "Emp4", "column")
    executeShowTables(hiveThriftServer, "show TabLES in default", pw)
    updateTableFromSnappy(hiveThriftServer,
      "update Emp4 set name = " +
        "'_TIBCO' where id > 1000 and id < 2000", snc, spark, pw, sqlContext, "Emp4", "column")
    deleteFromTableFromSnappy(hiveThriftServer, "delete from Emp4",
      snc, spark, pw, sqlContext, "Emp4", "column")
    executeDropTableFromSnappy(snc, "drop table if exists Emp4")

    executeShowTables(hiveThriftServer, "ShoW tAbLES iN DEFAULT", pw)

    /*     ------------------------------------------------------------------------   */

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Use App Schema")
    pw.println("Case 5 : Create row table from beeline, insert,update,delete from beeline...")
    executeUseSchema(hiveThriftServer, "app", snc, spark)
    createRowOrColumnTableFromBeeline(hiveThriftServer,
      "create TablE if not exists Emp5(id long, name String) using row", pw)
    insertIntoTableFromBeeline(hiveThriftServer, "insert into Emp5 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(4000000)", snc, spark,
      pw, sqlContext, "Emp5", "row")
    executeShowTables(hiveThriftServer, "show TabLES in app", pw)
    updateTableFromBeeline(hiveThriftServer,
      "update Emp5 set id = " +
        "id+5 where id < 150", snc, spark, pw, sqlContext, "Emp5", "row")
    deleteFromTableFromBeeline(hiveThriftServer, "delete from Emp5",
      snc, spark, pw, sqlContext, "Emp5", "row")
    executeDropTableFromBeeLine(hiveThriftServer, "drop table if exists Emp5")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 6 : Create column table from beeline, insert,update,delete from Snappy...")
    createRowOrColumnTableFromBeeline(hiveThriftServer,
      "create TablE if not exists Emp6(id long, name String) using column", pw)
    insertIntoTableFromSnappy(hiveThriftServer, "insert into Emp6 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(50000000)", snc,
      spark, pw, sqlContext, "Emp6", "column")
    executeShowTables(hiveThriftServer, "show TabLES in app", pw)
    updateTableFromSnappy(hiveThriftServer,
      "update Emp6 set name = " +
        "'_TIBCO' where id > 1000 and id < 2000", snc, spark, pw, sqlContext, "Emp6", "column")
    deleteFromTableFromSnappy(hiveThriftServer, "delete from Emp6",
      snc, spark, pw, sqlContext, "Emp6", "column")
    executeDropTableFromSnappy(snc, "drop table if exists Emp6")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 7 : Create row table from Snappy, insert,update,delete from BeeLine...")
    createRowOrColumnTableFromSnappy(snc,
      "create TablE if not exists Emp7(id long, name String) using row", pw)
    insertIntoTableFromBeeline(hiveThriftServer, "insert into Emp7 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(4000000)", snc,
      spark, pw, sqlContext, "Emp7", "row")
    executeShowTables(hiveThriftServer, "show TabLES in app", pw)
    updateTableFromBeeline(hiveThriftServer,
      "update Emp7 set id = " +
        "id+15 where id < 200", snc, spark, pw, sqlContext, "Emp7", "row")
    deleteFromTableFromBeeline(hiveThriftServer, "delete from Emp7",
      snc, spark, pw, sqlContext, "Emp7", "row")
    executeDropTableFromBeeLine(hiveThriftServer, "drop table if exists Emp7")

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 8 : Create column table from Snappy, insert,update,delete from Snappy...")
    createRowOrColumnTableFromSnappy(snc,
      "create TablE if not exists Emp8(id long, name String) using column", pw)
    insertIntoTableFromSnappy(hiveThriftServer, "insert into Emp8 select " +
      "id, concat(id, '_Snappy_TIBCO') from range(50000000)", snc,
      spark, pw, sqlContext, "Emp8", "column")
    executeShowTables(hiveThriftServer, "show TabLES in app", pw)
    updateTableFromSnappy(hiveThriftServer,
      "update Emp8 set name = " +
        "'_TIBCO' where id > 1000 and id < 2000", snc, spark, pw, sqlContext, "Emp8", "column")
    deleteFromTableFromSnappy(hiveThriftServer, "delete from Emp8",
      snc, spark, pw, sqlContext, "Emp8", "column")
    executeDropTableFromSnappy(snc, "drop table if exists Emp8")

    executeShowTables(hiveThriftServer, "ShoW tAbLES iN app", pw)
    /*     ------------------------------------------------------------------------   */
    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 9 : Use Default Schema. " +
      "Create the table with same name should throw an exception.")
    executeUseSchema(hiveThriftServer, "default", snc, spark)
    try {
      createRowOrColumnTableFromBeeline(hiveThriftServer,
        "CREATE TABLE Student(class Int, subject String) using column", pw)
      createRowOrColumnTableFromSnappy(snc,
        "CREATE TABLE Student(class Int, subject String) using row", pw)
    }
    catch {
      case e : Exception => pw.println("----- " + e.getMessage + " -----")
    }
    executeDropTableFromSnappy(snc, "DROP TABLE Student")
    executeShowTables(hiveThriftServer, "show tables in default", pw)

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 10 : Use App Schema. " +
      "Create the table with same name should throw an exception.")
    executeUseSchema(hiveThriftServer, "app", snc, spark)
    try {
      createRowOrColumnTableFromSnappy(snc,
        "CREATE TABLE Phone(name String, number long) using column", pw)
      createRowOrColumnTableFromBeeline(hiveThriftServer,
        "CREATE TABLE Phone(name String, number long) using row", pw)
     }
    catch {
      case e : Exception => pw.println("----- " +e.getMessage + " -----")
    }
    executeDropTableFromSnappy(snc, "DROP TABLE Phone")
    executeShowTables(hiveThriftServer, "show tables in app", pw)

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 11 : Use Default Schema. " +
      "Create table,drop table and run select query should throw TABLE NOT FOUND EXCEPTION.")
    executeUseSchema(hiveThriftServer, "default", snc, spark)
    createRowOrColumnTableFromBeeline(hiveThriftServer, "create table t1(id int) using column", pw)
    try {
      snc.sql("insert into t1 select id from range(1000)")
      snc.sql("drop table t1")
      snc.sql("select * from t1")
    }
    catch {
      case e : Exception => pw.println(e.getMessage)
    }

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 12 : Use App Schema. " +
      "Create table,drop table and run select query should throw TABLE NOT FOUND EXCEPTION.")
    executeUseSchema(hiveThriftServer, "app", snc, spark)
    createRowOrColumnTableFromSnappy(snc, "create table t1(id int) using column", pw)
    try {
      hiveThriftServer.stmt = hiveThriftServer.connection.createStatement()
      hiveThriftServer.rs = hiveThriftServer.stmt.executeQuery(
        "insert into t1 select id from range(1000)")
      hiveThriftServer.rs = hiveThriftServer.stmt.executeQuery("drop table t1")
      //  Below line, logs the exception in leader logs - not desirable.
      hiveThriftServer.rs = hiveThriftServer.stmt.executeQuery("select * from t1")
    }
    catch {
      case e : SQLException => pw.println(e.getMessage)
    }

    pw.println("-------------------------------------------------------------------------------")
    pw.println("Case 13 : Use sys Schema. " +
      "Run the command from BeeLine and Snappy. Verify the table Name.")
    showTablesInSys(hiveThriftServer, snc, "show tables in sys", pw, sqlContext)

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

  def showTablesInSys(hts : HiveThriftServer, snc : SnappyContext,
                      command : String, pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
      SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, command, command,
      "showtbls_sys", pw, sqlContext)
  }

  def executeShowSchemas(hts : HiveThriftServer, command : String,
                         snc : SnappyContext, pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    hts.isFailed = false
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)
    hts.rsMetaData = hts.rs.getMetaData
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, command, command,
      "showtbls", pw, sqlContext)
  }

  def executeShowTablesInSys(hts : HiveThriftServer, command : String, snc : SnappyContext,
                             pw : PrintWriter, sqlContext : SQLContext) : Unit = {
    hts.isFailed = false
    hts.stmt = hts.connection.createStatement()
    hts.rs = hts.stmt.executeQuery(command)

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
    pw.println("Creating the table from BeeLine...")
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }

  def createRowOrColumnTableFromSnappy(snc : SnappyContext, command : String,
                                        pw : PrintWriter) : Unit = {
    pw.println("Creating the table from Snappy...")
    snc.sql(command)
  }

  def insertIntoTableFromBeeline(hts : HiveThriftServer, command : String,
                                 snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                                 sqlContext : SQLContext, table : String,
                                 tblType : String) : Unit = {
    hts.isFailed = false
    var insertChk1 : String = null
    var insertChk2 : String = null
    pw.println("Inserting into the table from beeline...")
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
    insertChk1 = "select count(*) as Total from " + table
    insertChk2 = "select * from " + table + " where id > 9900000 order by id DESC"
//  Below code was producing - set set snappydata.hiveServer.enabled=true
//  Removed the below error.
//  com.google.common.util.concurrent.UncheckedExecutionException:
//  java.lang.ClassCastException:
//  org.apache.spark.sql.SparkSession cannot be cast to org.apache.spark.sql.SnappySession
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk1,
      "insertCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk2,
      "insertCheck2" + System.currentTimeMillis(), tblType, pw, sqlContext)
  }

  def updateTableFromBeeline(hts : HiveThriftServer, command : String,
                             snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                             sqlContext : SQLContext, table : String,
                             tblType: String) : Unit = {
      pw.println("Update the table from beeline...")
      var updateChk1 : String = null
      var updateChk2 : String = null
      hts.stmt = hts.connection.createStatement()
      hts.stmt.executeQuery(command)
      updateChk1 = "select count(*) as Total from " + table
      updateChk2 = "select * from " + table + " where id < 500 order by id ASC"
      SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, updateChk1,
        "updateCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
      SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, updateChk2,
        "updateCheck2" + System.currentTimeMillis(), tblType, pw, sqlContext)
 }

  def deleteFromTableFromBeeline(hts : HiveThriftServer, command : String,
                                 snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                                 sqlContext : SQLContext, table : String,
                                 tblType: String) : Unit = {
      pw.println("Delete from table from beeline...")
      var deleteChk1 : String = null
      var deleteChk2 : String = null
      hts.stmt = hts.connection.createStatement()
      hts.stmt.executeQuery(command)
      deleteChk1 = "select count(*) as Total from " + table
      deleteChk2 = "select * from " + table
      SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, deleteChk1,
         "deleteCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
      SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, deleteChk2,
        "deleteCheck2" + System.currentTimeMillis(), tblType, pw, sqlContext)
  }

  def insertIntoTableFromSnappy(hts : HiveThriftServer, command : String,
                                snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                                sqlContext : SQLContext, table : String,
                                tblType: String) : Unit = {
    hts.isFailed = false
    var insertChk1 : String = null
    var insertChk2 : String = null
    pw.println("Inserting into the table from Snappy...")
    snc.sql(command)
    insertChk1 = "select count(*) as Total from " + table
    insertChk2 = "select id, name from " + table +
//      " where id > 3000000 order by id DESC"
      " where id > 300 order by id DESC"
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk1,
      "insertCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, insertChk2,
      "insertCheck2" + System.currentTimeMillis(), tblType, pw, sqlContext)
  }

  def updateTableFromSnappy(hts : HiveThriftServer, command : String,
                            snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                            sqlContext : SQLContext, table : String,
                            tblType: String) : Unit = {
    pw.println("Update the table from Snappy...")
    var updateChk1 : String = null
    var updateChk2 : String = null
    snc.sql(command)
    updateChk1 = "select count(*) as Total from " + table
    updateChk2 = "select id, name from " + table + " where id < 500 order by id ASC"
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, updateChk1,
      "updateCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, updateChk2,
      "updateCheck2" + System.currentTimeMillis() , tblType, pw, sqlContext)
  }

  def deleteFromTableFromSnappy(hts : HiveThriftServer, command : String,
                                snc : SnappyContext, spark : SparkSession, pw : PrintWriter,
                                sqlContext : SQLContext, table : String,
                                tblType: String) : Unit = {
    pw.println("Delete from table from Snappy...")
    var deleteChk1 : String = null
    var deleteChk2 : String = null
    snc.sql(command)
    deleteChk1 = "select count(*) as Total from " + table
    deleteChk2 = "select * from " + table
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, deleteChk1,
      "deleteCheck1" + System.currentTimeMillis(), tblType, pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSetHiveThriftServer(snc, deleteChk2,
      "deleteCheck2" + System.currentTimeMillis(), tblType, pw, sqlContext)
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
        hts.rs.getString("tableName") + "\t" + hts.rs.getString("isTemporary")
    }
    pw.println("show tables : " + "\n" + result)
  }

  def executeDropTableFromBeeLine(hts : HiveThriftServer, command : String) : Unit = {
    hts.stmt = hts.connection.createStatement()
    hts.stmt.executeQuery(command)
  }

  def executeDropTableFromSnappy(snc : SnappyContext, command : String) : Unit = {
    snc.sql(command)
  }
}
