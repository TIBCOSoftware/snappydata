/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.cdcConnector


import java.io.PrintWriter
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import java.sql.SQLException

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random


object cdcValidationApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    // table file contains table namethat is semicolon seperated
    val tableFile = args(0)
    val conf = new SparkConf().
        setAppName("queringTest Application")
    val sc = SparkContext.getOrCreate(conf)
    val pw = new PrintWriter("SnappyJobOutPut.out")
    val snc = SnappyContext(sc)

    // scalastyle:off
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    Class.forName(driver)
    val url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433"
    val username = "sqldb"
    val password = "snappydata#msft1"
    val props = new Properties()
    props.put("username", username)
    props.put("password",password)
    var connection : Connection = null
    connection = DriverManager.getConnection(url, props)
    pw.println("Connection successful")
    connection.isClosed()
    val tableArray = scala.io.Source.fromFile(tableFile).getLines().mkString.split("\n")
   // val tableList = List("abc", "xyz", "pqr", "lmn")
    for(tabName <- 0 to tableArray.length - 1 ){
      //validation in case of inserts and deletes
      val snappyTableCnt = snc.sql(s"SELECT COUNT(*) FROM $tabName")
      val query1 = s"SELECT COUNT(*) FROM [testdatabase].[dbo].[$tabName]"
      val sqlServerTableCnt = connection.createStatement().executeQuery(query1)
      if(sqlServerTableCnt == snappyTableCnt){
        pw.println(s"SUCCESS :$tabName table has $snappyTableCnt records present in sqlserver and in snappy cluster ")
      }
      else
        {
          pw.println(s"FAILURE:$tabName table has incorrect number of records: sqlServer table has " +
              s"$sqlServerTableCnt and snappytable has $snappyTableCnt")
        }
    }

    //validation in case of updates:
    //TODO.
    // TODO

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    pw.close()
  }
}




