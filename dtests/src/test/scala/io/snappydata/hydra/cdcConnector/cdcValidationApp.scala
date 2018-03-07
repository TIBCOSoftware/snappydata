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
import java.sql.{Connection, DriverManager}
import java.util.Properties

import breeze.numerics.abs
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}


object cdcValidationApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    // table file contains table name that is ; seperated
    val tableFile = args(0)
    val flag = args(1)
    val conf = new SparkConf().
        setAppName("CdcValidation Application")
    val sc = SparkContext.getOrCreate(conf)
    val pw = new PrintWriter("CdcValidationApp" + flag + ".out")
    val snc = SnappyContext(sc)
    var outPutTable: Array[String] = null
    var snappyTableCnt1Arr: Array[String] = null // = new Array[String](10)
    var sqlServerTableCnt1Arr: Array[String] = null // = new Array[String](10)
    var cnt1 = 0;
    var cnt2 = 0;

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off
    Try {
      val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      Class.forName(driver)
      val url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433"
      val username = "sqldb"
      val password = "snappydata#msft1"
      val props = new Properties()
      props.put("username", username)
      props.put("password", password)
      var connection: Connection = null
      connection = DriverManager.getConnection(url, props)
      connection.isClosed()
      val tableArray = scala.io.Source.fromFile(tableFile).getLines().mkString.split(";")

      //flag value indicates the sequence of app execution :
      // Here it checks if it is the 2nd execution of the app.
      if(flag.equals(2.toString)){
        outPutTable = scala.io.Source.fromFile(s"${getCurrentDirectory}/CdcValidationApp1.out").getLines().mkString.split(";")
        snappyTableCnt1Arr =  new Array[String](outPutTable.length)
        sqlServerTableCnt1Arr =  new Array[String](outPutTable.length)

        /* output file is of the format
        SnappyTable:tableName = cnt
        SqlServerTable:tableName = cnt
        So snappyTable cnt will be even and sqlServer cnt will be odd and separate the counts in 2 different lists for validation:
        */
        for(i <- 0 to outPutTable.length - 1){
          if(i%2 == 0)
          {
            val tableStm = outPutTable(i )
            val arr = tableStm.split("=")
            snappyTableCnt1Arr(cnt2) = arr(1)
            cnt2 = +1
           }
          else
          {
            val tableStm = outPutTable(i)
            val arr = tableStm.split("=")
            sqlServerTableCnt1Arr(cnt1) = arr(1)
            cnt1 = +1
          }
        }
      }

       for (j <- 0 to tableArray.length - 1) {
        //validation in case of inserts and deletes
        val tableName = tableArray(j)
        var snappyTableCnt = 0.0
        var sqlServerTableCnt = 0.0
        val snappyResultSet = snc.sql(s"SELECT COUNT(*) FROM $tableName").collect()
        for(a <- 0 to snappyResultSet.length - 1){
          snappyTableCnt = snappyResultSet(a).getLong(0)
        }
        pw.println("SnappyTable:" + tableName + "="+snappyTableCnt+";")

        val query1 = s"SELECT COUNT(*) FROM [testdatabase].[dbo].[$tableName]"
        val resultSet = connection.createStatement().executeQuery(query1)
        while(resultSet.next()){
          sqlServerTableCnt = resultSet.getLong(1)
        }
        pw.println("SqlServerTable:" + tableName + "="+sqlServerTableCnt+";")

        if(flag.equals(2.toString )) {
          // Match the difference :
          pw.println("=====================================================================================================================")
          pw.println("For table " + tableName+" Present sqlServer cnt = " +sqlServerTableCnt + " Previous sqlServer cnt "+sqlServerTableCnt1Arr(j))
          pw.println("For table " + tableName+" Present snappy cnt = " +snappyTableCnt + " Previous snappy cnt "+snappyTableCnt1Arr(j))
          val sqlServerCntDiff = abs(sqlServerTableCnt - sqlServerTableCnt1Arr(j).toDouble)
          val snappyCntDiff = abs(snappyTableCnt - snappyTableCnt1Arr(j).toDouble)

          if (sqlServerCntDiff == snappyCntDiff) {
            pw.println(s"SUCCESS :$tableName in snappy cluster  has equal difference of $snappyCntDiff before and after ingestion ")
            pw.println(s"SUCCESS :$tableName in sqlServer cluster  has equal difference of $sqlServerCntDiff before and after ingestion")
          }
          else {
            pw.println("FAILURE:" +tableName +" table has incorrect number of records: sqlServer table has "
                +sqlServerTableCnt +" and snappytable has "+snappyTableCnt)
          }
          pw.println("=====================================================================================================================")
          pw.println()
        }
      }

      //validation in case of updates:
      //TODO.
      //TODO

      // def getCurrentDirectory = new java.io.File(".").getCanonicalPath
      //pw.close()
    }
    match
    {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/CdcValidationApp" + flag + ".out"
      case Failure(e) => pw.close();
        throw e;

    }
  }
}




