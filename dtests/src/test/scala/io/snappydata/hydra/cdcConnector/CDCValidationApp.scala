/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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


import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties


import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}


import scala.util.{Failure, Random, Success, Try}


object CDCValidationApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    // table file contains table name that is ; seperated
    Thread.sleep(60000)
    val tableFile = args(0)
    val flag = args(1)
    val dbName = args(2)
    val sqlServerInstance = args(3)
    val appName = args(4)
    val endRange = args(5).toInt
    val conf = new SparkConf().setAppName(appName)
    val sc = SparkContext.getOrCreate(conf)
    val pw = new PrintWriter(appName + flag + ".out")
    val pw1 = new PrintWriter(appName + ".out")
    val snc = SnappyContext(sc)
    var outPutTable: Array[String] = null
    var snappyTableCnt1Arr: Array[String] = null
    var sqlServerTableCnt1Arr: Array[String] = null
    var cnt1 = 0;
    var cnt2 = 0;
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    var connection: Connection = null

    // scalastyle:off
    Try {
      var hostPort = ""
      if(sqlServerInstance.equals("sqlServer1")){
        hostPort = "sqlent.westus.cloudapp.azure.com:1433"
      }
      else{
        hostPort = "sqlserver2-et16.copfedn1qbcz.us-west-2.rds.amazonaws.com:1435"
      }
      val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      Class.forName(driver)
      val sqlServerURL = s"jdbc:sqlserver://$hostPort"
      val username = "sqldb"
      val password = "snappydata#msft1"
      val props = new Properties()
      props.put("username", username)
      props.put("password", password)
      connection = DriverManager.getConnection(sqlServerURL, props)
      connection.isClosed()

      val tableArray = scala.io.Source.fromFile(tableFile).getLines().mkString.split(";")
      val idNameArr: Array[String] = new Array[String](tableArray.length)
      val tableNameArr: Array[String] = new Array[String](tableArray.length)
      for (i <- 0 to tableArray.length - 1) {
        val tempArr = tableArray(i).split("=")
        tableNameArr(i) = tempArr(0)
        idNameArr(i) = tempArr(1)
      }

      //flag value indicates the sequence of app execution :
      // Here it checks if it is the 2nd execution of the app.
      if (flag.equals(2.toString)) {
        System.out.println("Inside flag = 2")
        outPutTable = scala.io.Source.fromFile(s"${getCurrentDirectory}/"+appName+"1.out").getLines().mkString.split(";")
        snappyTableCnt1Arr = new Array[String](outPutTable.length)
        sqlServerTableCnt1Arr = new Array[String](outPutTable.length)

        /* output file is of the format
        SnappyTable:tableName = cnt
        SqlServerTable:tableName = cnt
        So snappyTable cnt will be even and sqlServer cnt will be odd and separate the counts in 2 different lists for validation:
        */
        for (i <- 0 to outPutTable.length - 1) {
          if (i % 2 == 0) {
            val tableStm = outPutTable(i)
            val arr = tableStm.split("=")
            snappyTableCnt1Arr(cnt2) = arr(1)
            cnt2 += 1
          }
          else {
            val tableStm = outPutTable(i)
            val arr = tableStm.split("=")
            sqlServerTableCnt1Arr(cnt1) = arr(1)
            cnt1 += 1
          }
        }
        printMapValues(snappyTableCnt1Arr)
        printMapValues(sqlServerTableCnt1Arr)
      }

     // else {
        for (j <- 0 to tableNameArr.length - 1) {
          //validation in case of inserts and deletes
         // val startRange = SnappyCDCPrms.getInitStartRange()

          val tableName = tableNameArr(j)
          var snappyTableCnt: Long = 0l
          var sqlServerTableCnt: Long = 0l
          val snappyResultSet = snc.sql(s"SELECT COUNT(*) FROM $tableName").collect()
          for (a <- 0 to snappyResultSet.length - 1) {
            snappyTableCnt = snappyResultSet(a).getLong(0)
          }
          pw.println("SnappyTable:" + tableName + "=" + snappyTableCnt + ";")
         for(k <- 1 to 5) {
           val sqlTable = s"[" + dbName + "].[dbo].[" + tableName + k + "]"
            val query1 = s"SELECT COUNT(*) FROM $sqlTable"
            val resultSet = connection.createStatement().executeQuery(query1)
            while (resultSet.next()) {
              sqlServerTableCnt = sqlServerTableCnt + resultSet.getLong(1)
            }
            pw.println("SqlServerTable:" + tableName + "=" + sqlServerTableCnt + ";")
          }

          if (flag.equals(2.toString)) {

            //Do full resultSet validation:
            pw.println()
            pw.println("=============================================ResultSet Validation ==================================================")
            val rndNo = new Random()
            val idVal = rndNo.nextInt(endRange) + 10
            var num = rndNo.nextInt(5)
            if(num == 0) {
              num += 1
            }
            val sqlTab = "[testdatabase].[dbo].[" + tableName + num + "]"
            val sqlQ = "SELECT * FROM "+ sqlTab +" WHERE "+ idNameArr(j) +" = " + idVal
            System.out.println("The sql query is " + sqlQ)
            val snappyQ = "SELECT * FROM "+ tableName +" WHERE "+ idNameArr(j) +" = " + idVal
            System.out.println("The snappy query is "+ snappyQ)
            checkDataConsistency(endRange)
            //fullResultSetValidation(snc, connection, sqlQ, snappyQ, pw)
            pw.println("=====================================================================================================================")
            // Match the difference :
            pw.println("=================================================Count Validation=====================================================")
            pw.println("For table " + tableName + " Present sqlServer cnt = " + sqlServerTableCnt + " Previous sqlServer cnt " + sqlServerTableCnt1Arr(j))
            pw.println("For table " + tableName + " Present snappy cnt = " + snappyTableCnt + " Previous snappy cnt " + snappyTableCnt1Arr(j))
            val sqlServerCntDiff = sqlServerTableCnt - sqlServerTableCnt1Arr(j).toLong
            val snappyCntDiff = snappyTableCnt - snappyTableCnt1Arr(j).toLong

            if (sqlServerCntDiff == snappyCntDiff) {
              pw.println(s"SUCCESS :$tableName in snappy cluster  has equal difference of $snappyCntDiff before and after ingestion ")
              pw.println(s"SUCCESS :$tableName in sqlServer cluster  has equal difference of $sqlServerCntDiff before and after ingestion")
            }
            else {
              pw.println("FAILURE:" + tableName + " table has incorrect number of records: sqlServer table has "
                  + sqlServerTableCnt + " and snappytable has " + snappyTableCnt)
            }
            pw.println("=====================================================================================================================")
            pw.println()
          }
        }
    //  }

      //Perform full result set validation
      def fullResultSetValidation(snc: SnappyContext, sqlConn: Connection, sqlString: String, snappyString: String, pw: PrintWriter): Any = {
        System.out.println("Inside fullResultSetValidation")
        val snappyDF = snc.sql(snappyString)
        var snappyColVal: Any =
          System.out.println("SnappyDF is " + snappyDF.show())
        val snappycolNames = snappyDF.columns
        val sqlResultSet = sqlConn.createStatement().executeQuery(sqlString)
        while (sqlResultSet.next()) {
          val resultMetaData = sqlResultSet.getMetaData
          val columnCnt = resultMetaData.getColumnCount
          System.out.println("columnCnt is " + columnCnt)
          for (i <- 1 to columnCnt) {
            System.out.println("The column type is " + resultMetaData.getColumnTypeName(i))
            val sqlColVal = sqlResultSet.getObject(i)
            val snappyVal = snappyDF.select(snappycolNames(i - 1)).collect()
            snappyVal.map(x => snappyColVal = x.get(0))
            System.out.println("sqlColVal = " + sqlColVal)
            System.out.println("sqlColVal = " + sqlColVal + " snappyColVal = " + snappyColVal)
            if (sqlColVal.equals(snappyColVal)) {
                pw.println("sqlColVal = " + sqlColVal + " is EQUAL to  snappyColVal = " + snappyColVal)
            }
            else {
              pw.println("FAILURE : sqlColVal = " + sqlColVal + " is NOT EQUAL to  snappyColVal = " + snappyColVal)
            }
          }
        }
      }

       // For printing the map values.
      def printMapValues(arr : Array[String]): Unit = {
        pw1.println(s"$arr values are : ")
        for(i <-0 until arr.length - 1){
          val value = arr(i)
          pw1.println(s"$value")
        }
      }

      //validation in case of updates:
      def checkDataConsistency(endRange : Integer): Unit = {
        // pw.println("Inside checkDataConsistency function")
       // Random rnd = new Random()
        for (i <- 0 until tableNameArr.length - 1) {
          val tableName = tableNameArr(i)
          val idName = idNameArr(i)
          val sqlServerQ = s"SELECT * FROM [testdatabase].[dbo].[$tableName] WHERE $idName > $endRange "
          pw.println("Query is " + sqlServerQ)
          val sqlServerResultSet = connection.createStatement().executeQuery(sqlServerQ)
          val snappyDF = snc.sql(s"SELECT * FROM $tableName")
          //.collect()
          var cnt = 0
          val colName = idNameArr(i)

          // List of snappy rows for a table.
          val snappyList = snappyDF.select(s"$colName").collectAsList()
          val snappArr = snappyList.toArray
          pw.println("List size = " + snappArr.length + " value is " + snappArr(i))

          val sqlServerIdValArr: Array[String] = new Array[String](snappyDF.count().toInt)

          while (sqlServerResultSet.next()) {
            sqlServerIdValArr(cnt) = sqlServerResultSet.getString(colName)
            cnt = +1
          }
          pw.println("sqlServerIdValArr size = = " + sqlServerIdValArr.length)

          val diff = sqlServerIdValArr.toSet.diff(snappArr.toSet)

          pw.println("The difference is " + diff)
          pw.println("==========================================================================")
          if(diff.nonEmpty)
            {
              pw.println("FAILURE: The sqlerver set doesnot match with snappy set")
            }
          else
           pw.println("SUCCESS: The sqlserver set matches with snappy set .")
          pw.println("==========================================================================")
          pw.println()


        }
      }
    }
    match {
      case Success(v) => pw.close() ;
        pw1.close();connection.close()
        s"See $getCurrentDirectory/"+appName + flag + ".out"
      case Failure(e) => pw.close(); pw1.close(); connection.close()
        throw e;

    }
  }
}




