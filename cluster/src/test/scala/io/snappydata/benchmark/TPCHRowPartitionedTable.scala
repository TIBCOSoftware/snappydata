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
package io.snappydata.benchmark

import java.io.PrintStream
import java.sql.Statement

import org.apache.spark.sql.{SQLContext, SnappyContext}

object TPCHRowPartitionedTable {

  def createPartTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE PART  ( " +
        "P_PARTKEY     INTEGER NOT NULL PRIMARY KEY,"+
        "P_NAME        VARCHAR(55) NOT NULL,"+
        "P_MFGR        VARCHAR(25) NOT NULL,"+
        "P_BRAND       VARCHAR(10) NOT NULL,"+
        "P_TYPE        VARCHAR(25) NOT NULL,"+
        "P_SIZE        INTEGER NOT NULL,"+
        "P_CONTAINER   VARCHAR(10) NOT NULL,"+
        "P_RETAILPRICE DECIMAL(15,2) NOT NULL,"+
        "P_COMMENT     VARCHAR(23) NOT NULL)"
    )
    println("Created Table PART")
  }


  def createPartSuppTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE PARTSUPP ( " +
        "PS_PARTKEY     INTEGER NOT NULL," +
        "PS_SUPPKEY     INTEGER NOT NULL," +
        "PS_AVAILQTY    INTEGER NOT NULL," +
        "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL," +
        "PS_COMMENT     VARCHAR(199) NOT NULL," +
        "PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY))"
//    stmt.execute("CREATE TABLE PARTSUPP ( " +
//        "PS_PARTKEY     INTEGER NOT NULL," +
//        "PS_SUPPKEY     INTEGER NOT NULL," +
//        "PS_AVAILQTY    INTEGER NOT NULL," +
//        "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL," +
//        "PS_COMMENT     VARCHAR(199) NOT NULL," +
//        "SHARD KEY(PS_PARTKEY),"+
//        "KEY(PS_SUPPKEY),"+
//        "PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY))"

    )
    println("Created Table PARTSUPP")
  }

  def createCustomerTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)"
//    stmt.execute("CREATE TABLE CUSTOMER ( " +
//        "C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY," +
//        "C_NAME        VARCHAR(25) NOT NULL," +
//        "C_ADDRESS     VARCHAR(40) NOT NULL," +
//        "C_NATIONKEY   INTEGER NOT NULL," +
//        "C_PHONE       VARCHAR(15) NOT NULL," +
//        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
//        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
//        "C_COMMENT     VARCHAR(117) NOT NULL,"+
//        "KEY(C_NATIONKEY))"
    )
    println("Created Table CUSTOMER")
  }

  def createPopulatePartTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext,
      path: String, isSnappy: Boolean, buckets:String, loadPerfPrintStream : PrintStream): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val partData = sc.textFile(s"$path/part.tbl")
    val partReadings = partData.map(s => s.split('|')).map(s => TPCHTableSchema.parsePartRow(s))
    val partDF = sqlContext.createDataFrame(partReadings)

    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        s"""CREATE TABLE PART (
                P_PARTKEY INTEGER NOT NULL PRIMARY KEY,
                P_NAME VARCHAR(55) NOT NULL,
                P_MFGR VARCHAR(25) NOT NULL,
                P_BRAND VARCHAR(10) NOT NULL,
                P_TYPE VARCHAR(25) NOT NULL,
                P_SIZE INTEGER NOT NULL,
                P_CONTAINER VARCHAR(10) NOT NULL,
                P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                P_COMMENT VARCHAR(23) NOT NULL
             ) PARTITION BY COLUMN (P_PARTKEY)
             BUCKETS $buckets
        """ + usingOptionString
      )
      println("Created Table PART")
      partDF.write.insertInto("PART")
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create PART Table : ${endTime-startTime}")
    } else {
      partDF.createOrReplaceTempView("PART")
      sqlContext.cacheTable("PART")
      sqlContext.table("PART").count()
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create PART Table : ${endTime-startTime}")
    }
  }

  def createPopulatePartSuppTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext,
      path: String, isSnappy: Boolean, bukcets:String, loadPerfPrintStream : PrintStream): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val partSuppData = sc.textFile(s"$path/partsupp.tbl")
    val partSuppReadings = partSuppData.map(s => s.split('|')).map(s => TPCHTableSchema.parsePartSuppRow(s))
    val partSuppDF = sqlContext.createDataFrame(partSuppReadings)

    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        s"""CREATE TABLE PARTSUPP (
                PS_PARTKEY INTEGER NOT NULL,
                PS_SUPPKEY INTEGER NOT NULL,
                PS_AVAILQTY INTEGER NOT NULL,
                PS_SUPPLYCOST DECIMAL(15,2) NOT NULL,
                PS_COMMENT VARCHAR(199) NOT NULL,
                PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
             ) PARTITION BY COLUMN (PS_PARTKEY) COLOCATE WITH (PART)
             BUCKETS $bukcets
        """ + usingOptionString
      )
      println("Created Table PARTSUPP")
      partSuppDF.write.insertInto("PARTSUPP")
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create PARTSUPP Table : ${endTime-startTime}")
    } else {
      partSuppDF.createOrReplaceTempView("PARTSUPP")
      sqlContext.cacheTable("PARTSUPP")
      sqlContext.table("PARTSUPP").count()
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create PARTSUPP Table : ${endTime-startTime}")
    }
  }

  def createPopulateCustomerTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext,
      path: String, isSnappy: Boolean, buckets:String, loadPerfPrintStream : PrintStream): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val customerData = sc.textFile(s"$path/customer.tbl")
    val customerReadings = customerData.map(s => s.split('|')).map(s => TPCHTableSchema.parseCustomerRow(s))
    val customerDF = sqlContext.createDataFrame(customerReadings)

    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        s"""CREATE TABLE CUSTOMER (
                C_CUSTKEY INTEGER NOT NULL PRIMARY KEY,
                C_NAME VARCHAR(25) NOT NULL,
                C_ADDRESS VARCHAR(40) NOT NULL,
                C_NATIONKEY INTEGER NOT NULL ,
                C_PHONE VARCHAR(15) NOT NULL,
                C_ACCTBAL DECIMAL(15,2) NOT NULL,
                C_MKTSEGMENT VARCHAR(10) NOT NULL,
                C_COMMENT VARCHAR(117) NOT NULL
             ) PARTITION BY COLUMN (C_CUSTKEY)
             BUCKETS $buckets
        """ + usingOptionString
      )
      println("Created Table CUSTOMER")
      customerDF.write.insertInto("CUSTOMER")
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create CUSTOMER Table : ${endTime-startTime}")
    } else {
      customerDF.createOrReplaceTempView("CUSTOMER")
      sqlContext.cacheTable("CUSTOMER")
      sqlContext.table("CUSTOMER").count()
      val endTime = System.currentTimeMillis()
      loadPerfPrintStream.println(s"Time taken to create CUSTOMER Table : ${endTime-startTime}")
    }
  }

}
