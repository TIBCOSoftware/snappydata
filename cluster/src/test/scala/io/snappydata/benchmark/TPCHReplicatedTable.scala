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

import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext}

object TPCHReplicatedTable {

  def createRegionTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE REFERENCE TABLE REGION (" +
        "R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY," +
        "R_NAME       CHAR(25) NOT NULL," +
        "R_COMMENT    VARCHAR(152))"
    )
    println("Created Table REGION")
  }

  def createNationTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE REFERENCE TABLE NATION  (" +
        "N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY," +
        "N_NAME       CHAR(25) NOT NULL," +
        "N_REGIONKEY  INTEGER NOT NULL," +
        "N_COMMENT    VARCHAR(152))"
    )
    println("Created Table NATION")
  }

  def createSupplierTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE REFERENCE TABLE SUPPLIER ( " +
        "S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY," +
        "S_NAME        CHAR(25) NOT NULL," +
        "S_ADDRESS     VARCHAR(40) NOT NULL," +
        "S_NATIONKEY   INTEGER NOT NULL," +
        "S_PHONE       CHAR(15) NOT NULL," +
        "S_ACCTBAL     DECIMAL(15,2) NOT NULL," +
        "S_COMMENT     VARCHAR(101) NOT NULL)"
    )
    println("Created Table SUPPLIER")
  }

  def createPopulateRegionTable(usingOptionString: String, sqlContext: SQLContext, path: String,
      isSnappy: Boolean, loadPerfPrintStream: PrintStream = null,
      trace : Boolean = false, cacheTables : Boolean = true): Unit = {
    val sc = sqlContext.sparkContext
    val startTime = System.currentTimeMillis()
    val regionData = sc.textFile(s"$path/region.tbl")
    val regionReadings = regionData.map(s => s.split('|')).map(s => TPCHTableSchema
        .parseRegionRow(s))
    val regionDF = sqlContext.createDataFrame(regionReadings)
    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("REGION", ifExists = true)
      snappyContext.sql(
        """CREATE TABLE REGION (
            R_REGIONKEY INTEGER NOT NULL PRIMARY KEY,
            R_NAME VARCHAR(25) NOT NULL,
            R_COMMENT VARCHAR(152)
         ) """ + usingOptionString
      )
      println("Created Table REGION")
      regionDF.write.insertInto("REGION")
    } else {
      regionDF.createOrReplaceTempView("REGION")
      if(cacheTables) {
        sqlContext.cacheTable("REGION")
      }
      sqlContext.table("REGION").count()
    }
    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"REGION,${endTime - startTime}")
    }
  }

  def createPopulateNationTable(usingOptionString: String, sqlContext: SQLContext, path: String,
      isSnappy: Boolean, loadPerfPrintStream: PrintStream = null,
      trace : Boolean = false, cacheTables : Boolean = true): Unit = {
    val sc = sqlContext.sparkContext
    val startTime = System.currentTimeMillis()
    val nationData = sc.textFile(s"$path/nation.tbl")
    val nationReadings = nationData.map(s => s.split('|')).map(s => TPCHTableSchema
        .parseNationRow(s))
    val nationDF = sqlContext.createDataFrame(nationReadings)
    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("NATION", ifExists = true)
      snappyContext.sql(
        """CREATE TABLE NATION (
            N_NATIONKEY INTEGER NOT NULL PRIMARY KEY,
            N_NAME VARCHAR(25) NOT NULL,
            N_REGIONKEY INTEGER NOT NULL REFERENCES REGION(R_REGIONKEY),
            N_COMMENT VARCHAR(152)
         ) """ + usingOptionString
      )
      println("Created Table NATION")
      nationDF.write.insertInto("NATION")
    } else {
      nationDF.createOrReplaceTempView("NATION")
      if(cacheTables) {
        sqlContext.cacheTable("NATION")
      }
      sqlContext.table("NATION").count()
    }
    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"NATION,${endTime - startTime}")
    }
  }

  def createPopulateSupplierTable(usingOptionString: String, sqlContext: SQLContext, path: String,
      isSnappy: Boolean, loadPerfPrintStream: PrintStream = null, numberOfLoadingStages : Int = 1,
      trace : Boolean = false, cacheTables : Boolean = true)
      : Unit = {
    val sc = sqlContext.sparkContext
    val startTime = System.currentTimeMillis()
    var unionSupplierDF : DataFrame = null

    for(i <- 1 to numberOfLoadingStages) {
      // apply a tbl.i suffix to table filename only when data is loaded in more than one stages.
      var stage = ""
      if (numberOfLoadingStages > 1) {
        stage = s".$i"
      }
      val supplierData = sc.textFile(s"$path/supplier.tbl$stage")
      val supplierReadings = supplierData.map(s => s.split('|')).map(s => TPCHTableSchema
          .parseSupplierRow(s))
      val supplierDF = sqlContext.createDataFrame(supplierReadings)
      if (isSnappy) {
        if (i == 1) {
          val snappyContext = sqlContext.asInstanceOf[SnappyContext]
          snappyContext.dropTable("SUPPLIER", ifExists = true)
          snappyContext.sql(
            """CREATE TABLE SUPPLIER (
            S_SUPPKEY INTEGER NOT NULL PRIMARY KEY,
            S_NAME VARCHAR(25) NOT NULL,
            S_ADDRESS VARCHAR(40) NOT NULL,
            S_NATIONKEY INTEGER NOT NULL,
            S_PHONE VARCHAR(15) NOT NULL,
            S_ACCTBAL DECIMAL(15,2) NOT NULL,
            S_COMMENT VARCHAR(101) NOT NULL
         ) """ + usingOptionString
          )
          println("Created Table SUPPLIER")
        }
        supplierDF.write.insertInto("SUPPLIER")
      } else {
        if (i == 1) {
          unionSupplierDF = supplierDF
        } else {
          unionSupplierDF = unionSupplierDF.union(supplierDF)
        }
      }
    }
    if(!isSnappy){
      unionSupplierDF.createOrReplaceTempView("SUPPLIER")
      if(cacheTables) {
        sqlContext.cacheTable("SUPPLIER")
      }
      sqlContext.table("SUPPLIER").count()
    }

    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"Time taken to create SUPPLIER Table : ${endTime - startTime}")
    }
  }


}
