package io.snappydata.benchmark

import java.io.PrintStream
import java.sql.Statement

import org.apache.spark.sql.{SQLContext, SnappyContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCHReplicatedTable {

  def createRegionTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE REFERENCE TABLE REGION (" +
        "R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY," +
        "R_NAME       CHAR(25) NOT NULL," +
        "R_COMMENT    VARCHAR(152))"
    )
    println("Created Table REGION")
  }

  def createNationTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE REFERENCE TABLE NATION  (" +
        "N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY," +
        "N_NAME       CHAR(25) NOT NULL," +
        "N_REGIONKEY  INTEGER NOT NULL," +
        "N_COMMENT    VARCHAR(152))"
    )
    println("Created Table NATION")
  }

  def createSupplierTable_Memsql(stmt:Statement): Unit = {
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
      isSnappy: Boolean, loadPerfPrintStream: PrintStream=null): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val regionData = sc.textFile(s"$path/region.tbl")
    val regionReadings = regionData.map(s => s.split('|')).map(s => TPCHTableSchema.parseRegionRow(s))
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
      sqlContext.cacheTable("REGION")
      sqlContext.table("REGION").count()
    }
    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"Time taken to create REGION Table : ${endTime - startTime}")
    }
  }

  def createPopulateNationTable(usingOptionString: String, sqlContext: SQLContext, path: String,
      isSnappy: Boolean, loadPerfPrintStream: PrintStream=null): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val nationData = sc.textFile(s"$path/nation.tbl")
    val nationReadings = nationData.map(s => s.split('|')).map(s => TPCHTableSchema.parseNationRow(s))
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
      sqlContext.cacheTable("NATION")
      sqlContext.table("NATION").count()
    }
    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"Time taken to create NATION Table : ${endTime - startTime}")
    }
  }

  def createPopulateSupplierTable(usingOptionString: String, sqlContext: SQLContext, path: String,
      isSnappy: Boolean, loadPerfPrintStream: PrintStream=null): Unit = {
    val sc = sqlContext.sparkContext
    val startTime=System.currentTimeMillis()
    val supplierData = sc.textFile(s"$path/supplier.tbl")
    val supplierReadings = supplierData.map(s => s.split('|')).map(s => TPCHTableSchema.parseSupplierRow(s))
    val supplierDF = sqlContext.createDataFrame(supplierReadings)
    if (isSnappy) {
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
      supplierDF.write.insertInto("SUPPLIER")
    } else {
      supplierDF.createOrReplaceTempView("SUPPLIER")
      sqlContext.cacheTable("SUPPLIER")
      sqlContext.table("SUPPLIER").count()
    }
    val endTime = System.currentTimeMillis()
    if (loadPerfPrintStream != null) {
      loadPerfPrintStream.println(s"Time taken to create SUPPLIER Table : ${endTime - startTime}")
    }
  }



}
