package io.snappydata.benchmark

import java.sql.Statement

import org.apache.spark.sql.{SQLContext, SaveMode, SnappyContext}

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

  def createPopulateRegionTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val regionData = sc.textFile(s"$path/region.tbl")
    val regionReadings = regionData.map(s => s.split('|')).map(s => parseRegionRow(s))
    val regionDF = sqlContext.createDataFrame(regionReadings)
    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        """CREATE TABLE REGION (
            R_REGIONKEY INTEGER NOT NULL PRIMARY KEY,
            R_NAME CHAR(25) NOT NULL,
            R_COMMENT VARCHAR(152)
         ) """ + usingOptionString
      )
      println("Created Table REGION")
      regionDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("REGION")
    } else {
      regionDF.registerTempTable("REGION")
      sqlContext.cacheTable("REGION")
      val cnts = sqlContext.sql("select count(*) from REGION").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createPopulateNationTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val nationData = sc.textFile(s"$path/nation.tbl")
    val nationReadings = nationData.map(s => s.split('|')).map(s => parseNationRow(s))
    val nationDF = sqlContext.createDataFrame(nationReadings)
    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        """CREATE TABLE NATION (
            N_NATIONKEY INTEGER NOT NULL PRIMARY KEY,
            N_NAME CHAR(25) NOT NULL,
            N_REGIONKEY INTEGER NOT NULL REFERENCES REGION(R_REGIONKEY),
            N_COMMENT VARCHAR(152)
         ) """ + usingOptionString
      )
      println("Created Table NATION")
      nationDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("NATION")
    } else {
      nationDF.registerTempTable("NATION")
      sqlContext.cacheTable("NATION")
      val cnts = sqlContext.sql("select count(*) from NATION").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createPopulateSupplierTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val supplierData = sc.textFile(s"$path/supplier.tbl")
    val supplierReadings = supplierData.map(s => s.split('|')).map(s => parseSupplierRow(s))
    val supplierDF = sqlContext.createDataFrame(supplierReadings)
    if (isSnappy) {
      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.sql(
        """CREATE TABLE SUPPLIER (
            S_SUPPKEY INTEGER NOT NULL PRIMARY KEY,
            S_NAME CHAR(25) NOT NULL,
            S_ADDRESS VARCHAR(40) NOT NULL,
            S_NATIONKEY INTEGER NOT NULL,
            S_PHONE CHAR(15) NOT NULL,
            S_ACCTBAL DECIMAL(15,2) NOT NULL,
            S_COMMENT VARCHAR(101) NOT NULL
         ) """ + usingOptionString
      )
      println("Created Table SUPPLIER")
      supplierDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("SUPPLIER")
    } else {
      supplierDF.registerTempTable("SUPPLIER")
      sqlContext.cacheTable("SUPPLIER")
      val cnts = sqlContext.sql("select count(*) from SUPPLIER").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  case class StreamMessageRegionObject(
      r_regionkey: Int,
      r_name: String,
      r_comment: String
      )

  def parseRegionRow(s: Array[String]): StreamMessageRegionObject = {
    StreamMessageRegionObject(
      s(0).toInt,
      s(1),
      s(2)
    )
  }

  case class StreamMessageNationObject(
      n_nationkey: Int,
      n_name: String,
      n_regionkey: Int,
      n_comment: String
      )

  def parseNationRow(s: Array[String]): StreamMessageNationObject = {
    StreamMessageNationObject(
      s(0).toInt,
      s(1),
      s(2).toInt,
      s(3)
    )
  }

  case class StreamMessageSupplierObject(
      s_suppkey: Int,
      s_name: String,
      s_address: String,
      s_nationkey: Int,
      s_phone: String,
      s_acctbal: Double,
      s_comment: String
      )

  def parseSupplierRow(s: Array[String]): StreamMessageSupplierObject = {
    StreamMessageSupplierObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3).toInt,
      s(4),
      s(5).toDouble,
      s(6)
    )
  }

}
