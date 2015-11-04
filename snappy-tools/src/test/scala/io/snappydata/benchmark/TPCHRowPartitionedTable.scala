package io.snappydata.benchmark

import java.sql.Statement

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SnappyContext, SaveMode}


/**
 * Created by kishor on 19/10/15.
 */
object TPCHRowPartitionedTable {

  def createPartTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE PART  ( " +
        "P_PARTKEY     INTEGER NOT NULL PRIMARY KEY,"+
        "P_NAME        VARCHAR(55) NOT NULL,"+
        "P_MFGR        CHAR(25) NOT NULL,"+
        "P_BRAND       CHAR(10) NOT NULL,"+
        "P_TYPE        VARCHAR(25) NOT NULL,"+
        "P_SIZE        INTEGER NOT NULL,"+
        "P_CONTAINER   CHAR(10) NOT NULL,"+
        "P_RETAILPRICE DECIMAL(15,2) NOT NULL,"+
        "P_COMMENT     VARCHAR(23) NOT NULL)"
    )
    println("Created Table PART")
  }


  def createPartTable(usingOptionString: String, sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    snappyContext.sql(
      """CREATE TABLE PART (
                P_PARTKEY INTEGER NOT NULL PRIMARY KEY,
                P_NAME VARCHAR(55) NOT NULL,
                P_MFGR CHAR(25) NOT NULL,
                P_BRAND CHAR(10) NOT NULL,
                P_TYPE VARCHAR(25) NOT NULL,
                P_SIZE INTEGER NOT NULL,
                P_CONTAINER CHAR(10) NOT NULL,
                P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                P_COMMENT VARCHAR(23) NOT NULL
             ) PARTITION BY COLUMN (P_PARTKEY)
      """ + usingOptionString
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
    )
    println("Created Table PARTSUPP")
  }

  def createPartSuppTable(usingOptionString: String, sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    snappyContext.sql(
      """CREATE TABLE PARTSUPP (
                PS_PARTKEY INTEGER NOT NULL,
                PS_SUPPKEY INTEGER NOT NULL,
                PS_AVAILQTY INTEGER NOT NULL,
                PS_SUPPLYCOST DECIMAL(15,2) NOT NULL,
                PS_COMMENT VARCHAR(199) NOT NULL,
                PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
             ) PARTITION BY COLUMN (PS_PARTKEY)
      """ + usingOptionString
    )
    println("Created Table PARTSUPP")
  }

  def createCustomerTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       CHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  CHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)"
    )
    println("Created Table CUSTOMER")
  }

  def createCustomerTable(usingOptionString: String, sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    snappyContext.sql(
      """CREATE TABLE CUSTOMER (
                C_CUSTKEY INTEGER NOT NULL PRIMARY KEY,
                C_NAME VARCHAR(25) NOT NULL,
                C_ADDRESS VARCHAR(40) NOT NULL,
                C_NATIONKEY INTEGER NOT NULL ,
                C_PHONE CHAR(15) NOT NULL,
                C_ACCTBAL DECIMAL(15,2) NOT NULL,
                C_MKTSEGMENT CHAR(10) NOT NULL,
                C_COMMENT VARCHAR(117) NOT NULL
             ) PARTITION BY COLUMN (C_CUSTKEY)
      """ + usingOptionString
    )
    println("Created Table CUSTOMER")
  }

  def populatePartTable(props: Map[String, String], sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    val partData = sc.textFile("/home/kishor/snappy/TPCH_Data/GB1/part.tbl")
    val partReadings = partData.map(s => s.split('|')).map(s => parsePartRow(s))
    val partDF = snappyContext.createDataFrame(partReadings)
    partDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("PART")
  }

  def populatePartSuppTable(props: Map[String, String], sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    val partSuppData = sc.textFile("/home/kishor/snappy/TPCH_Data/GB1/partsupp.tbl")
    val partSuppReadings = partSuppData.map(s => s.split('|')).map(s => parsePartSuppRow(s))
    val partSuppDF = snappyContext.createDataFrame(partSuppReadings)
    partSuppDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("PARTSUPP")
  }

  def populateCustomerTable(props: Map[String, String], sc: SparkContext): Unit = {
    val snappyContext = SnappyContext(sc)
    val customerData = sc.textFile("/home/kishor/snappy/TPCH_Data/GB1/customer.tbl")
    val customerReadings = customerData.map(s => s.split('|')).map(s => parseCustomerRow(s))
    val customerDF = snappyContext.createDataFrame(customerReadings)
    customerDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("CUSTOMER")
  }


  case class StreamMessagePartObject(
      p_partkey: Int,
      p_name: String,
      p_mfgr: String,
      p_brand: String,
      p_type: String,
      p_size: Int,
      p_container: String,
      p_retailprice: Double,
      p_comment: String
  )

  def parsePartRow(s: Array[String]): StreamMessagePartObject = {
    StreamMessagePartObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3),
      s(4),
      s(5).toInt,
      s(6),
      s(7).toDouble,
      s(8)
    )
  }

  case class StreamMessagePartSuppObject(
      ps_partkey: Int,
      ps_suppkey: Int,
      ps_availqty: Int,
      ps_supplycost: Double,
      ps_comment: String
  )

  def parsePartSuppRow(s: Array[String]): StreamMessagePartSuppObject = {
    StreamMessagePartSuppObject(
      s(0).toInt,
      s(1).toInt,
      s(2).toInt,
      s(3).toDouble,
      s(4)
    )
  }

  case class StreamMessageCustomerObject(
      C_CUSTKEY: Int,
      C_NAME: String,
      C_ADDRESS: String,
      C_NATIONKEY: Int,
      C_PHONE: String,
      C_ACCTBAL: Double,
      C_MKTSEGMENT: String,
      C_COMMENT: String
      )

  def parseCustomerRow(s: Array[String]): StreamMessageCustomerObject = {
    StreamMessageCustomerObject(
      s(0).toInt,
      s(1),
      s(2),
      s(3).toInt,
      s(4),
      s(5).toDouble,
      s(6),
      s(7)
    )
  }

}
