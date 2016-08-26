package io.snappydata.benchmark

import java.sql.Statement

import org.apache.spark.sql.{SQLContext, SaveMode, SnappyContext}


/**
 * Created by kishor on 19/10/15.
 */
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

  def createPopulatePartTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets:String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
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
      partDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("PART")
    } else {
      partDF.registerTempTable("PART")
      sqlContext.cacheTable("PART")
      val cnts = sqlContext.sql("select count(*) from PART").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createPopulatePartSuppTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, bukcets:String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
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
      partSuppDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("PARTSUPP")
    } else {
      partSuppDF.registerTempTable("PARTSUPP")
      sqlContext.cacheTable("PARTSUPP")
      val cnts = sqlContext.sql("select count(*) from PARTSUPP").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

//  def createPopulateCustomerTable(usingOptionString: String, props: Map[String, String], sc: SparkContext, path: String, isSnappy: Boolean): Unit = {
//    val snappyContext = SnappyContext.getOrCreate(sc)
//    val customerData = sc.textFile(s"$path/customer.tbl")
//    val customerReadings = customerData.map(s => s.split('|')).map(s => parseCustomerRow(s))
//    val customerDF = snappyContext.createDataFrame(customerReadings)
//
//    if (isSnappy) {
//      snappyContext.sql(
//        """CREATE TABLE CUSTOMER (
//                C_CUSTKEY INTEGER NOT NULL PRIMARY KEY,
//                C_NAME VARCHAR(25) NOT NULL,
//                C_ADDRESS VARCHAR(40) NOT NULL,
//                C_NATIONKEY INTEGER NOT NULL ,
//                C_PHONE VARCHAR(15) NOT NULL,
//                C_ACCTBAL DECIMAL(15,2) NOT NULL,
//                C_MKTSEGMENT VARCHAR(10) NOT NULL,
//                C_COMMENT VARCHAR(117) NOT NULL
//             ) PARTITION BY COLUMN (C_CUSTKEY)
//        """ + usingOptionString
//      )
//      println("Created Table CUSTOMER")
//      customerDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("CUSTOMER")
//    } else {
//      customerDF.registerTempTable("CUSTOMER")
//      snappyContext.cacheTable("CUSTOMER")
//      val cnts = snappyContext.sql("select count(*) from CUSTOMER").collect()
//      for (s <- cnts) {
//        var output = s.toString()
//        println(output)
//      }
//    }
//  }

  def createPopulateCustomerTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets:String): Unit = {
    //val snappyContext = snappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
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
      customerDF.write.format("row").mode(SaveMode.Append)/*.options(props)*/.saveAsTable("CUSTOMER")
    } else {
      customerDF.registerTempTable("CUSTOMER")
      sqlContext.cacheTable("CUSTOMER")
      val cnts = sqlContext.sql("select count(*) from CUSTOMER").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

}
