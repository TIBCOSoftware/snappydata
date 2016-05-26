package io.snappydata.benchmark

import java.sql.{Date, Statement}

import org.apache.spark.SparkContext
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{SQLContext, SaveMode, SnappyContext}


/**
 * Created by kishor on 19/10/15.
 */
object TPCHColumnPartitionedTable  {

  def createPartTable_Memsql(stmt:Statement): Unit = {
    stmt.execute("CREATE TABLE PART  ( " +
        "P_PARTKEY     INTEGER NOT NULL,"+
        "P_NAME        VARCHAR(55) NOT NULL,"+
        "P_MFGR        VARCHAR(25) NOT NULL,"+
        "P_BRAND       VARCHAR(10) NOT NULL,"+
        "P_TYPE        VARCHAR(25) NOT NULL,"+
        "P_SIZE        INTEGER NOT NULL,"+
        "P_CONTAINER   VARCHAR(10) NOT NULL,"+
        "P_RETAILPRICE DECIMAL(15,2) NOT NULL,"+
        "P_COMMENT     VARCHAR(23) NOT NULL," +
        "KEY (P_PARTKEY) USING CLUSTERED COLUMNSTORE)"
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
        "KEY (PS_PARTKEY) USING CLUSTERED COLUMNSTORE)"
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
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL," +
        "KEY (C_CUSTKEY) USING CLUSTERED COLUMNSTORE)"
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

  def createOrderTable_Memsql(stmt: Statement): Unit = {

    stmt.execute("CREATE TABLE ORDERS  ( " +
        "O_ORDERKEY       INTEGER NOT NULL," +
        "O_CUSTKEY        INTEGER NOT NULL," +
        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
        "O_ORDERDATE      DATE NOT NULL," +
        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
        "O_CLERK          CHAR(15) NOT NULL," +
        "O_SHIPPRIORITY   INTEGER NOT NULL," +
        "O_COMMENT        VARCHAR(79) NOT NULL," +
        "KEY (O_CUSTKEY) USING CLUSTERED COLUMNSTORE,"+
        "SHARD KEY(O_ORDERKEY))"
//        //"KEY (O_ORDERDATE) USING CLUSTERED COLUMNSTORE," +
//
//    stmt.execute("CREATE TABLE ORDERS  ( " +
//        "O_ORDERKEY       INTEGER NOT NULL PRIMARY KEY," +
//        "O_CUSTKEY        INTEGER NOT NULL," +
//        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
//        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
//        "O_ORDERDATE      DATE NOT NULL," +
//        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
//        "O_CLERK          CHAR(15) NOT NULL," +
//        "O_SHIPPRIORITY   INTEGER NOT NULL," +
//        "O_COMMENT        VARCHAR(79) NOT NULL," +
//        //"KEY (O_ORDERDATE) USING CLUSTERED COLUMNSTORE," +
//        "KEY(O_CUSTKEY))"
    )
    println("Created Table ORDERS")
  }

//  def createAndPopulateOrderTable(props: Map[String, String], sc: SparkContext, path: String, isSnappy: Boolean): Unit = {
//    val snappyContext = SnappyContext.getOrCreate(sc)
//    val orderData = sc.textFile(s"$path/orders.tbl")
//    val orderReadings = orderData.map(s => s.split('|')).map(s => parseOrderRow(s))
//    val orderDF = snappyContext.createDataFrame(orderReadings)
//    if (isSnappy) {
//      val p1 = Map("PARTITION_BY"-> "o_orderkey")
//
//      snappyContext.dropTable("ORDERS", ifExists = true)
//      //snappyContext.dropExternalTable("ORDERS", ifExists = true)
//      //snappyContext.createExternalTable("ORDERS", "column", orderDF.schema, p1)
//      snappyContext.createTable("ORDERS", "column", orderDF.schema, p1)
//      orderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("ORDERS")
//      //orderDF.registerAndInsertIntoExternalStore("ORDERS", props)
//      println("Created Table ORDERS")
//    } else {
//      orderDF.registerTempTable("ORDERS")
//      snappyContext.cacheTable("ORDERS")
//      val cnts = snappyContext.sql("select count(*) from ORDERS").collect()
//      for (s <- cnts) {
//        var output = s.toString()
//        println(output)
//      }
//    }
//  }

  def createAndPopulateOrderTable(props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets: String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val orderData = sc.textFile(s"$path/orders.tbl")
    val orderReadings = orderData.map(s => s.split('|')).map(s => parseOrderRow(s))
    val orderDF = sqlContext.createDataFrame(orderReadings)
    println("KBKBKBKB: Buckets : " + buckets)
    if (isSnappy) {
      val p1 = Map(("PARTITION_BY"-> "o_orderkey"),("BUCKETS"-> buckets))
      //val p1 = Map(("PARTITION_BY"-> "o_orderkey"))

      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("ORDERS", ifExists = true)
      //snappyContext.dropExternalTable("ORDERS", ifExists = true)
      //snappyContext.createExternalTable("ORDERS", "column", orderDF.schema, p1)
      snappyContext.createTable("ORDERS", "column", orderDF.schema, p1)
      orderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("ORDERS")
      //orderDF.registerAndInsertIntoExternalStore("ORDERS", props)
      println("Created Table ORDERS")
    } else {
      orderDF.registerTempTable("ORDERS")
      sqlContext.cacheTable("ORDERS")
      val cnts = sqlContext.sql("select count(*) from ORDERS").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createLineItemTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,"+
        "L_PARTKEY     INTEGER NOT NULL,"+
        "L_SUPPKEY     INTEGER NOT NULL,"+
        "L_LINENUMBER  INTEGER NOT NULL,"+
        "L_QUANTITY    DECIMAL(15,2) NOT NULL,"+
        "L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,"+
        "L_DISCOUNT    DECIMAL(15,2) NOT NULL,"+
        "L_TAX         DECIMAL(15,2) NOT NULL,"+
        "L_RETURNFLAG  CHAR(1) NOT NULL,"+
        "L_LINESTATUS  CHAR(1) NOT NULL,"+
        "L_SHIPDATE    DATE NOT NULL,"+
        "L_COMMITDATE  DATE NOT NULL,"+
        "L_RECEIPTDATE DATE NOT NULL,"+
        "L_SHIPINSTRUCT CHAR(25) NOT NULL,"+
        "L_SHIPMODE     CHAR(10) NOT NULL,"+
        "L_COMMENT      VARCHAR(44) NOT NULL,"+
        "KEY (L_PARTKEY) USING CLUSTERED COLUMNSTORE,"+
        "SHARD KEY (L_ORDERKEY)) "
        //"KEY (L_SHIPDATE) USING CLUSTERED COLUMNSTORE,"+
        //"SHARD KEY (L_ORDERKEY)) "
//    stmt.execute("CREATE TABLE LINEITEM ( " +
//        "L_ORDERKEY    INTEGER NOT NULL,"+
//        "L_PARTKEY     INTEGER NOT NULL,"+
//        "L_SUPPKEY     INTEGER NOT NULL,"+
//        "L_LINENUMBER  INTEGER NOT NULL,"+
//        "L_QUANTITY    DECIMAL(15,2) NOT NULL,"+
//        "L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,"+
//        "L_DISCOUNT    DECIMAL(15,2) NOT NULL,"+
//        "L_TAX         DECIMAL(15,2) NOT NULL,"+
//        "L_RETURNFLAG  CHAR(1) NOT NULL,"+
//        "L_LINESTATUS  CHAR(1) NOT NULL,"+
//        "L_SHIPDATE    DATE NOT NULL,"+
//        "L_COMMITDATE  DATE NOT NULL,"+
//        "L_RECEIPTDATE DATE NOT NULL,"+
//        "L_SHIPINSTRUCT CHAR(25) NOT NULL,"+
//        "L_SHIPMODE     CHAR(10) NOT NULL,"+
//        "L_COMMENT      VARCHAR(44) NOT NULL,"+
//        "PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),"+
//        "FOREIGN SHARD KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY),"+
//        "KEY (L_PARTKEY),"+
//        "KEY (L_SUPPKEY))"
    )

    println("Created Table LINEITEM")
  }

//  def createAndPopulateLineItemTable(props: Map[String, String], sc: SparkContext, path:String, isSnappy:Boolean): Unit = {
//    val snappyContext = SnappyContext.getOrCreate(sc)
//    val lineItemData = sc.textFile(s"$path/lineitem.tbl")
//    val lineItemReadings = lineItemData.map(s => s.split('|')).map(s => parseLineItemRow(s))
//    val lineOrderDF = snappyContext.createDataFrame(lineItemReadings)
//    if (isSnappy) {
//      val p1 = Map(("PARTITION_BY"-> "l_orderkey"),("COLOCATE_WITH"->"ORDERS"))
//
//      //snappyContext.dropExternalTable("LINEITEM", ifExists = true)
//      snappyContext.dropTable("LINEITEM", ifExists = true)
//      //snappyContext.createExternalTable("LINEITEM", "column", lineOrderDF.schema, p1)
//      snappyContext.createTable("LINEITEM", "column", lineOrderDF.schema, p1)
//      lineOrderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("LINEITEM")
//      //    lineOrderDF.registerAndInsertIntoExternalStore("LINEITEM", props)
//      println("Created Table LINEITEM")
//    } else {
//      lineOrderDF.registerTempTable("LINEITEM")
//      snappyContext.cacheTable("LINEITEM")
//      var cnts = snappyContext.sql("select count(*) from LINEITEM").collect()
//      for (s <- cnts) {
//        var output = s.toString()
//        println(output)
//      }
//    }
//  }

  def createAndPopulateLineItemTable(props: Map[String, String], sqlContext: SQLContext, path:String, isSnappy:Boolean, buckets: String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val lineItemData = sc.textFile(s"$path/lineitem.tbl")
    val lineItemReadings = lineItemData.map(s => s.split('|')).map(s => parseLineItemRow(s))
    val lineOrderDF = sqlContext.createDataFrame(lineItemReadings)
    if (isSnappy) {
      val p1 = Map(("PARTITION_BY"-> "l_orderkey"),("COLOCATE_WITH"->"ORDERS"),("BUCKETS"->buckets))

      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      //snappyContext.dropExternalTable("LINEITEM", ifExists = true)
      snappyContext.dropTable("LINEITEM", ifExists = true)
      //snappyContext.createExternalTable("LINEITEM", "column", lineOrderDF.schema, p1)
      snappyContext.createTable("LINEITEM", "column", lineOrderDF.schema, p1)
      lineOrderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("LINEITEM")
      //    lineOrderDF.registerAndInsertIntoExternalStore("LINEITEM", props)
      println("Created Table LINEITEM")
    } else {
      lineOrderDF.registerTempTable("LINEITEM")
      sqlContext.cacheTable("LINEITEM")
      var cnts = sqlContext.sql("select count(*) from LINEITEM").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createPopulateCustomerTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets:String): Unit = {
    //val snappyContext = snappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val customerData = sc.textFile(s"$path/customer.tbl")
    val customerReadings = customerData.map(s => s.split('|')).map(s => TPCHRowPartitionedTable.parseCustomerRow(s))
    val customerDF = sqlContext.createDataFrame(customerReadings)

    if (isSnappy) {
      val p1 = Map(("PARTITION_BY"-> "c_custkey"),("BUCKETS"->buckets))

      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("CUSTOMER", ifExists = true)
      snappyContext.createTable("CUSTOMER", "column", customerDF.schema, p1)
      customerDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("CUSTOMER")
      println("Created Table CUSTOMER")
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


  def createPopulatePartTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets:String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val partData = sc.textFile(s"$path/part.tbl")
    val partReadings = partData.map(s => s.split('|')).map(s => TPCHRowPartitionedTable.parsePartRow(s))
    val partDF = sqlContext.createDataFrame(partReadings)

    if (isSnappy) {
      val p1 = Map(("PARTITION_BY"-> "p_partkey"),("BUCKETS"->buckets))

      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("PART", ifExists = true)
      snappyContext.createTable("PART", "column", partDF.schema, p1)
      partDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("PART")
      println("Created Table PART")
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

  def createPopulatePartSuppTable(usingOptionString: String, props: Map[String, String], sqlContext: SQLContext, path: String, isSnappy: Boolean, buckets:String): Unit = {
    //val snappyContext = SnappyContext.getOrCreate(sc)
    val sc = sqlContext.sparkContext
    val partSuppData = sc.textFile(s"$path/partsupp.tbl")
    val partSuppReadings = partSuppData.map(s => s.split('|')).map(s => TPCHRowPartitionedTable.parsePartSuppRow(s))
    val partSuppDF = sqlContext.createDataFrame(partSuppReadings)

    if (isSnappy) {
      val p1 = Map(("PARTITION_BY"-> "ps_partkey"),("BUCKETS"->buckets))

      val snappyContext = sqlContext.asInstanceOf[SnappyContext]
      snappyContext.dropTable("PARTSUPP", ifExists = true)
      snappyContext.createTable("PARTSUPP", "column", partSuppDF.schema, p1)
      partSuppDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("PARTSUPP")
      println("Created Table PARTSUPP")
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


  def createAndPopulateOrderSampledTable(props: Map[String, String],
      sc: SparkContext, path: String): Unit = {
    val snappyContext = SnappyContext(sc)
    val orderDF = snappyContext.table("ORDERS")
    val orderSampled = orderDF.stratifiedSample(Map(
      "qcs" -> "O_ORDERDATE", // O_SHIPPRIORITY
      "fraction" -> 0.03,
      "strataReservoirSize" -> 50))
    orderSampled.registerTempTable("ORDERS_SAMPLED")
    snappyContext.cacheTable("orders_sampled")
    println("Created Sampled Table ORDERS_SAMPLED " + snappyContext.sql(
      "select count(*) as sample_count from orders_sampled").collectAsList())
  }

  def createAndPopulateLineItemSampledTable(props: Map[String, String],
      sc: SparkContext, path: String): Unit = {
    val snappyContext = SnappyContext(sc)
    val lineOrderDF = snappyContext.table("LINEITEM")
    val lineOrderSampled = lineOrderDF.stratifiedSample(Map(
      "qcs" -> "L_SHIPDATE", // L_RETURNFLAG
      "fraction" -> 0.03,
      "strataReservoirSize" -> 50))
    println(" Logic relation while creation " + lineOrderSampled.logicalPlan.output)
    lineOrderSampled.registerTempTable("LINEITEM_SAMPLED")
    snappyContext.cacheTable("lineitem_sampled")
    println("Created Sampled Table LINEITEM_SAMPLED " + snappyContext.sql(
      "select count(*) as sample_count from lineitem_sampled").collectAsList())
  }

  case class StreamMessageOrderObject(
      o_orderkey:Int,
      o_custkey:Int,
      o_orderstatus:String,
      o_totalprice:Double,
      o_orderdate:Date,
      o_orderpriority:String,
      o_clerk:String,
      o_shippriority:Int,
      o_comment:String
      )

  def  parseOrderRow(s: Array[String]): StreamMessageOrderObject = {
    StreamMessageOrderObject(
      s(0).toInt,
      s(1).toInt,
      s(2),
      s(3).toDouble,
      formatDate(s(4)),
      s(5),
      s(6),
      s(7).toInt,
      s(8)
    )
  }

//  case class StreamMessageLineItemObject(
//      L_ORDERKEY:Int,
//      L_PARTKEY:Int,
//      L_SUPPKEY:Int,
//      L_LINENUMBER:Int,
//      L_QUANTITY:Double,
//      L_EXTENDEDPRICE:Double,
//      L_DISCOUNT:Double,
//      L_TAX:Double,
//      L_RETURNFLAG:String,
//      L_LINESTATUS:String,
//      L_SHIPDATE:Date,
//      L_COMMITDATE:Date,
//      L_RECEIPTDATE:Date,
//      L_SHIPINSTRUCT:String,
//      L_SHIPMODE:String,
//      L_COMMENT:String
//      )

  case class StreamMessageLineItemObject(
      l_orderkey:Int,
      l_partkey:Int,
      l_suppkey:Int,
      l_linenumber:Int,
      l_quantity:Double,
      l_extendedprice:Double,
      l_discount:Double,
      l_tax:Double,
      l_returnflag:String,
      l_linestatus:String,
      l_shipdate:Date,
      l_commitdate:Date,
      l_receiptdate:Date,
      l_shipinstruct:String,
      l_shipmode:String,
      l_comment:String
      )

  def parseLineItemRow(s: Array[String]): StreamMessageLineItemObject = {
    StreamMessageLineItemObject(
      s(0).toInt,
      s(1).toInt,
      s(2).toInt,
      s(3).toInt,
      s(4).toDouble,
      s(5).toDouble,
      s(6).toDouble,
      s(7).toDouble,
      s(8),
      s(9),
      formatDate(s(10)),
      formatDate(s(11)),
      formatDate(s(12)),
      s(13),
      s(14),
      s(15)
    )
  }

  def formatDate(dateString:String): Date = {
    val splittedDate = dateString.split("-")
    val year = splittedDate(0)
    val month = splittedDate(1)
    val day= splittedDate(2)
    new Date((year.toInt - 1900), (month.toInt -1), day.toInt)
  }

}
