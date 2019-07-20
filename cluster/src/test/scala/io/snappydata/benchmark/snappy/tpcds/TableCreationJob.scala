package io.snappydata.benchmark.snappy.tpcds

import java.io.{File, FileOutputStream, PrintStream}

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object TableCreationJob extends SnappySQLJob{

  var sqlSparkProperties: Array[String] = _
  var dataLocation: String = _
  var buckets_ColumnTable: String = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    snc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000");
    val isSnappy = true

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(new File("Snappy_LoadPerf.out"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    val tables = Seq("call_center", "catalog_page", "date_dim", "household_demographics",
      "income_band", "promotion", "reason", "ship_mode", "store", "time_dim",
      "warehouse", "web_page" , "web_site", "item", "customer_demographics")

    tables.map { tableName =>
      //println(s"Table Creation Started...$tableName")
      val df = snSession.read.parquet(s"$dataLocation/$tableName")
      snSession.createTable(tableName, "row",
        new StructType(df.schema.map(_.copy(nullable = true)).toArray),
        Map[String, String] ())
      df.write.insertInto(tableName)
      val cnt = df.collect().length;
      // scalastyle:off println
      println("-----------------------------------------------")
      println(s"Table Created...$tableName with rows $cnt")

      println("-----------------------------------------------")
    }


    var props = Map(("PARTITION_BY" -> "cr_order_number"), ("BUCKETS" -> buckets_ColumnTable))
    var tableName = "catalog_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "cs_order_number"), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "CATALOG_RETURNS"))
    tableName = "catalog_sales"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "wr_order_number"), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "CATALOG_SALES"))
    tableName = "web_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ws_order_number"), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "WEB_RETURNS"))
    tableName = "web_sales"
    createColumnPartitionedTables(snSession, props, tableName)


    props = Map(("PARTITION_BY" -> "inv_item_sk"), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "inventory"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "sr_item_sk"), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "INVENTORY"))
    tableName = "store_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ss_item_sk"), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "STORE_RETURNS"))
    tableName = "store_sales"
    createColumnPartitionedTables(snSession, props, tableName)


    props = Map(("PARTITION_BY" -> "c_customer_sk"), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "customer"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ca_address_sk"), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "customer_address"
    createColumnPartitionedTables(snSession, props, tableName)

    val avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"Snappy_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

  }

  def createColumnPartitionedTables(snappy: SnappySession,
      props: Map[String,String] , tableName: String): Unit = {

    val df = snappy.read.parquet(s"$dataLocation/$tableName")
    snappy.createTable(tableName, "column",
      new StructType(df.schema.map(_.copy(nullable = false)).toArray), props)
    df.write.insertInto(tableName)
    val cnt = df.collect().length
    // scalastyle:off println
    println("-----------------------------------------------")
    println(s"Table Created...$tableName with rows $cnt")
    println("-----------------------------------------------")
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "
    sqlSparkProperties = sqlSparkProps.split(",")

    dataLocation = if (config.hasPath("dataDir")) {
      config.getString("dataDir")
    } else {
      "/QASNAPPY/TPCH/DATA/1"
    }

    buckets_ColumnTable = if (config.hasPath("Buckets_ColumnTable")) {
      config.getString("Buckets_ColumnTable")
    } else {
      "8"
    }

    SnappyJobValid()
  }
}
