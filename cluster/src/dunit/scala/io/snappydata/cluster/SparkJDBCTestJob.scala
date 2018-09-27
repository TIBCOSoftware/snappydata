/*
 *
 */
package io.snappydata.cluster

import java.util.Properties

import org.apache.spark.sql.SparkSession


object SparkJDBCTestJob {

  val JDBC_URL = "jdbc:snappydata:pool://localhost:" + System.getProperty("locatorPort") + "/"
  val DRIVER_NAME = "io.snappydata.jdbc.ClientPoolDriver"

  private def getProperties(): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("pool-maxTotal", "50")
    connectionProperties.setProperty("pool-maxIdle", "20")
    connectionProperties.setProperty("pool-initialSize", "50")
    connectionProperties.setProperty("pool-minActive", "50")
    connectionProperties.setProperty("pool-minIdleSize", "10")
    connectionProperties.setProperty("driver", DRIVER_NAME)
    connectionProperties
  }

  def main(args: Array[String]): Unit = {

    val builder = SparkSession.builder.appName("JDBCClientPoolDriverAPITest").master("local[*]")

    val spark: SparkSession = builder.getOrCreate

    jdbcReadAPIUrlTableProperties(spark)
    jdbcReadWithQueryPushDown(spark)
    jdbcReadPlanWithAllColumns(spark)
    jdbcReadPlanWithSpecifiedColumns(spark)
    jdbcReadPlanWithPredicatePushDown(spark)
    jdbcReadPlanWithUpperAndLowerBoundries(spark)
    // jdbcWriteWithPartitionColumn(spark)
  }

  def jdbcReadAPIUrlTableProperties(spark: SparkSession): Unit = {
    // scalastyle:off
    println("Test Spark JDBC Read API spark.read.jdbc(JDBC_URL, TABLE_NAME, properties) ")
    val properties = getProperties()
    val df = spark.read.jdbc(JDBC_URL, "AIRLINE", properties)
    val result = df.select("flightnum").groupBy("flightnum").avg("flightnum")
    result.show()
  }

  def jdbcReadWithQueryPushDown(spark: SparkSession): Unit = {

    val pushdown_query = s"(select * from AIRLINE where flightnum=2 limit 2) emp_alias"
    println(s"Test Spark JDBC Read API Query Push down spark.read.jdbc(JDBC_URL, $pushdown_query, properties) ")
    val df = spark.read.jdbc(url = JDBC_URL, table = pushdown_query, properties = getProperties)
    df.show()
  }

  def jdbcReadPlanWithAllColumns(spark: SparkSession): Unit = {
    println("Test Spark JDBC Read API Select all columns ")
    spark.read.jdbc(JDBC_URL, "airline", getProperties).show(2)
  }


  def jdbcReadPlanWithSpecifiedColumns(spark: SparkSession): Unit = {

    println("Test Spark JDBC Read API, read selected columns ")
    // Explain plan with column selection will prune columns and just return the ones specified
    // Notice that only the 3 specified columns are in the explain plan
    spark.read.jdbc(JDBC_URL, "AIRLINE", getProperties).select("flightnum", "Year", "month").show(2)
  }


  def jdbcReadPlanWithPredicatePushDown(spark: SparkSession): Unit = {

    println("Test Spark JDBC Read API, with predicate pushdown \n")
    // You can push query predicates down too
    // Notice the Filter at the top of the Physical Plan
    val df = spark.read.jdbc(JDBC_URL, "AIRLINE", getProperties).select("flightnum", "Year", "month").where("flightNum = '1'")
    df.show(2)
  }


  def jdbcReadPlanWithUpperAndLowerBoundries(spark: SparkSession): Unit = {

    println("Test Spark JDBC Read API - spark.read.jdbc(url, table, column," +
      " lowerBound, upperBound, numPartitions, properties)\n")
    // These options specify the parallelism on read.
    // These options must all be specified if any of them is specified.

    // These options specify the parallelism of the table read.
    // lowerBound and upperBound decide the partition stride, but do not filter the rows in table.
    // Therefore, Spark partitions and returns all rows in the table.
    val df = spark.read.jdbc(url = JDBC_URL,
      table = "AIRLINE",
      columnName = "YEAR",
      lowerBound = 0,
      upperBound = 100,
      numPartitions = 16,
      connectionProperties = getProperties)
    df.show()
  }


  def jdbcWriteWithPartitionColumn(spark: SparkSession): Unit = {

    println(" =============== jdbcWriteWithPartitionColumn ============== \n")

    val customerDF = spark.read.parquet(s"/home/pradeep/trunk/snappydata/examples/quickstart/data/airlineParquetData")
    // props1 map specifies the properties for the table to be created
    //   "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    val props1 = Map("PARTITION_BY" -> "Year")
    val properties = getProperties()
    properties.setProperty("PARTITION_BY", "Year")
    properties.setProperty("buckets", "16")
    // Given the number of partitions above, you can reduce the partition value by calling coalesce()
    // or increase it by calling repartition() to manage the number of connections.
    customerDF.write.saveAsTable("AIRLINE")

  }
}