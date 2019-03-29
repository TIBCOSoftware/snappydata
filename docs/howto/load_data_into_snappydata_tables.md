<a id="howto-load"></a>
# How to Load Data into SnappyData Tables

SnappyData relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. By integrating the loading mechanism with the Query engine (Catalyst optimizer) it is often possible to push down filters and projections all the way to the data source minimizing data transfer. Here is the list of important features:

*	**Support for many Sources** </br>There is built-in support for many data sources as well as data formats. Data can be accessed from S3, file system, HDFS, Hive, RDB, etc. Moreover, loaders have built-in support to handle CSV, Parquet, ORC, Avro, JSON, Java/Scala Objects, etc. as the data formats.
*	**Access virtually any modern data store**</br> Virtually all major data providers have a native Spark connector that complies with the Data Sources API. For example, you can load data from any RDB like Amazon Redshift, Cassandra, Redis, Elastic Search, Neo4J, etc. While thee connectors are not built-in, you can easily deploy these connectors as dependencies into a SnappyData cluster. All the connectors are typically registered in spark-packages.org.
*	**Avoid Schema wrangling** </br>Spark supports schema inference. Which means, all you need to do is point to the external source in your 'create table' DDL (or Spark SQL API) and schema definition is learned by reading in the data. There is no need to define each column and type explicitly. This is extremely useful when dealing with disparate, complex and wide data sets.
*	**Read nested, sparse data sets**</br> When data is accessed from a source, the schema inference occurs by not just reading a header but often by reading the entire data set. For instance, when reading JSON files, the structure could change from document to document. The inference engine builds up the schema as it reads each record and keeps unioning them to create a unified schema. This approach allows developers to become very productive with disparate data sets.

## Loading Data using Spark API or SQL
You can use SQL to point to any data source or use the native Spark Scala/Java API to load. For instance, you can first [create an external table](../reference/sql_reference/create-external-table.md). 

```pre
CREATE EXTERNAL TABLE <tablename> USING <any-data-source-supported> OPTIONS <options>
```

Next, use it in any SQL query or DDL. For example,


```pre
CREATE EXTERNAL TABLE STAGING_CUSTOMER USING parquet OPTIONS(path 'quickstart/src/main/resources/customerparquet')

CREATE TABLE CUSTOMER USING column OPTIONS(buckets '8') AS ( SELECT * FROM STAGING_CUSTOMER)

```

## Example - Loading Data from CSV

You can either explicitly define the schema or infer the schema and the column data types. To infer the column names, we need the CSV header to specify the names. In this example we do not have the names, so we explicitly define the schema. 

```pre
// Get a SnappySession in a local cluster
val spark: SparkSession = SparkSession
    .builder
    .appName("CreateColumnTable")
    .master("local[*]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

We explicitly define the table definition first ....

```pre
snSession.sql("CREATE TABLE CUSTOMER ( " +
    "C_CUSTKEY     INTEGER NOT NULL," +
    "C_NAME        VARCHAR(25) NOT NULL," +
    "C_ADDRESS     VARCHAR(40) NOT NULL," +
    "C_NATIONKEY   INTEGER NOT NULL," +
    "C_PHONE       VARCHAR(15) NOT NULL," +
    "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
    "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
    "C_COMMENT     VARCHAR(117) NOT NULL)" +
    "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY')")
```

**Load Data in the CUSTOMER Table from a CSV File by using Data Sources API**

```pre
val tableSchema = snSession.table("CUSTOMER").schema
val customerDF = snSession.read.schema(schema = tableSchema).csv(s"$dataFolder/customer.csv")
customerDF.write.insertInto("CUSTOMER")
```

The [Spark SQL programming guide](https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#data-sources) provides a full description of the Data Sources API 

## Example - Loading Data from Parquet Files

```pre
val customerDF = snSession.read.parquet(s"$dataDir/customer_parquet")
customerDF.write.insertInto("CUSTOMER")
```

**Inferring Schema from Data File**

A schema for the table can be inferred from the data file. Data is first introspected to learn the schema (column names and types) without requiring this input from the user. The example below illustrates reading a parquet data source and creates a new columnar table in SnappyData. The schema is automatically defined when the Parquet data files are read. 

```pre
val customerDF = snSession.read.parquet(s"quickstart/src/main/resources/customerparquet")
// props1 map specifies the properties for the table to be created
// "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
customerDF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")
```

In the code snippet below a schema is inferred from a CSV file. Column names are derived from the header in the file.

```pre
val customer_csv_DF = snSession.read.option("header", "true")
    .option("inferSchema", "true").csv("quickstart/src/main/resources/customer_with_headers.csv")

// props1 map specifies the properties for the table to be created
// "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
// For complete list of attributes refer the documentation
val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
customer_csv_DF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")
```

The source code to load the data from a CSV/Parquet files is in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala). 

## Example - Reading JSON Documents
As mentioned before when dealing with JSON you have two challenges - (1) the data can be highly nested (2) the structure of the documents can keep changing. 

Here is a simple example that loads multiple JSON records that show dealing with schema changes across documents:   [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)

!!! Note

	When loading data from sources like CSV or Parquet the files would need to be accessible from all the cluster members in SnappyData. Make sure it is NFS mounted or made accessible through the Cloud solution (shared storage like S3).

## Troubleshooting Tip
When reading or writing CSV/Parquet to and from S3, the `ConnectionPoolTimeoutException` error may be reported. To avoid this error, in the Spark context, set the value of the `fs.s3a.connection.maximum` property to a number greater than the possible number of partitions. </br>
For example, `snc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000")`