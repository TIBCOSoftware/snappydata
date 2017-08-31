# Overview
This section introduces you to several common operations such as starting a cluster, working with tables (load, query, update), working with streams and running approximate queries.

**Running the Examples:**
Topics in this section refer to source code examples that are shipped with the product. Instructions to run these examples can be found in the source code.

Source code for these examples is located in the **quickstart/src/main/scala/org/apache/spark/examples/snappydata** and in **quickstart/python** directories of the SnappyData product distribution.

Refer to the [Getting Started](quickstart.md) to either run SnappyData on premise, using AWS or Docker. 

You can run the examples in any of the following ways:

* **In the Local Mode**: By using `bin/run-example` script (to run Scala examples) or by using `bin/spark-submit` script (to run Python examples). The examples run colocated with Spark+SnappyData Store in the same JVM. 

* **As a Job**:	Many of the Scala examples are also implemented as a SnappyData job. In this case, examples can be submitted as a job to a running SnappyData cluster. Refer to [jobs](#howto-job) section for details on how to run a job.

!!! Note: 
	SnappyData also supports Java API. Refer to the [documentation](programming_guide.md#building-snappy-applications-using-spark-api) for more details on Java API.

The following topics are covered in this section:

* [How to Start a SnappyData Cluster](#howto-startcluster)

* [How to Check the Status of a SnappyData Cluster](#howto-statuscluster)

* [How to Stop a SnappyData Cluster](#howto-stopcluster)

* [How to Run Spark Job inside the Cluster](#howto-job)

* [How to Access SnappyData Store from existing Spark Installation using Smart Connector](#howto-splitmode)

* [How to Create Row Tables and Run Queries](#howto-row)

* [How to Create Column Tables and Run Queries](#howto-column)

* [How to Load Data into SnappyData Tables](#howto-load)

* [How to Load Data from External Data Stores (e.g. HDFS, Cassandra, Hive, etc)](#howto-external-source)

* [How to Perform a Colocated Join](#howto-collacatedJoin)

* [How to Connect using JDBC Driver](#howto-jdbc)

* [How to Store and Query JSON Objects](#howto-JSON)

* [How to Store and Query Objects](#howto-objects)

* [How to Use Stream Processing with SnappyData](#howto-streams)

* [How to Use Synopsis Data Engine to Run Approximate Queries](#howto-sde)

* [How to Use Python to Create Tables and Run Queries](#howto-python)

* [How to Connect using ODBC Driver](#howto-odbc)

* [How to Connect to the Cluster from External Clients](#howto-external-client)

* [How to Use Apache Zeppelin with SnappyData](#howto-zeppelin)


<a id="howto-startcluster"></a>
## How to Start a SnappyData Cluster
### Start SnappyData Cluster on a Single Machine

If you have [downloaded and extracted](install.md#install-on-premise) the SnappyData product distribution, navigate to the SnappyData product root directory.

**Start the Cluster**: Run the `sbin/snappy-start-all.sh` script to start SnappyData cluster on your single machine using default settings. This starts one lead node, one locator, and one data server.

```bash
$ sbin/snappy-start-all.sh
```

It may take 30 seconds or more to bootstrap the entire cluster on your local machine.

**Sample Output**: The sample output for `snappy-start-all.sh` is displayed as:

```
Starting SnappyData Locator using peer discovery on: localhost[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/user/snappyData/work/localhost-locator-1/snappylocator.log
SnappyData Locator pid: 9368 status: running
Starting SnappyData Server using locators for peer discovery: user1-laptop[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/user1/snappyData/work/localhost-server-1/snappyserver.log
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
Starting SnappyData Leader using locators for peer discovery: user1-laptop[10334]
Logs generated in /home/user1/snappyData/work/localhost-lead-1/snappyleader.log
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```
### Start SnappyData Cluster on Multiple Hosts

To start the cluster on multiple hosts:

1. The easiest way to run SnappyData on multiple nodes is to use a shared file system such as NFS on all the nodes.</br> You can also extract the product distribution on each node of the cluster. If all nodes have NFS access, install SnappyData on any one of the nodes.

2. Create the configuration files using the templates provided in the **conf** folder. Copy the existing template files (**servers.template**, **locators.template** and **leads.template**) and rename them to **servers**, **locators**, **leads**.
</br> Edit the files to include the hostnames on which to start the server, locator, and lead. Refer to the [configuration](configuring_cluster/configuring_cluster.md) section for more information on properties.

3. Start the cluster using `sbin/snappy-start-all.sh`. SnappyData starts the cluster using SSH.

!!! Note: 
	It is recommended that you set up passwordless SSH on all hosts in the cluster. Refer to the documentation for more details on [installation](install.md#install-on-premise) and [cluster configuration](configuring_cluster/configuring_cluster.md).

<a id="howto-statuscluster"></a>
## How to Check the Status of a SnappyData Cluster
You can check the status of a running cluster using the following command:

```bash
$ sbin/snappy-status-all.sh
SnappyData Locator pid: 9368 status: running
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```

You can check the SnappyData UI by opening `http://<leadHostname>:5050` in your browser, where `<leadHostname>` is the host name of your lead node. Use [Snappy SQL shell](howto.md#howto-snappyShell) to connect to the cluster and perform various SQL operations.

<a id="howto-stopcluster"></a>
## How to Shut Down a SnappyData Cluster
You can shut down the cluster using the `sbin/snappy-stop-all.sh` command:

```
$ sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```
!!! Note:
	Ensure that all write operations on column table have finished execution when you shut down a cluster, else, it can lead to the possible occurrence of a partial write.
    
    
<a id="howto-job"></a>
## How to Run Spark Code inside the Cluster
A Spark program that runs inside a SnappyData cluster is implemented as a SnappyData job.

**Implementing a Job**: 
A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. In the `runSnappyJob` method of the job, you implement the logic for your Spark program using SnappySession object instance passed to it. You can perform all operations such as create a table, load data, execute queries using the SnappySession. <br/>
Any of the Spark APIs can be invoked by a SnappyJob.

```
class CreatePartitionedRowTable extends SnappySQLJob {
  /** SnappyData uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappySession, jobConfig: Config): Any

  /**
  SnappyData calls this function to validate the job input and reject invalid job requests.
  You can implement custom validations here, for example, validating the configuration parameters
  **/
  def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
}
```

**Dependencies**:
To compile your job, use the Maven/SBT dependencies for the latest released version of SnappyData.

**Example: Maven dependency**:

```
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11 -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-cluster_2.11</artifactId>
    <version>0.9</version>
</dependency>
```

**Example: SBT dependency**:

```
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "0.9"
```

**Running the Job**: 
Once you create a jar file for SnappyData job, use `bin/snappy-job.sh` to submit the job to SnappyData cluster and run the job. This is similar to Spark-submit for any Spark application. 

For example, to run the job implemented in [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala) you can use the following command.
Here **quickstart.jar** contains the program and is bundled in the product distribution.<br/>
For example, the command submits the job and runs it as:

```
 # first cd to your SnappyData product dir
 $ cd $SNAPPY_HOME
 $ bin/snappy-job.sh submit
    --app-name CreatePartitionedRowTable
    --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
    --app-jar examples/jars/quickstart.jar
    --lead hostNameOfLead:8090
```
In the above comand, **--lead** option specifies the host name of the lead node along with the port on which it accepts jobs (default 8090).

**Output**: It returns output similar to:

```
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```

**Check Status**: You can check the status of the job using the Job ID listed above:

```
bin/snappy-job.sh status --lead hostNameOfLead:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48
```

Refer to the [Building SnappyData applications using Spark API](programming_guide.md#building-snappydata-applications-using-spark-api) section of the documentation for more details.


<a id="howto-splitmode"></a>
## How to Access SnappyData store from an Existing Spark Installation using Smart Connector

SnappyData comes with a Smart Connector that enables Spark applications to work with the SnappyData cluster, from any compatible Spark cluster (you can use any distribution that is compatible with Apache Spark 2.0.x). The Spark cluster executes in its own independent JVM processes and connects to SnappyData as a Spark data source. This is no different than how Spark applications today work with stores like Cassandra, Redis, etc.

For more information on the various modes, refer to the [SnappyData Smart Connector](deployment.md#snappydata-smart-connector-mode) section of the documentation.

**Code Example:**
The code example for this mode is in [SmartConnectorExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SmartConnectorExample.scala)

**Configure a SnappySession**: 

The code below shows how to initialize a SparkSession. Here the property `snappydata.connection` instructs the connector to acquire cluster connectivity and catalog metadata and registers it locally in the Spark cluster. Its value is consists of  locator host and JDBC client port on which the locator listens for connections (default 1527).

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("SmartConnectorExample")
        // It can be any master URL
        .master("local[4]")
         // snappydata.connection property enables the application to interact with SnappyData store
        .config("snappydata.connection", "localhost:1527")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)

```

**Create Table and Run Queries**: 
You can now create tables and run queries in SnappyData store using your Apache Spark program

```
    // reading an already created SnappyStore table SNAPPY_COL_TABLE
    val colTable = snSession.table("SNAPPY_COL_TABLE")
    colTable.show(10)

    snSession.dropTable("TestColumnTable", ifExists = true)

    // Creating a table from a DataFrame
    val dataFrame = snSession.range(1000).selectExpr("id", "floor(rand() * 10000) as k")

    snSession.sql("create table TestColumnTable (id bigint not null, k bigint not null) using column")

    // insert data in TestColumnTable
    dataFrame.write.insertInto("TestColumnTable")
```


<a id="howto-snappyShell"></a>
## How to Use Snappy SQL shell (snappy-sql)
`snappy-sql` can be used to execute SQL on SnappyData cluster. In the background, `snappy-sql` uses JDBC connections to execute SQL.

**Connect to a SnappyData Cluster**: 
Use the `snappy-sql` and `connect client` command on the Snappy SQL Shell

```
$ bin/snappy-sql
snappy> connect client '<locatorHostName>:1527';
```

Where `<locatorHostName>` is the host name of the node on which the locator is started and **1527** is the default port on which the locator listens for connections. 

**Execute SQL queries**: Once connected you can execute SQL queries using `snappy-sql`

```
snappy> CREATE TABLE APP.PARTSUPP (PS_PARTKEY INTEGER NOT NULL PRIMARY KEY, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL) USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY') ;

snappy> INSERT INTO APP.PARTSUPP VALUES(100, 1, 5000, 100);

snappy> INSERT INTO APP.PARTSUPP VALUES(200, 2, 50, 10);

snappy> SELECT * FROM APP.PARTSUPP;
PS_PARTKEY |PS_SUPPKEY |PS_AVAILQTY|PS_SUPPLYCOST    
-----------------------------------------------------
100        |1          |5000       |100.00           
200        |2          |50         |10.00            

2 rows selected

```

**View the members of cluster**: 
Use the `show members` command
```
snappy> show members;
ID                            |HOST                          |KIND                          |STATUS              |NETSERVERS                    |SERVERGROUPS                  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
192.168.63.1(21412)<v1>:61964 |192.168.63.1                  |datastore(normal)             |RUNNING             |localhost/127.0.0.1[1528]     |                              
192.168.63.1(21594)<v2>:29474 |192.168.63.1                  |accessor(normal)              |RUNNING             |                              |IMPLICIT_LEADER_SERVERGROUP   
localhost(21262)<v0>:22770    |localhost                     |locator(normal)               |RUNNING             |localhost/127.0.0.1[1527]     |                              

3 rows selected

```

**View the list tables in a schema**: 
Use `show tables in <schema>` command
```
snappy> show tables in app;
TABLE_SCHEM         |TABLE_NAME                    |TABLE_TYPE|REMARKS             
-----------------------------------------------------------------------------------
APP                 |PARTSUPP                      |TABLE     |                    

1 row selected
```

<a id="howto-row"></a>
## How to Create Row Tables and Run Queries

Each record in a Row table is managed in contiguous memory, and therefore, optimized for selective queries (For example. key based point lookup ) or updates. 
A row table can either be replicated to all nodes or partitioned across nodes. It can be created by using DataFrame API or using SQL.

Refer to the [Row and column tables](programming_guide.md#ddl) documentation for complete list of attributes for row tables.

Full source code, for example, to create and perform operations on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


### Create a Row Table using DataFrame API:

The code snippet below shows how to create a replicated row table using API.

**Get a SnappySession**

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**Create the Table using API**:
First, define the table schema and then create the table using createTable API

```
    val schema = StructType(Array(StructField("S_SUPPKEY", IntegerType, false),
      StructField("S_NAME", StringType, false),
      StructField("S_ADDRESS", StringType, false),
      StructField("S_NATIONKEY", IntegerType, false),
      StructField("S_PHONE", StringType, false),
      StructField("S_ACCTBAL", DecimalType(15, 2), false),
      StructField("S_COMMENT", StringType, false)
    ))

    // props1 map specifies the properties for the table to be created
    // "PERSISTENCE" flag indicates that the table data should be persisted to
    // disk asynchronously
    val props1 = Map("PERSISTENCE" -> "asynchronous")
    // create a row table using createTable API
    snSession.createTable("SUPPLIER", "row", schema, props1)
```

**Creating a Row table using SQL**:
The same table can be created using SQL as shown below:
```
    // First drop the table if it exists
    snSession.sql("DROP TABLE IF EXISTS SUPPLIER")
    // Create a row table using SQL
    // "PERSISTENCE" that the table data should be persisted to disk asynchronously
    // For complete list of attributes refer the documentation
    snSession.sql(
      "CREATE TABLE SUPPLIER ( " +
          "S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, " +
          "S_NAME STRING NOT NULL, " +
          "S_ADDRESS STRING NOT NULL, " +
          "S_NATIONKEY INTEGER NOT NULL, " +
          "S_PHONE STRING NOT NULL, " +
          "S_ACCTBAL DECIMAL(15, 2) NOT NULL, " +
          "S_COMMENT STRING NOT NULL " +
          ") USING ROW OPTIONS (PERSISTENCE 'asynchronous')")
```

You can perform various operations such as inset data, mutate it (update/delete), select data from the table. All these operations can be done either through APIs or by using SQL queries.
For example:

**To insert data in the SUPPLIER table:** 

```	
	snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")
```
**To print the contents of the SUPPLIER table:** 

```    
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
**To update the table account balance for SUPPLIER4:** 

```    
    snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")
```
**To print contents of the SUPPLIER table after update** 

```
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
**To delete the records for SUPPLIER2 and SUPPLIER3** 
```
    snSession.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")
```
**To print the contents of the SUPPLIER table after delete**

```    
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```

<a id="howto-column"></a>
## How to Create Column Tables and Run Queries

Column tables organize and manage data in a columnar form such that modern day CPUs can traverse and run computations like a sum or an average fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](programming_guide.md#tables-in-snappydata) documentation for the complete list of attributes for column tables.

Full source code, for example, to create and perform operations on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

### Create a Column Table using DataFrame API

The code snippet below shows how to create a column table using DataFrame API.

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```
**Define the table schema**

```
val tableSchema = StructType(Array(StructField("C_CUSTKEY", IntegerType, false),
      StructField("C_NAME", StringType, false),
      StructField("C_ADDRESS", StringType, false),
      StructField("C_NATIONKEY", IntegerType, false),
      StructField("C_PHONE", StringType, false),
      StructField("C_ACCTBAL", DecimalType(15, 2), false),
      StructField("C_MKTSEGMENT", StringType, false),
      StructField("C_COMMENT", StringType, false)
    ))
```

**Create the table and load data from CSV**

```
    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    snSession.createTable("CUSTOMER", "column", tableSchema, props1)

     val tableSchema = snSession.table("CUSTOMER").schema
    // insert some data in it
    // loading data in CUSTOMER table from a text file with delimited columns
    val customerDF = snSession.read.schema(schema = tableSchema).csv("quickstart/src/main/resources/customer.csv")
    customerDF.write.insertInto("CUSTOMER")
```

### Create a Column Table using SQL

The same table can be created using SQL as shown below:
```
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

You can execute selected queries on a column table, join the column table with other tables, and append data to it.

<a id="howto-load"></a>
## How to Load Data into SnappyData Tables

SnappyData relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. By integrating the loading mechanism with the Query engine (Catalyst optimizer) it is often possible to push down filters and projections all the way to the data source minimizing data transfer. Here is the list of important features:

**Support for many Sources** There is built-in support for many data sources as well as data formats. Data can be accessed from S3, file system, HDFS, Hive, RDB, etc. And the loaders have built-in support to handle CSV, Parquet, ORC, Avro, JSON, Java/Scala Objects, etc as the data formats. 

**Access virtually any modern data store** Virtually all major data providers have a native Spark connector that complies with the Data Sources API. For e.g. you can load data from any RDB like Amazon Redshift, Cassandra, Redis, Elastic Search, Neo4J, etc. While these connectors are not built-in, you can easily deploy these connectors as dependencies into a SnappyData cluster. All the connectors are typically registered in spark-packages.org

**Avoid Schema wrangling** Spark supports schema inference. Which means, all you need to do is point to the external source in your 'create table' DDL (or Spark SQL API) and schema definition is learned by reading in the data. There is no need to explicitly define each column and type. This is extremely useful when dealing with disparate, complex and wide data sets. 

**Read nested, sparse data sets** When data is accessed from a source, the schema inference occurs by not just reading a header but often by reading the entire data set. For instance, when reading JSON files the structure could change from document to document. The inference engine builds up the schema as it reads each record and keeps unioning them to create a unified schema. This approach allows developers to become very productive with disparate data sets.

**Load using Spark API or SQL** You can use SQL to point to any data source or use the native Spark Scala/Java API to load. 
For instance, you can use 'create external table <tablename> using <any Data Source supported> options <options>' and then use it in any SQL query or DDL (e.g. create table snappyTable using column as (select * from externalTable) )


**Example - Load from CSV**

You can either explicitly define the schema or infer the schema and the column data types. To infer the column names, we need the CSV header to specify the names. In this example we don't have the names, so we explicitly define the schema. 


```
    // Get a SnappySession in a local cluster
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

We explicitly define the table definition first ....

```
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

**Load data in the CUSTOMER table from a CSV file by using Data Sources API**

```
    val tableSchema = snSession.table("CUSTOMER").schema
    val customerDF = snSession.read.schema(schema = tableSchema).csv(s"$dataFolder/customer.csv")
    customerDF.write.insertInto("CUSTOMER")
```

The Spark SQL programming guide provides a full description of the Data Sources API - https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#data-sources

**Example - Load from Parquet files**

```
val customerDF = snSession.read.parquet(s"$dataDir/customer_parquet")
customerDF.write.insertInto("CUSTOMER")
```

**Inferring schema from data file**

A schema for the table can be inferred from the data file. Data is first introspected to learn the schema (column names and types) without requring this input from the user. The example below illustrates reading a parquet data source and creates a new columnar table in SnappyData. The schema is automatically defined when the Parquet data files are read. 
```
    val customerDF = snSession.read.parquet(s"quickstart/src/main/resources/customerparquet")
    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    customerDF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")
```

In the code snippet below a schema is inferred from a CSV file. Column names are derived from the header in the file.

```
    val customer_csv_DF = snSession.read.option("header", "true")
        .option("inferSchema", "true").csv("quickstart/src/main/resources/customer_with_headers.csv")

    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
    // For complete list of attributes refer the documentation
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    customer_csv_DF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")
```

The source code to load the data from a CSV/Parquet files is in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala). 

** Example - reading JSON documents**
As mentioned before when dealing with JSON you have two challenges - (1) the data can be highly nested (2) the structure of the documents can keep changing. 

Here is a simple example that loads multiple JSON records that show dealing with schema changes across documents -   [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)

!!! Note
When loading data from sources like CSV or Parquet the files would need to be accessible from all the cluster members in SnappyData. Make sure it is NFS mounted or made accessible through the Cloud solution (shared storage like S3). 


<a id="howto-external-source"></a>
## How to Load Data from External Data Stores (e.g. HDFS, Cassandra, Hive, etc) 

SnappyData comes bundled with the libraries to access HDFS (Apache compatible). You can load your data using SQL or DataFrame API.

### Example - Loading data from CSV file using SQL

```scala
-- Create an external table based on CSV file
CREATE EXTERNAL TABLE CUSTOMER_STAGING_1 USING csv OPTIONS (path '../../quickstart/src/main/resources/customer_with_headers.csv', header 'true', inferSchema 'true');

-- create a snappydata table and load data into CUSTOMER table
CREATE TABLE CUSTOMER using column options() as (select * from CUSTOMER_STAGING_1);

```

!!!Tip:
	Similarly, you can create an external table for all data sources and use SQL "insert into" query to load data. For more information on creating external tables refer to, [CREATE EXTERNAL TABLE](reference/sql_reference/create-external-table/)


### Example - Loading CSV Files from HDFS using API

The example below demonstrates how you can read CSV files from HDFS using an API:
```
val dataDF=snc.read.option("header","true").csv ("../../quickstart/src/main/resources/customer_with_headers.csv'")

// drop table if exist
snc.sql("drop table if exists CUSTOMER")

// Load data into table
dataDF.write.saveAsTable("CUSTOMER")
```

### Example - Loading and Enriching CSV Data from HDFS 

The example below demonstrates how you can load and enrich CSV Data from HDFS:
```
val dataDF=snc.read.option("header","true").csv ("../../quickstart/src/main/resources/customer_with_headers.csv'")

// drop table if exist and create it with only required fields 
snc.sql("drop table if exists CUSTOMER")
snc.sql("create table CUSTOMER(C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE VARCHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT VARCHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL) using column options()")

import snc.implicits._
// Project and transform data from df and load it in table.
dataDF.select($"INCIDNTNUM",$"DAYOFWEEK".substr(1,3).alias("DAYOFWEEK"),$"X",$"Y").write.mode(SaveMode.Overwrite).saveAsTable("CUSTOMER")

//Here X and Y are latitude and longitude columns in raw data frame
```

### Example - Loading from Hive
As SnappyData manages the catalog at all times and it is not possible to configure an external Hive catalog service like in Spark when using a SnappySession. But, it is still possible to access Hive using the native SparkSession (with **enableHiveSupport** set to **true**). 
Here is an example using the SparkSession(spark object below) to access a Hive table as a DataFrame, then converted to an RDD so it can be passed to a SnappySession to store it in a SnappyData Table. 

```
val ds = spark.table("hiveTable")
val rdd = ds.rdd
val session = new SnappySession(sparkContext)
val df = session.createDataFrame(rdd, ds.schema)
df.write.format("column").saveAsTable("columnTable")
```

### Importing Data using JDBC from a relational DB

!!! Note:
	Before you begin, you must install the corresponding JDBC driver. To do so, copy the JDBC driver jar file in **/jars** directory located in the home directory and then restart the cluster.

<!--**TODO: This is a problem- restart the cluster ? Must confirm package installation or at least get install_jar tested for this case. -- Jags**
-->

The example below demonstrates how to connect to any SQL database using JDBC:


1. Verify and load the SQL Driver:

	    Class.forName("com.mysql.jdbc.Driver")
    
2. Specify all the properties to access the database

        import java.util.Properties
        val jdbcUsername = "USER_NAME"
        val jdbcPassword = "PASSWORD"
        val jdbcHostname = "HOSTNAME"
        val jdbcPort = 3306
        val jdbcDatabase ="DATABASE"
        val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}&relaxAutoCommit=true"

        val connectionProperties = new Properties()
        connectionProperties.put("user", "USERNAME")
        connectionProperties.put("password", "PASSWORD")

3. Fetch the table meta data from the RDB and creates equivalent column tables 

        val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
        connection.isClosed()
        val md:DatabaseMetaData = connection.getMetaData();
        val rs:ResultSet = md.getTables(null, null, "%", null);
        while (rs.next()) {

        val tableName=rs.getString(3)
        val df=snc.read.jdbc(jdbcUrl, tableName, connectionProperties)
        df.printSchema
        df.show()
        // Create and load a column table with same schema as that of source table 
           df.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
        }

** Using SQL to access external RDB tables **
You can also use plain SQL to access any external RDB using external tables. Create external table on RDBMS table and query it directly from SnappyData as described below:

        snc.sql("drop table if exists external_table")
        snc.sql(s"CREATE  external TABLE external_table USING jdbc OPTIONS (dbtable 'tweet', driver 'com.mysql.jdbc.Driver',  user 'root',  password 'root',  url '$jdbcUrl')")
        snc.sql("select * from external_table").show

Refer to the [Spark SQL JDBC source access for how to parallelize access when dealing with large data sets](https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#jdbc-to-other-databases).


### Loading Data from NoSQL store (Cassandra)

The example below demonstrates how you can load data from a NoSQL store:

!!!Note:
	Before you begin, you must install the corresponding Spark-Cassandra connector jar. To do so, copy the Spark-Cassandra connector jar file to the **/jars** directory located in the home directory and then restart the cluster.

<!--**TODO** This isn't a single JAR from what I know. The above step needs testing and clarity. -- Jags
-->

```

val df = snc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "CUSTOMER", "keyspace" -> "test")) .load
df.write.format("column").mode(SaveMode.Append).saveAsTable("CUSTOMER")
snc.sql("select * from CUSTOMER").show
```

<a id="howto-collacatedJoin"></a>
## How to Perform a Colocated Join

When two tables are partitioned on columns and colocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData server. Colocating the data of two tables based on a partitioning column's value is a best practice if you frequently perform queries on those tables that join on that column.
When colocated tables are joined on the partitioning columns, the join happens locally on the node where data is present, without the need of shuffling the data.

**Code Example: ORDERS table is colocated with CUSTOMER table**

A partitioned table can be colocated with another partitioned table by using the "COLOCATE_WITH" attribute in the table options. <br/>
For example, in the code snippet below, the ORDERS table is colocated with the CUSTOMER table. The complete source for this example can be found in the file [CollocatedJoinExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CollocatedJoinExample.scala)

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("ColocatedJoinExample")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**Create Table Customer:**

```
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
**Create Table Orders:**

```
    snSession.sql("CREATE TABLE ORDERS  ( " +
        "O_ORDERKEY       INTEGER NOT NULL," +
        "O_CUSTKEY        INTEGER NOT NULL," +
        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
        "O_ORDERDATE      DATE NOT NULL," +
        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
        "O_CLERK          CHAR(15) NOT NULL," +
        "O_SHIPPRIORITY   INTEGER NOT NULL," +
        "O_COMMENT        VARCHAR(79) NOT NULL) " +
        "USING COLUMN OPTIONS (PARTITION_BY 'O_CUSTKEY', " +
        "COLOCATE_WITH 'CUSTOMER' )")
```

**Perform a Colocate join:** 

```
    // Selecting orders for all customers
    val result = snSession.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```

<a id="howto-jdbc"></a>
## How to Connect using JDBC Driver

You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. The locator passes the information of all available servers based on which, the driver automatically connects to one of the servers.

**To connect to the SnappyData cluster**: Using JDBC, use URL of the form `jdbc:snappydata://<locatorHostName>:<locatorClientPort>/`

Where the `<locatorHostName>` is the hostname of the node on which the locator is started and `<locatorClientPort>` is the port on which the locator accepts client connections (default 1527).

**Code Example:**

**Connect to a SnappyData cluster using JDBC on default client port**

The code snippet shows how to connect to a SnappyData cluster using JDBC on default client port 1527. The complete source code of the example is located at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)
```
val url: String = s"jdbc:snappydata://localhost:1527/"
val conn1 = DriverManager.getConnection(url)

val stmt1 = conn1.createStatement()
// Creating a table (PARTSUPP) using JDBC connection
stmt1.execute("DROP TABLE IF EXISTS APP.PARTSUPP")
stmt1.execute("CREATE TABLE APP.PARTSUPP ( " +
     "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
     "PS_SUPPKEY     INTEGER NOT NULL," +
     "PS_AVAILQTY    INTEGER NOT NULL," +
     "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
    "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY')")

// Inserting records in PARTSUPP table via batch inserts
val preparedStmt1 = conn1.prepareStatement("INSERT INTO APP.PARTSUPP VALUES(?, ?, ?, ?)")

var x = 0
for (x <- 1 to 10) {
  preparedStmt1.setInt(1, x*100)
  preparedStmt1.setInt(2, x)
  preparedStmt1.setInt(3, x*1000)
  preparedStmt1.setBigDecimal(4, java.math.BigDecimal.valueOf(100.2))
  preparedStmt1.addBatch()
}
preparedStmt1.executeBatch()
preparedStmt1.close()
```

!!! Note: 
	If the tool does not automatically select a driver class, you may have the option of selecting a class from within the JAR file. In this case, select the **io.snappydata.jdbc.ClientDriver** class.

<a id="howto-JSON"></a>
## How to Store and Query JSON Objects

You can insert JSON data in SnappyData tables and execute queries on the tables.

**Code Example: Loads JSON data from a JSON file into a column table and executes query**

The code snippet loads JSON data from a JSON file into a column table and executes the query against it.
The source code for JSON example is located at [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala). After creating SnappySession, the JSON file is read using Spark API and loaded into a SnappyData table.

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("WorkingWithJson")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**Create a DataFrame from the JSON file**:

```
    val some_people_path = s"quickstart/src/main/resources/some_people.json"
    // Read a JSON file using Spark API
    val people = snSession.read.json(some_people_path)
    people.printSchema()
```

**Create a SnappyData table and insert the JSON data in it using the DataFrame**:

```
    //Drop the table if it exists
    snSession.dropTable("people", ifExists = true)

   //Create a columnar table with the Json DataFrame schema
    snSession.createTable(tableName = "people",
      provider = "column",
      schema = people.schema,
      options = Map.empty[String,String],
      allowExisting = false)

    // Write the created DataFrame to the columnar table
    people.write.insertInto("people")
```

**Append more data from a second JSON file**:

```
    // Append more people to the column table
    val more_people_path = s"quickstart/src/main/resources/more_people.json"

    //Explicitly passing schema to handle record level field mismatch
    // e.g. some records have "district" field while some do not.
    val morePeople = snSession.read.schema(people.schema).json(more_people_path)
    morePeople.write.insertInto("people")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snSession.table("people").schema)
```

**Execute queries and return the results**
```
    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT " +
        "name, " +
        "address.city, " +
        "address.state, " +
        "address.district, " +
        "address.lane " +
        "FROM people")

nameAndAddress.toJSON.show()

```

<a id="howto-objects"></a>
## How to Store and Query Objects

You can use domain object to load data into SnappyData tables and select the data by executing queries against the table.

**Code Example: Insert Person object into the column table**

The code snippet below inserts Person objects into a column table. The source code for this example is located at [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala). After creating SnappySession, the Person objects are inserted using Spark API and loads into a SnappyData table.

**Get a SnappySession**:

```scala
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**Create DataFrame objects**:
```scala
    //Import the implicits for automatic conversion between Objects to DataSets.
    import snSession.implicits._

    // Create a Dataset using Spark APIs
    val people = Seq(Person("Tom", Address("Columbus", "Ohio"), Map("frnd1"-> "8998797979", "frnd2" -> "09878786886"))
      , Person("Ned", Address("San Diego", "California"), Map.empty[String,String])).toDS()
```

**Create a SnappyData table and insert data into it**:

```scala
    //Drop the table if it exists.
    snSession.dropTable("Persons", ifExists = true)

    //Create a columnar table with a Struct to store Address
    snSession.sql("CREATE table Persons(name String, address Struct<city: String, state:String>, " +
         "emergencyContacts Map<String,String>) using column options()")

    // Write the created DataFrame to the columnar table.
    people.write.insertInto("Persons")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snSession.table("Persons").schema)
    

    // Append more people to the column table
    val morePeople = Seq(Person("Jon Snow", Address("Columbus", "Ohio"), Map.empty[String,String]),
      Person("Rob Stark", Address("San Diego", "California"), Map.empty[String,String]),
      Person("Michael", Address("Null", "California"), Map.empty[String,String])).toDS()

    morePeople.write.insertInto("Persons")
```

**Execute query on the table and return results**:

```scala
    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT name, address, emergencyContacts FROM Persons")

    //Reconstruct the objects from obtained Row
    val allPersons = nameAndAddress.as[Person]
    //allPersons is a Spark Dataset of Person objects. 
    // Use of the Dataset APIs to transform, query this data set. 
```

<a id="howto-streams"></a>
## How to Use Stream Processing with SnappyData
SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to the [documentation](programming_guide.md#stream-processing-using-sql).

**Code Example**: 
Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets below show how to declare a stream table, register continuous queries(CQ) and update SnappyData table using the stream data.

**First get a SnappySession and a SnappyStreamingContext**: 
Here SnappyStreamingContext is initialized in a batch duration of one second.
```
    val spark: SparkSession = SparkSession
        .builder
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate

    val snsc = new SnappyStreamingContext(spark.sparkContext, Seconds(1))
```

The example starts an embedded Kafka instance on which a few messages are published. SnappyData processes these message and updates a table based on the stream data.

The SQL below shows how to declare a stream table using SQL. The rowConverter attribute specifies a class used to return Row objects from the received stream messages.
```
    snsc.sql(
      "create stream table adImpressionStream (" +
          " time_stamp timestamp," +
          " publisher string," +
          " advertiser string," +
          " website string," +
          " geo string," +
          " bid double," +
          " cookie string) " + " using directkafka_stream options(" +
          " rowConverter 'org.apache.spark.examples.snappydata.RowsConverter'," +
          s" kafkaParams 'metadata.broker.list->$address;auto.offset.reset->smallest'," +
          s" topics 'kafka_topic')"
    )
```

RowsConverter decodes a stream message consisting of comma separated fields and forms a Row object from it.

```
class RowsConverter extends StreamToRowsConverter with Serializable {
  override def toRows(message: Any): Seq[Row] = {
    val log = message.asInstanceOf[String]
    val fields = log.split(",")
    val rows = Seq(Row.fromSeq(Seq(new java.sql.Timestamp(fields(0).toLong),
      fields(1),
      fields(2),
      fields(3),
      fields(4),
      fields(5).toDouble,
      fields(6)
    )))
    rows
  }
}
```

**To create a row table that is updated based on the streaming data**:

```
snsc.sql("create table publisher_bid_counts(publisher string, bidCount int) using row")
```

**To declare a continuous query that is executed on the streaming data**: This query returns a number of bids per publisher in one batch.

```
    val resultStream: SchemaDStream = snsc.registerCQ("select publisher, count(bid) as bidCount from " +
        "adImpressionStream window (duration 1 seconds, slide 1 seconds) group by publisher")
```

**To process that the result of above continuous query to update the row table publisher_bid_counts**:

```
    // this conf is used to get a JDBC connection
    val conf = new ConnectionConfBuilder(snsc.snappySession).build()

    resultStream.foreachDataFrame(df => {
        println("Data received in streaming window")
        df.show()

        println("Updating table publisher_bid_counts")
        val conn = ConnectionUtil.getConnection(conf)
        val result = df.collect()
        val stmt = conn.prepareStatement("update publisher_bid_counts set " +
            s"bidCount = bidCount + ? where publisher = ?")

        result.foreach(row => {
          val publisher = row.getString(0)
          val bidCount = row.getLong(1)
          stmt.setLong(1, bidCount)
          stmt.setString(2, publisher)
          stmt.addBatch()
        }
        )
        stmt.executeBatch()
        conn.close()
      }
    })

```

**To display the total bids by each publisher by querying publisher_bid_counts table**:

```
snsc.snappySession.sql("select publisher, bidCount from publisher_bid_counts").show()

```

<a id="howto-sde"></a>
## How to Use Synopsis Data Engine to Run Approximate Queries

Synopsis Data Engine (SDE) uses statistical sampling techniques and probabilistic data structures to answer analytic queries with sub-second latency. There is no need to store or process the entire Dataset. The approach trades off query accuracy for fast response time.
For more information on  SDE, refer to [SDE documentation](aqp.md).

**Code Example**:
The complete code example for SDE is in [SynopsisDataExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SynopsisDataExample.scala). The code below creates a sample table and executes queries that run on the sample table.

**Get a SnappySession**:
```
    val spark: SparkSession = SparkSession
        .builder
        .appName("SynopsisDataExample")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**The base column table(AIRLINE) is created from temporary parquet table as follows**:

```
    // Create temporary staging table to load parquet data
    snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINE " +
        "USING parquet OPTIONS(path " + s"'${dataFolder}/airlineParquetData')")

    // Create a column table AIRLINE
    snSession.sql("CREATE TABLE AIRLINE USING column AS (SELECT Year AS Year_, " +
        "Month AS Month_ , DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, " +
        "CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
        "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, " +
        "TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, " +
        "WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay, " +
        "ArrDelaySlot FROM STAGING_AIRLINE)")
```

**Create a sample table for the above base table**:
Attribute 'qcs' in the statement below specifies the columns used for stratification and attribute 'fraction' specifies how big the sample needs to be (3% of the base table AIRLINE in this case). For more information on Synopsis Data Engine, refer to the [SDE documentation](aqp.md#working-with-stratified-samples).


```

    snSession.sql("CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE OPTIONS" +
        "(qcs 'UniqueCarrier, Year_, Month_', fraction '0.03')  " +
        "AS (SELECT Year_, Month_ , DayOfMonth, " +
        "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, " +
        "FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, " +
        "ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, " +
        "Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, " +
        "NASDelay, SecurityDelay, LateAircraftDelay, ArrDelaySlot FROM AIRLINE)")
```

**Execute queries that return approximate results using sample tables**:
The query below returns airlines by number of flights in descending order. The 'with error 0.20' clause in the query below signals query engine to execute the query on the sample table instead of the base table and maximum 20% error is allowed.

```
    var result = snSession.sql("select  count(*) flightRecCount, description AirlineName, " +
        "UniqueCarrier carrierCode ,Year_ from airline , airlineref where " +
        "airline.UniqueCarrier = airlineref.code group by " +
        "UniqueCarrier,description, Year_ order by flightRecCount desc limit " +
        "10 with error 0.20").collect()
    result.foreach(r => println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))
```

**Join the sample table with a reference table**:
You can join the sample table with a reference table to execute queries. For example a reference table (AIRLINEREF) is created as follows from a parquet data file.
```
    // create temporary staging table to load parquet data
    snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINEREF USING " +
        "parquet OPTIONS(path " + s"'${dataFolder}/airportcodeParquetData')")
    snSession.sql("CREATE TABLE AIRLINEREF USING row AS (SELECT CODE, " +
        "DESCRIPTION FROM STAGING_AIRLINEREF)")
```
**Join the sample table and reference table to find out which airlines arrive on schedule**:

```
    result = snSession.sql("select AVG(ArrDelay) arrivalDelay, " +
        "relative_error(arrivalDelay) rel_err, description AirlineName, " +
        "UniqueCarrier carrier from airline, airlineref " +
        "where airline.UniqueCarrier = airlineref.Code " +
        "group by UniqueCarrier, description order by arrivalDelay " +
        "with error").collect()

   result.foreach(r => println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))
```



<a id="howto-python"></a>
## How to Use Python to Create Tables and Run Queries

Developers can write programs in Python to use SnappyData features.

**First create a SnappySession**:

```
 from pyspark.sql.snappy import SnappySession
 from pyspark import SparkContext, SparkConf

 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 snappy = SnappySession(sc)
```

**Create table using SnappySession**:

```
    # Creating partitioned table PARTSUPP using SQL
    snappy.sql("DROP TABLE IF EXISTS PARTSUPP")
    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY),
    # For complete list of table attributes refer the documentation
    # http://snappydatainc.github.io/snappydata/programming_guide
    snappy.sql("CREATE TABLE PARTSUPP ( " +
                  "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
                  "PS_SUPPKEY     INTEGER NOT NULL," +
                  "PS_AVAILQTY    INTEGER NOT NULL," +
                  "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
                  "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY' )")
```

**Inserting data in table using INSERT query**:

```
    snappy.sql("INSERT INTO PARTSUPP VALUES(100, 1, 5000, 100)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(200, 2, 50, 10)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(300, 3, 1000, 20)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(400, 4, 200, 30)")
    # Printing the contents of the PARTSUPP table
    snappy.sql("SELECT * FROM PARTSUPP").show()
```

**Update the data using SQL**:

```
    # Update the available quantity for PARTKEY 100
    snappy.sql("UPDATE PARTSUPP SET PS_AVAILQTY = 50000 WHERE PS_PARTKEY = 100")
    # Printing the contents of the PARTSUPP table after update
    snappy.sql("SELECT * FROM PARTSUPP").show()
```    

**Delete records from the table**:
```
    # Delete the records for PARTKEY 400
    snappy.sql("DELETE FROM PARTSUPP WHERE PS_PARTKEY = 400")
    # Printing the contents of the PARTSUPP table after delete
    snappy.sql("SELECT * FROM PARTSUPP").show()
```

**Create table using API**:
This same table can be created by using createTable API. First create a schema and then create the table, and then mutate the table data using API:

```
    # drop the table if it exists
    snappy.dropTable('PARTSUPP', True)

    schema = StructType([StructField('PS_PARTKEY', IntegerType(), False),
                     StructField('PS_SUPPKEY', IntegerType(), False),
                     StructField('PS_AVAILQTY', IntegerType(),False),
                     StructField('PS_SUPPLYCOST', DecimalType(15, 2), False)
                     ])

    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY)
    # For complete list of table attributes refer the documentation at
    # http://snappydatainc.github.io/snappydata/programming_guide
    snappy.createTable('PARTSUPP', 'row', schema, False, PARTITION_BY = 'PS_PARTKEY')

    # Inserting data in PARTSUPP table using DataFrame
    tuples = [(100, 1, 5000, Decimal(100)), (200, 2, 50, Decimal(10)),
              (300, 3, 1000, Decimal(20)), (400, 4, 200, Decimal(30))]
    rdd = sc.parallelize(tuples)
    tuplesDF = snappy.createDataFrame(rdd, schema)
    tuplesDF.write.insertInto("PARTSUPP")
    #Printing the contents of the PARTSUPP table
    snappy.sql("SELECT * FROM PARTSUPP").show()

    # Update the available quantity for PARTKEY 100
    snappy.update("PARTSUPP", "PS_PARTKEY =100", [50000], ["PS_AVAILQTY"])
    # Printing the contents of the PARTSUPP table after update
    snappy.sql("SELECT * FROM PARTSUPP").show()

    # Delete the records for PARTKEY 400
    snappy.delete("PARTSUPP", "PS_PARTKEY =400")
    # Printing the contents of the PARTSUPP table after delete
    snappy.sql("SELECT * FROM PARTSUPP").show()
```

The complete source code for the above example is in [CreateTable.py](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/python/CreateTable.py)

<a id="howto-odbc"></a>
## How to Connect using ODBC Driver

You can connect to SnappyData Cluster using SnappyData ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.
<a id="howto-odbc-step1"></a>
### Step 1: Install Visual C++ Redistributable for Visual Studio 2015 

To download and install the Visual C++ Redistributable for Visual Studio 2015:

1. [Download Visual C++ Redistributable for Visual Studio 2015](https://www.microsoft.com/en-in/download/details.aspx?id=48145)

2. Depending on your Windows installation, download the required version of the SnappyData ODBC Driver.

3. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
### Step 2: Install SnappyData ODBC Driver

To download and install the ODBC driver:

1. Download the SnappyData ODBC Driver from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases).

2. Depending on your Windows installation, download the 32-bit or 64-bit version of the SnappyData ODBC Driver.

	* [32-bit for 32-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc32.zip)

	* [32-bit for 64-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc32_64.zip) 

	* [64-bit for 64-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc64.zip) 

3. Extract the contents of the downloaded file.

4. Double-click on the **SnappyDataODBCDriverInstaller.msi** file, and follow the steps to complete the installation.

	!!! Note: 
		Ensure that [SnappyData version 0.8 or later is installed](http://snappydatainc.github.io/snappydata/install/) and the [SnappyData cluster is running](howto.md#howto-startCluster).

### Connect to the SnappyData cluster 
Once you have installed SnappyData ODBC Driver, you can connect to SnappyData cluster in any of the following ways:

* Use the SnappyData Driver Connection URL:

		Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>

* Create a SnappyData DSN (Data Source Name) using the installed SnappyData ODBC Driver.</br> 
 Please refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>When prompted, select the SnappyData ODBC Driver from the driver's list and enter a Data Source name, SnappyData Server Host, Port, User Name and Password. 

Refer to the documentation for detailed information on [Setting Up SnappyData ODBC Driver and Tableau Desktop](setting_up_odbc_driver-tableau_desktop.md).  

<a id="howto-external-client"></a>
## How to Connect to the Cluster from an External Network

You can also connect to the SnappyData cluster from a different network as a client (DbVisualizer, SQuirreL SQL etc.). </br>For example, to connect to a cluster on AWS from your local machine set the following properties in the *conf/locators* and *conf/servers* files:

* `client-bind-address`: Set the hostname or IP address to which the locator or server binds. 

* `hostname-for-clients`: Set the IP address or host name that this server/locator listens on, for client connections. Setting a specific `hostname-for-clients` will cause locators to use this value when telling clients how to connect to this server. The default value causes the `bind-address` to be given to clients.

	!!! Note: 
    	By default, the locator or server binds to localhost. You may need to set either or both these properties to enable connection from external clients. If not set, external client connections may fail.

* Port Settings: Locator or server listens on the default port 1527 for client connections. Ensure that this port is open in your firewall settings. <br> You can also change the default port by setting the `client-port` property in the *conf/locators* and *conf/servers*.

!!! Note: 
	For ODBC clients, you must use the host and port details of the server and not the locator.

<a id="howto-zeppelin"></a>
## How to Use Apache Zeppelin with SnappyData

### Step 1: Download, Install and Configure SnappyData
1. [Download and Install SnappyData](install.md#download-snappydata) </br>
 The table below lists the version of the SnappyData Zeppelin Interpreter and Apache Zeppelin Installer for the supported SnappyData Release.
	
    | SnappyData Zeppelin Interpreter | Apache Zeppelin Binary Package | SnappyData Release|
	|--------|--------|--------|
	|[Version 0.6.1](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/tag/v0.6.1)|[Version 0.6](https://zeppelin.apache.org/download.html) |[Release 0.7](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.7) </br> [Release 0.8](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.8) and [future releases](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.9)|
    |[Version 0.7.1](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/tag/v0.7.1) |[Version 0.7](https://zeppelin.apache.org/download.html) |[Release 0.8](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.8) [and future releases](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.9)|

2. [Configure the SnappyData Cluster](configuring_cluster/configuring_cluster.md).

3. In [lead node configuration](configuring_cluster/configuring_cluster.md#configuring-leads) set the following properties:

	- Enable the SnappyData Zeppelin interpreter by adding `-zeppelin.interpreter.enable=true` 

    - In the classpath option, define the location where the SnappyData Interpreter is downloaded by adding `-classpath=/<download_location>/snappydata-zeppelin-<version_number>.jar`.

4. [Start the SnappyData cluster](howto.md#how-to-start-a-snappydata-cluster)

5. Extract the contents of the Zeppelin binary package. </br> 

6. Install the SnappyData Zeppelin interpreter in Apache Zeppelin by executing the following command from Zeppelin's bin directory: </br>
	`./install-interpreter.sh --name snappydata --artifact io.snappydata:snappydata-zeppelin:<snappydata_interpreter_version_number>`. </br>
    Zeppelin interpreter allows the SnappyData interpreter to be plugged into Zeppelin using which, you can run queries.

7. Rename the **zeppelin-site.xml.template** file (located in zeppelin-<_version_number_>-bin-all/conf directory) to **zeppelin-site.xml**.

8. Edit the **zeppelin-site.xml** file, and in the `zeppelin.interpreters` property, add the following interpreter class names: `org.apache.zeppelin.interpreter.SnappyDataZeppelinInterpreter,org.apache.zeppelin.interpreter.SnappyDataSqlZeppelinInterpreter`.

9. Start the Zeppelin daemon using the command: </br> `bin/zeppelin-daemon.sh start`.

10. To ensure that the installation is successful, log into the Zeppelin UI (**http://localhost:8080**) from your web browser.

### Step 2: Configure SnappyData for Apache Zeppelin

1. Log on to Zeppelin from your web browser and select **Interpreter** from the **Settings** option.

2. Click **Create** ![Create](Images/create_interpreter.png) to add an interpreter.	 

3. From the **Interpreter group** drop-down select **snappydata**.
	 ![Configure Interpreter](Images/snappydata_interpreter_properties.png)

	!!! Note: 
    	If **snappydata** is not displayed in the **Interpreter group** drop-down list, try the following options, and then restart Zeppelin daemon: 

    	* Delete the **interpreter.json** file located in the **conf** directory (in the Zeppelin home directory).

    	* Delete the **zeppelin-spark_<_version_number_>.jar** file located in the **interpreter/snappydata** directory (in the Zeppelin home directory).


4. Click the **Connect to existing process** option. The fields **Host** and **Port** are displayed.

5. Specify the host on which the SnappyData lead node is executing, and the SnappyData Zeppelin Port (Default is 3768).
	
	| Property | Default Values | Description |
	|--------|--------| -------- |
	|Host|localhost        |Specify host on which the SnappyData lead node is executing  |
	|Port        |3768        |Specify the Zeppelin server port  |
	
6. Configure the interpreter properties. </br>The table lists the properties required for SnappyData.

	| Property | Value | Description |
	|--------|--------| -------- |
	|default.ur|jdbc:snappydata://localhost:1527/	| Specify the JDBC URL for SnappyData cluster in the format `jdbc:snappydata://<locator_hostname>:1527` |
	|default.driver|com.pivotal.gemfirexd.jdbc.ClientDriver| Specify the JDBC driver for SnappyData|
	|snappydata.connection|localhost:1527| Specify the `host:clientPort` combination of the locator for the JDBC connection |
	|master|local[*]| Specify the URI of the spark master (only local/split mode) |
	|zeppelin.jdbc.concurrent.use|true| Specify the Zeppelin scheduler to be used. </br>Select **True** for Fair and **False** for FIFO | 

7. If required, edit other properties, and then click **Save** to apply your changes.</br>

8. Bind the interpreter and set SnappyData as the default interpreter.</br> SnappyData Zeppelin Interpreter group consist of two interpreters. Click and drag *<_Interpreter_Name_>* to the top of the list to set it as the default interpreter. 
 	
	| Interpreter Name | Description |
	|--------|--------|
    | %snappydata.snappydata or </br> %snappydata.spark | This interpreter is used to write Scala code in the paragraph. SnappyContext is injected in this interpreter and can be accessed using variable **snc** |
    |%snappydata.sql | This interpreter is used to execute SQL queries on the SnappyData cluster. It also has features of executing approximate queries on the SnappyData cluster.|

9. Click **Save** to apply your changes.

!!! Note: 
	You can modify the default port number of the Zeppelin interpreter by setting the property:</br>
	`-zeppelin.interpreter.port=<port_number>` in [lead node configuration](configuring_cluster/configuring_cluster.md#configuring-leads). 

### Known Issue

If you are using SnappyData Zeppelin Interpreter 0.7.1 and Zeppelin Installer 0.7 with SnappyData 0.8 or future releases, the approximate result does not work on the sample table, when you execute a paragraph with the `%sql show-instant-results-first` directive.

### More Information
Refer to these sections for information:

* [About the Interpreter](aqp_aws.md#using-the-interpreter) 

* [Example Notebooks](aqp_aws.md#creating-notebooks-try-it-yourself)

