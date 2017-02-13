#EXTRA "HOW TO" TOPICS FOR GA REALEASE

* How to Import/Export data
* How to connect to SnappyData using third party tools-
	- Tablaeu: Steps for how to use with SnappyData
	- Zoomdata: Steps for how to use with SnappyData
	- Zeppelin: Steps for how to use with SnappyData (already covered in iSight?)


#Overview
This section introduces you to several common operations such as, starting a cluster, working with tables(load, query, update), working with streams and running approximate queries.

**Running the Examples:**
Topics in this section refer to source code examples that are shipped with the product. Instructions to run these examples can be found in the source code.

Source code for these examples is located in the **quickstart/src/main/scala/org/apache/spark/examples/snappydata** and in **quickstart/python** directories of the SnappyData product distribution.

Refer to the [Getting Started](quickstart.md) to either run SnappyData on premise, using AWS or Docker. 

You can run the examples in any of the following ways:

* **In the Local Mode**: By using `bin/run-example` script (to run Scala examples) or by using `bin/spark-submit` script (to run Python examples). The examples run collocated with Spark+SnappyData Store in the same JVM. 

* **As a Job**:	Many of the Scala examples are also implemented as a SnappyData job. In this case, examples can be submitted as a job to a running SnappyData cluster. Refer to [jobs](#howto-job) section for details on how to run a job.

<Note> Note: SnappyData also supports Java API. Refer to the [documentation](programming_guide/#building-snappy-applications-using-spark-api) for more details on Java API.</note>

The following topics are covered in this section:

* [How to Start a SnappyData Cluster](#howto-startCluster)
* [How to Run Spark Job inside the Cluster](#howto-job)
* [How to Access SnappyData Store from existing Spark Installation using Smart Connector](#howto-splitmode)
* [How to Create Row Tables and Run Queries](#howto-row)
* [How to Create Column Tables and Run Queries](#howto-column)
* [How to Load Data in SnappyData Tables](#howto-load)
* [How to perform a Collocated Join](#howto-collacatedJoin)
* [How to Connect using JDBC Driver](#howto-jdbc)
* [How to Store and Query JSON Objects](#howto-JSON)
* [How to Store and Query Objects](#howto-objects)
* [How to Use Stream Processing with SnappyData](#howto-streams)
* [How to Use Synopsis Data Engine to Run Approximate Queries](#howto-sde)
* [How to Use Python to Create Tables and Run Queries](#howto-python)


<a id="howto-startCluster"></a>
## How to Start a SnappyData Cluster
### Start SnappyData Cluster on a Single Machine

If you have [downloaded and extracted](install/#install-on-premise) the SnappyData product distribution, navigate to the SnappyData product root directory.

**Start the Cluster**: Run the `sbin/snappy-start-all.sh` script to start SnappyData cluster on your single machine using default settings. This starts one lead node, one locator, and one data server.

```
$ sbin/snappy-start-all.sh
```
It may take 30 seconds or more to bootstrap the entire cluster on your local machine.

**Sample Output**: The sample output for `snappy-start-all.sh` is displayed as:

```
Starting SnappyData Locator using peer discovery on: localhost[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/user/snappyData/work/localhost-locator-1/snappylocator.log
SnappyData Locator pid: 9368 status: running
Starting SnappyData Server using locators for peer discovery: shirishd-laptop[10334]
Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
Logs generated in /home/shirishd/snappyData/work/localhost-server-1/snappyserver.log
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
Starting SnappyData Leader using locators for peer discovery: shirishd-laptop[10334]
Logs generated in /home/shirishd/snappyData/work/localhost-lead-1/snappyleader.log
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```

**Check Status**: You can check the status of a running cluster using the following command:

```
$ sbin/snappy-status-all.sh
SnappyData Locator pid: 9368 status: running
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```

You can check SnappyData UI by opening `http://<leadHostname>:4040` in browser, where **leadHostname** is the host name of your lead node). Use [snappy-shell](#howto-snappyShell) to connect to the cluster and perform various SQL operations.

**Shutdown Cluster**: You can shutdown the cluster using the `sbin/snappy-stop-all.sh` command:

```
$ sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```

### Start SnappyData Cluster on Multiple Hosts

To start the cluster on multiple hosts:

1. The easiest way to run SnappyData on multiple nodes is to use a shared file system such as NFS on all the nodes.</br> You can also extract the product distribution on each node of the cluster. If all nodes have NFS access, install SnappyData on any one of the nodes.

2. Create the configuration files using the templates provided in the **conf** folder. Copy the exiting template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.
</br> Edit the files to include the hostnames on which to start the server, locator, and lead. Refer to the [configuration](configuration/#configuration-files) section for more information on properties.

3. Start the cluster using `sbin/snappy-start-all.sh`. SnappyData starts the cluster using SSH.

<Note> Note: It is recommended that you set up passwordless SSH on all hosts in the cluster. Refer to the documentation for more details on [installation](install/#install-on-premise) and [cluster configuration](configuration).
</Note>

<a id="howto-job"></a>
## How to Run Spark Code inside the Cluster
A Spark program that runs inside a SnappyData cluster is implemented as a SnappyData job.

**Implementing a Job**: 
A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. In the `runSnappyJob` method of the job, you implement the logic for your Spark program using SnappySession object instance passed to it. You can perform all operations such as create table, load data, execute queries using the SnappySession. <br/>
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
    <version>0.7</version>
</dependency>
```
**Example: SBT dependency**:

```
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "0.7"
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

For more information on the various modes, refer to the [SnappyData Smart Connector](deployment#snappydata-smart-connector-mode) section of the documentation.

**Code Example:**
The code example for this mode is in [SmartConnectorExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SmartConnectorExample.scala)

**Configure a SnappySession**: 
The code below shows how to initialize a SparkSession. Here the property `snappydata.store.locators` instructs the connector to acquire cluster connectivity and catalog meta data, and registers it locally in the Spark cluster.

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("SmartConnectorExample")
        // It can be any master URL
        .master("local[4]")
        // snappydata.store.locators property enables the application to interact with SnappyData store
        .config("snappydata.store.locators", "localhost:10334")
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
## How to Use SnappyData SQL shell (snappy-shell)
`snappy-shell` can be used to execute SQL on SnappyData cluster. In the background, `snappy-shell` uses JDBC connections to execute SQL.

**Connect to a SnappyData Cluster**: 
Use the `snappy-shell` and `connect client` command on the Snappy Shell

```
$ bin/snappy-shell
snappy> connect client 'locatorHostName:1527';
```

 **1527** is the default port on which locatorHost listens for connections. 

**Execute SQL queries**: Once connected you can execute SQL queries using `snappy-shell`

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

Refer to the [Row and column tables](programming_guide#tables-in-snappydata) documentation for complete list of attributes for row tables.

Full source code, for example, to create and perform operations on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


###Create a Row Table using DataFrame API:

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
First we define the table schema and then create the table using createTable API

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
    // "PERSISTENT" flag indicates that the table data should be persisted to
    // disk asynchronously
    val props1 = Map("PERSISTENT" -> "asynchronous")
    // create a row table using createTable API
    snSession.createTable("SUPPLIER", "row", schema, props1)
```

**Creating a Row table using SQL**:
The same table can be created using SQL as shown below:
```
    // First drop the table if it exists
    snSession.sql("DROP TABLE IF EXISTS SUPPLIER")
    // Create a row table using SQL
    // "PERSISTENT" that the table data should be persisted to disk asynchronously
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
          ") USING ROW OPTIONS (PERSISTENT 'asynchronous')")
```

You can perform various operations such as inset data, mutate it (update/delete), select data from the table. All these operations can be done either through APIs or by using SQL queries.
For example:

** To insert data in the SUPPLIER table:**

```	
	snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")
```
** To print the contents of the SUPPLIER table:**

```    
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
** To update the table account balance for SUPPLIER4:**

```    
    snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")
```
**To print contents of the SUPPLIER table after update**

```
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
**To delete the records for SUPPLIER2 and SUPPLIER3
**
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

Column tables organize and manage data in columnar form such that modern day CPUs can traverse and run computations like a sum or an average fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](programming_guide#tables-in-snappydata) documentation for the complete list of attributes for column tables.

Full source code, for example, to create and perform operations on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

###Create a Column Table using DataFrame API

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
**Define the table schema **

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

###Create a Column Table using SQL

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
## How to Load Data in SnappyData Tables
You can use Spark's DataFrameReader API in order to load data into SnappyData tables using different formats (Parquet, CSV, JSON etc.).

**Code Example**

**Get a SnappySession**

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[*]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**A column table is created as follows**

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

**Load data in the CUSTOMER table from a CSV file by using DataFrameReader API**

```
    val tableSchema = snSession.table("CUSTOMER").schema
    val customerDF = snSession.read.schema(schema = tableSchema).csv(s"$dataFolder/customer.csv")
    customerDF.write.insertInto("CUSTOMER")
```

**For Parquet file, use code as given below**

```
val customerDF = snSession.read.parquet(s"$dataDir/customer_parquet")
customerDF.write.insertInto("CUSTOMER")
```

**Inferring schema from data file**

A schema for the table can be inferred from the data file. In this case, you do not need to create a table before loading the data. In the code snippet below, we create a DataFrame for a Parquet file and then use saveAsTable API to create a table with data loaded from it.
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

The source code to load the data from a CSV/Parquet files is in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala). Source for the code to load data from a JSON file can be found in [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)


<a id="howto-collacatedJoin"></a>
## How to Perform a Collocated Join

When two tables are partitioned on columns and collocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData server. Collocating the data of two tables based on a partitioning column's value is a best practice if you frequently perform queries on those tables that join on that column.
When collocated tables are joined on the partitioning columns, the join happens locally on the node where data is present, without the need of shuffling the data.

**Code Example: ORDERS table is collocated with CUSTOMER table**

A partitioned table can be collocated with another partitioned table by using the "COLOCATE_WITH" attribute in the table options. <br/>
For example, in the code snippet below, the ORDERS table is collocated with the CUSTOMER table. The complete source for this example can be found in the file [CollocatedJoinExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CollocatedJoinExample.scala)

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CollocatedJoinExample")
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

**Perform a Collocate join: **

```
    // Selecting orders for all customers
    val result = snSession.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```

<a id="howto-jdbc"></a>
## How to Connect using JDBC Driver

You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. The locator passes the information of all available servers based on which, the driver automatically connects to one of the servers.

**To connect to the SnappyData cluster**: Using JDBC, use URL of the form `jdbc:snappydata://locatorHostName:locatorClientPort/`

**Code Example: **
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
<Note>Note: If the tool does not automatically select a driver class, you may have the option of selecting a class from within the JAR file. In this case, select the **io.snappydata.jdbc.ClientDriver** class.</Note>

<a id="howto-JSON"></a>
## How to Store and Query JSON Objects

You can insert JSON data in SnappyData tables and execute queries on the tables.

**Code Example: Loads JSON data from a JSON file into a column table and executes query**

The code snippet given below loads JSON data from a JSON file into a column table and executes the query against it.
The source code for JSON example is located at [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala). After creating SnappySession, we read the JSON file using Spark API and load into a SnappyData table

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
    val people = snSession.jsonFile(some_people_path)
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

    val builder = new StringBuilder
    nameAndAddress.collect.map(row => {
      builder.append(s"${row(0)} ,")
      builder.append(s"${row(1)} ,")
      builder.append(s"${row(2)} ,")
      builder.append(s"${row(3)} ,")
      builder.append(s"${row(4)} \n")
    })
    builder.toString

```

<a id="howto-objects"></a>
## How to Store and Query Objects

You can use domain object to load data into SnappyData tables and select the data by executing queries against the table.

**Code Example: Insert Person object into the column table**

The code snippet below inserts Person objects into a column table. The source code for this example is located at [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala). After creating SnappySession, the Person objects is inserted using Spark API and loads into a SnappyData table.

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
    val allPersons = nameAndAddress.collect.map(row => {
      Person(row(0).asInstanceOf[String],
        Address(
          row(1).asInstanceOf[Row](0).asInstanceOf[String],
          row(1).asInstanceOf[Row](1).asInstanceOf[String]
        ),
        row(2).asInstanceOf[Map[String, String]]
      )
    })
```

<a id="howto-streams"></a>
## How to Use Stream Processing with SnappyData
SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to the [documentation](programming_guide/#stream-processing-using-sql).

**Code Example**: 
Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets below shows how to declare a stream table, register continuous queries(CQ) and update SnappyData table using the stream data.

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
For more information on  SDE, refer to [SDE documentation](programming_guide/#tables-in-snappydataaqp).

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
Attribute 'qcs' in the statement below specifies the columns used for stratification and attribute 'fraction' specifies how big the sample needs to be (3% of the base table AIRLINE in this case). For more information on Synopsis Data Engine, refer to the [SDE documentation](/aqp/#working-with-stratified-samples).


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
This same table can be created by using createTable API. First we create a schema and then create the table, and then mutate the table data using API:

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
