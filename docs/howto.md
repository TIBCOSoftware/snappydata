This section provides several examples using which, you can understand and try common operations such as, starting a cluster, creating tables, ingesting streaming data and running queries using SnappyData. 

You can also follow the instructions given in examples source code to run them directly.
Source code for these examples is located in the **quickstart/src/main/scala/org/apache/spark/examples/snappydata** directory of SnappyData product distribution.

**Running the Examples:**
You can run the examples any of the following ways:

* **In the Local Mode**: It spawns a single node SnappyData system) by using `bin/run-example` script  
* **As a Job**:	Submitted as a [job](#howto-job) to an already running SnappyData cluster

The following topics are covered in this section:

* [How to Start a SnappyData cluster](#howto-startCluster)
* [How to Run Spark Code inside the Cluster](#howto-job)
* [How to Use snappy-shell](#howto-snappyShell)
* [How to Create Row Tables and Run Queries](#howto-row)
* [How to Create Column Tables and Run Queries](#howto-column)
* [How to perform a Collocated Join](#howto-collacatedJoin)
* [How to Connect using JDBC Driver and Run Queries](#howto-jdbc)
* [How to Store and Query JSON Objects](#howto-JSON)
* [How to Store and Query Objects](#howto-objects)
* [How to Access SnappyData Store from existing Spark Installation using Split Mode](#howto-splitmode)
* [How to Use Python to Create Tables and Run Queries](#howto-python)

<a id="howto-startCluster"></a>
### How to Start a SnappyData Cluster
#### Start SnappyData Cluster on a Single Machine

If you have [downloaded and extracted](http://snappydatainc.github.io/snappydata/#download-binary-distribution) the SnappyData product distribution, navigate to the SnappyData product root directory.

**Start the Cluster**: Run the `sbin/snappy-start-all.sh` script to start SnappyData cluster on your single machine using default settings. This starts one lead node, one locator and one data server.

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

**Shutdown Cluster**: You can shutdown the cluster using the `sbin/snappy-stop-all.sh` command:

```
$ sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```

#### Start SnappyData Cluster on Multiple Hosts

To start the cluster on multiple hosts:

1. Extract the product distribution on each node of the cluster(or use a file system shared by all nodes such as NFS to keep the product distribution). 
2. Create the configuration files **conf/servers**, **conf/locators**, **conf/leads** and mention the hostnames on which to start the server, locators, lead respectively. 
3. Start the cluster using `sbin/snappy-start-all.sh`. SnappyData starts the cluster using SSH.

> Note: It is recommended that you set up passwordless SSH on all hosts in the cluster. Refer to the documentation for [cluster configuration](http://snappydatainc.github.io/snappydata/configuration/) for more details.

<a id="howto-job"></a>
### How to Run Spark Code inside the Cluster
A Spark program that runs inside a SnappyData cluster is implemented as a SnappyData job. A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait.

For example: In the `runSnappyJob` method, you implement the logic for your Spark program using SnappyContext object instance passed to it.

```
class MySnappyJob extends SnappySQLJob {
  /** SnappyData uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation
}
```
Similarly, you can do all operations such as create table, load data, execute queries using the SnappyContext.

Use the following Maven dependencies for your program that implements the SnappyData job:

```
groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 0.7
```

Once you create a jar for SnappyData job, use `bin/snappy-job.sh` to submit the job to SnappyData cluster and run the job.

For example, to run the job implemented in [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala) you can use the following command.
Here **quickstart.jar** is the jar in which this program is bundled along with the product distribution.
For example: The command submits the job and runs it as:

```
 # first cd to your SnappyData product dir
 $ cd $SNAPPY_HOME
 $ bin/snappy-job.sh submit
    --app-name CreatePartitionedRowTable
    --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
    --app-jar examples/jars/quickstart.jar
    --lead [leadHost:port]
```

It returns output similar to:

```
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```

You can check the status of the job using the job id listed above:

```
bin/snappy-job.sh status -lead hostNameOfLead:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48
```

Refer to the [Building Snappy applications using Spark API](./jobs) section of the documentation for more details.


<a id="howto-snappyShell"></a>
### How to Use snappy-shell
`snappy-shell` can be used to execute any SQL queries on SnappyData cluster. `snappy-shell` uses JDBC connection to execute these queries.

To connect to a SnappyData cluster using `snappy-shell` use the `connect client` command on snappy shell. 

For example:

```
$ bin/snappy-shell
snappy> connect client 'locatorHost:1527';
```

Use the hostaname of locatorHost instead of leadHost. **1527** is the default port on which locatorHost listens for connections. If you have changed the client port, use the correct port number.

Once connected you can execute SQL queries using `snappy-shell`

```
snappy> CREATE TABLE PARTSUPP (PS_PARTKEY INTEGER NOT NULL PRIMARY KEY, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL) USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY') ;

snappy> INSERT INTO PARTSUPP VALUES(100, 1, 5000, 100);

snappy> INSERT INTO PARTSUPP VALUES(200, 2, 50, 10);

snappy> SELECT * FROM PARTSUPP;
PS_PARTKEY |PS_SUPPKEY |PS_AVAILQTY|PS_SUPPLYCOST    
-----------------------------------------------------
100        |1          |5000       |100.00           
200        |2          |50         |10.00            

2 rows selected

```

<a id="howto-row"></a>
### How to Create Row Tables and Run Queries

Row tables in SnappyData are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location is determined by a hash function and thereofre fast for point lookups or updates. 
A row table can either be replicated to all nodes or partitioned across nodes. It can be created by using DataFrame API or by using SQL.

Refer to the [Row and column tables](./rowAndColumnTables/) documentation for complete list of attributes for row tables.

Full source code for examples to create and perform opeartions on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


####Create a row table using DataFrame API:

The code snippet below shows how to create a replicated row table using API.

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**Create the table using API**: For that, first we define the table schema and then create the using createTable API

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

####Create a row table using SQL:
The same table can be created using SQL as shown below:
```
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

* To insert data in the SUPPLIER table:
```	
	snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")
```
* To print the contents of the SUPPLIER table:
```    
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
* The update the table account balance for SUPPLIER4:
```    
    snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")
```
* To print the contents of the SUPPLIER table after update:
```
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```
* To delete the records for SUPPLIER2 and SUPPLIER3:
```
    snSession.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")
```
* To print the contents of the SUPPLIER table after delete:
```    
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```

<a id="howto-column"></a>
### How to Create Column Tables and Run Queries

Column tables organize and manage data in columnar form such that modern day CPUs can traverse and run computations like a sum or an average fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](./rowAndColumnTables) documentation for complete list of attributes for column tables.

Full source code for example to create and perform opeartions on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

####Create a Column Table using DataFrame API:

The code snippet below shows how to create a column table using Dataframe API.

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[4]")
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

**Create the table and load data from CSV**:

```
    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    snSession.createTable("CUSTOMER", "column", tableSchema, props1)

    // insert some data in it
    // loading data in CUSTOMER table from a text file with delimited columns
    val customerDF = snSession.read.
        format("com.databricks.spark.csv").schema(schema = tableSchema).
        load(s"quickstart/src/resources/customer.csv")
    customerDF.write.insertInto("CUSTOMER")
```

####Create a Column Table using SQL:
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

You can execute select queries on column table, join the column table with other tables and append data to it.

<a id="howto-collacatedJoin"></a>
### How to perform a Collocated Join

When two tables are partitioned on columns and collocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData server. Colocating the data of two tables based on a partitioning column's value is a best practice if you frequently perform queries on those tables that join on that column.
When collocated tables are joined on the partitioning columns, the join happens locally on the node where data is present without the need of shuffling the data.

**Code Example: ORDERS table is collocated with CUSTOMER table**

A partitioned table can be collocated with another partitioned table by using "COLOCATE_WITH" with atrribute in the table options. For example, in the code snippet below ORDERS table is collocated with CUSTOMER table. The complete source for this example can be found in file [CollocatedJoinExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CollocatedJoinExample.scala)
**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

**To create Table Customer:**
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
**To create Table Orders:**
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

**To perform a collocate join: **

```
    // Selecting orders for all customers
    val result = snSession.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```

<a id="howto-jdbc"></a>
### How to Connect using JDBC Driver

You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. The locator passes the information of all available servers based on which the driver automatically connects to one of the servers.

**To connect to the SnappyData cluster**: Using JDBC, use URL of the form `jdbc:snappydata://locatorHostName:locatorClientPort/`

**Code Example: Connect to a SnappyData cluster using JDBC on default client port**

The code snippet shows how to connect to a SnappyData cluster using JDBC on default client port 1527. The complete source code of the example is located at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)
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

<a id="howto-JSON"></a>
### How to Store and Query JSON objects

You can insert JSON data in SnappyData tables and execute queries on the tables.

**Code Example: Loads JSON data from a JSON file into a column table and executes query**

The code snippet given below loads JSON data from a JSON file into a column table and executes query against it.
The source code for JSON example is located at [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

Read the JSON file using Spark API and load the data into a table

```
    val some_people_path = s"quickstart/src/main/resources/some_people.json"
    // Read a JSON file using Spark API
    val people = snSession.jsonFile(some_people_path)
    people.printSchema()

    //Drop the table if it exists.
    snSession.dropTable("people", ifExists = true)

    // Write the created DataFrame to a column table.
    people.write.format("column").saveAsTable("people")

    // Append more people to the column table
    val more_people_path = s"quickstart/src/main/resources/more_people.json"

    //Explicitly passing schema to handle record level field mismatch
    // e.g. some records have "district" field while some do not.
    val morePeople = snSession.read.schema(people.schema).json(more_people_path)
    morePeople.write.insertInto("people")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snSession.table("people").schema)

    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT " +
        "name, " +
        "address.city, " +
        "address.state, " +
        "address.district, " +
        "address.lane " +
        "FROM people")

    // return the query result
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
### How to Store and Query Objects

You can use domain object to load the data into SnappyData tables and select the data by executing queries against the table.

**Code Example: Insert Person object into the column table**

The code snippet below inserts Person objects into a column table. The source code for this example is located at [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala)

**Get a SnappySession**:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
```

Insert the Person objects into a tabe and excute queries
```
    //Import the implicits for automatic conversion between Objects to DataSets.
    import snSession.implicits._

    val snSession = snc.snappySession
    // Create a Dataset using Spark APIs
    val people = Seq(Person("Tom", Address("Columbus", "Ohio")), Person("Ned", Address("San Diego", "California"))).toDS()


    //Drop the table if it exists.
    snSession.dropTable("people", ifExists = true)

    // Write the created Dataset to a column table.
    people.write
        .format("column")
        .options(Map("BUCKETS" -> "1", "PARTITION_BY" -> "name"))
        .saveAsTable("people")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snSession.table("people").schema)
    

    // Append more people to the column table
    val morePeople = Seq(Person("Jon Snow", Address("Columbus", "Ohio")),
      Person("Rob Stark", Address("San Diego", "California")),
      Person("Michael", Address("Null", "California"))).toDS()

    morePeople.write.insertInto("people")

    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT name, address.city, address.state FROM people")

    // return the result
    val builder = new StringBuilder
    nameAndAddress.collect.map(row => {
      builder.append(s"${row(0)} ,")
      builder.append(s"${row(1)} ,")
      builder.append(s"${row(2)} \n")

    })
    builder.toString

```

<a id="howto-splitmode"></a>
### How to Access SnappyData store from an Existing Spark Installation using split mode

If you have an existing Apache Spark installation, you can use SnappyData cluster as a store to create and store data in tables. In this case, Spark executors and SnappyStore form independent clusters. When SnappyData is used in split mode, the Apache Spark cluster gets access to the store catalog (through the driver node). When accessing partitioned data, all access is automatically parallelized between Spark executors and SnappyData nodes.

For more information on Split mode, refer to the [deployment](http://snappydatainc.github.io/snappydata/deployment/#split-cluster-mode) section of documentation


**Code Example:**
The code example for split mode is in [SplitModeApplicationExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/SplitModeApplicationExample.scala) The code below shows how to initialize a SparkSession to use it in split mode. Here the property `snappydata.store.locators` instructs the Spark to start SnappyData accessor process to read the SnappyData catalog.

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("SplitModeApplicationExample")
        // It can be any master URL
        .master("local[4]")
        // snappydata.store.locators property enables the application to interact with SnappyData store
        .config("snappydata.store.locators", "localhost:10334")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)

```

**Create Table and run Queries**: You can now create tables and fire queries in SnappyData store using you Apache Spark program

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


<a id="howto-python"></a>
### How to Use Python to Create Tables and Run Queries

Developers can write programs in Python to use SnappyData features.

In order to use SnappyData features in your Python program, first create SnappyContext:

```
    conf = SparkConf().setAppName('Python Example').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # get the SnappyContext
    snContext = SnappyContext(sc)
```

You may execute SQL queries to create tables and execute queries using SnappyContext:

```
    # Creating partitioned table PARTSUPP using SQL
    snContext.sql("DROP TABLE IF EXISTS PARTSUPP")
    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY),
    # For complete list of table attributes refer the documentation
    # http://snappydatainc.github.io/snappydata/rowAndColumnTables/
    snContext.sql("CREATE TABLE PARTSUPP ( " +
                  "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
                  "PS_SUPPKEY     INTEGER NOT NULL," +
                  "PS_AVAILQTY    INTEGER NOT NULL," +
                  "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
                  "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY' )")

    # Inserting data in PARTSUPP table using INSERT query
    snContext.sql("INSERT INTO PARTSUPP VALUES(100, 1, 5000, 100)")
    snContext.sql("INSERT INTO PARTSUPP VALUES(200, 2, 50, 10)")
    snContext.sql("INSERT INTO PARTSUPP VALUES(300, 3, 1000, 20)")
    snContext.sql("INSERT INTO PARTSUPP VALUES(400, 4, 200, 30)")
    # Printing the contents of the PARTSUPP table
    snContext.sql("SELECT * FROM PARTSUPP").show()

    # Update the available quantity for PARTKEY 100
    snContext.sql("UPDATE PARTSUPP SET PS_AVAILQTY = 50000 WHERE PS_PARTKEY = 100")
    # Printing the contents of the PARTSUPP table after update
    snContext.sql("SELECT * FROM PARTSUPP").show()

    # Delete the records for PARTKEY 400
    snContext.sql("DELETE FROM PARTSUPP WHERE PS_PARTKEY = 400")
    # Printing the contents of the PARTSUPP table after delete
    snContext.sql("SELECT * FROM PARTSUPP").show()
```

This same table can be created by using createTable API. First we create a schema and then create the table, and mutate the table data using API:

```
    # drop the table if it exists
    snContext.dropTable('PARTSUPP', True)

    schema = StructType([StructField('PS_PARTKEY', IntegerType(), False),
                     StructField('PS_SUPPKEY', IntegerType(), False),
                     StructField('PS_AVAILQTY', IntegerType(),False),
                     StructField('PS_SUPPLYCOST', DecimalType(15, 2), False)
                     ])

    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY)
    # For complete list of table attributes refer the documentation at
    # http://snappydatainc.github.io/snappydata/rowAndColumnTables/
    snContext.createTable('PARTSUPP', 'row', schema, False, PARTITION_BY = 'PS_PARTKEY')

    # Inserting data in PARTSUPP table using dataframe
    tuples = [(100, 1, 5000, Decimal(100)), (200, 2, 50, Decimal(10)),
              (300, 3, 1000, Decimal(20)), (400, 4, 200, Decimal(30))]
    rdd = sc.parallelize(tuples)
    tuplesDF = snContext.createDataFrame(rdd, schema)
    tuplesDF.write.insertInto("PARTSUPP")
    #Printing the contents of the PARTSUPP table
    snContext.sql("SELECT * FROM PARTSUPP").show()

    # Update the available quantity for PARTKEY 100
    snContext.update("PARTSUPP", "PS_PARTKEY =100", [50000], ["PS_AVAILQTY"])
    # Printing the contents of the PARTSUPP table after update
    snContext.sql("SELECT * FROM PARTSUPP").show()

    # Delete the records for PARTKEY 400
    snContext.delete("PARTSUPP", "PS_PARTKEY =400")
    # Printing the contents of the PARTSUPP table after delete
    snContext.sql("SELECT * FROM PARTSUPP").show()
```

The complete source code for the above example is in [CreateTable.py](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/examples/src/main/python/CreateTable.py)
