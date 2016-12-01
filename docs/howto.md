This section provides you examples that demonstrate how to do common operations such as starting a cluster, create tables, ingest streaming data and run queries using SnappyData. You can follow the instructions given in examples source code to run those directly.

Source code for these examples is located at `quickstart/src/main/scala/org/apache/spark/examples/snappydata` directory of SnappyData product distribution.

These examples can be run either in local mode(in which case, it will spawn a single node SnappyData system) by using `bin/run-example` script or can be submitted as a [job](#howto-job) to an already running SnappyData cluster.

* [How to start SnappyData cluster](#howto-startCluster)
* [How to run Spark code inside cluster](#howto-job)
* [How to use snappy-shell](#howto-snappyShell)
* [How to create row tables and run queries](#howto-row)
* [How to create column tables and run queries](#howto-column)
* [How to do collacated join](#howto-collacatedJoin)
* [How to connect using JDBC driver and run queries](#howto-jdbc)
* [How to store and query JSON objects](#howto-JSON)
* [How to store and query objects](#howto-objects)
* [How to access SnappyData store from existing Spark installation using split mode](#howto-splitmode)

<a id="howto-startCluster"></a>
### How to start SnappyData cluster

If you have [downloaded and extracted](http://snappydatainc.github.io/snappydata/#download-binary-distribution) the SnappyData product distribution, navigate to the SnappyData product root directory. You can use the sbin/snappy-start-all.sh script to start SnappyData cluster on your single machine with default settings. This will start one lead node, one locator and one data server.

```
$ sbin/snappy-start-all.sh
```
This may take 30 seconds or more to bootstrap the entire cluster on your local machine. The sample output for snappy-start-all.sh  should look like:

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

You can check the status of a running cluster using following command:

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

You can shutdown the cluster using sbin/snappy-stop-all.sh command:

```
$ sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```


**Starting cluster using multiple hosts**

If you want to start the cluster on multiple hosts, you need to extract the product distribution on each node of the cluster(or use a file system shared by all nodes such as NFS to keep the product distribution). Create the configuration files conf/servers, conf/locators, conf/leads and mention the hostnames on which to start the server, locators, lead respectively and then start the cluster using sbin/snappy-start-all.sh. SnappyData will start the cluster using ssh. It is recommended that you set up passwordless ssh on all hosts in the cluster. Refer to the documentation for [cluster configuration](http://snappydatainc.github.io/snappydata/configuration/) for more details.

<a id="howto-job"></a>
### How to run Spark code inside cluster
A Spark program that can be run inside a SnappyData clustser is implemented as a SnappyData job. A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait.

For example:
```
class MySnappyJob extends SnappySQLJob {
  /** SnappyData uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation
}

```
In the runSnappyJob method above, you implement the logic for your Spark program using SnappyContext object instance passed to it. You can do all operations such as create table, load data, execute queries using the SnappyContext.

Use following Maven dependencies ofr your program that implements the SnappyData job:

```
groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 0.7
```

Once you create a jar for SnappyData job, use `bin/snappy-job.sh` to submit the job to SnappyData cluster and run the job.

For example, to run the job implemented in [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala) you may use the following command. Here quickstart.jar is the jar in which this program is bundled along with the product distribution.

```
 # first cd to your SnappyData product dir
 $ cd $SNAPPY_HOME
 $ bin/snappy-job.sh submit
    --app-name CreatePartitionedRowTable
    --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
    --app-jar examples/jars/quickstart.jar
    --lead [leadHost:port]
```

The above command will submit the job and run it and return output similar to:

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

Refer to the [Building Snappy applications using Spark API](http://snappydatainc.github.io/snappydata/jobs/) section of the documentation for more details.


<a id="howto-snappyShell"></a>
### How to use snappy-shell
`snappy-shell` can be used to execute any SQL queries on SnappyData cluster. `snappy-shell` uses JDBC connection to execute these queries.

To connect to a SnappyData cluster using `snappy-shell` use the `connect client` command on snappy shell. For example:

```
$ bin/snappy-shell
snappy> connect client 'locatorHost:1527';
```
Use the hostaname of locatorHost instead of leadHost. 1527 is the default port on which locatorHost listens for connections. If you have changed the client port, use the correct port number.

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
### How to create row tables and run queries

Row tables in SnappyData are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location is determined by a hash function and hence very fast for point lookups or updates. A row table can either be replicated to all nodes or partitioned across nodes. A row table can be created by using DataFrame API or by using SQL.

Refer to the [Row and column tables](http://snappydatainc.github.io/snappydata/rowAndColumnTables/) documentation for complete list of attributes for row tables.

Full source code for examples to create and perform opeartions on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


**Create a row table using DataFrame API:**

The code snippet below shows how to create a replicated row table using API.

First get a SnappySession:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)
```

Now create the table using API. For that, first we define the table schema and then create the using createTable API

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

**Create a row table using SQL:**
The same table can be created using SQL as shown below
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

You can perform various operations such as inset data, mutate it (update/delete), select data from the table. All these operations can be done either thru APIs or by using SQL queries.

For example:

```
    // inserting data in SUPPLIER table
    snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")

    // printing the contents of the SUPPLIER table
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    // update the table account balance for SUPPLIER4
    snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")

    // printing the contents of the SUPPLIER table after update
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    // delete the records for SUPPLIER2 and SUPPLIER3
    snSession.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

    // printing the contents of the SUPPLIER table after delete
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```

<a id="howto-column"></a>
### How to create column tables and run queries

Column tables organize and manage data in columnar form such that modern day CPUs can traverse and run computations like a sum or an average really fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](http://snappydatainc.github.io/snappydata/rowAndColumnTables/) documentation for complete list of attributes for column tables.

Full source code for example to create and perform opeartions on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

**Create a column table using DataFrame API:**

The code snippet below shows how to create a column table using API.

First get a SnappySession:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)
```

Now create the table using API and load data into it from CSV. For that, first we define the table schema and then create the using createTable API.

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

**Create a column table using SQL:**
The same table can be created using SQL as shown below
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
### How to do collacated join

When two tables are partitioned on columns and colocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData server. Colocating the data of two tables based on a partitioning column's value is a best practice if you will frequently perform queries on those tables that join on that column. When colocated tables are joined on the partitioning columns, the join happens locally on the node where data is present without the need of shuffling the data.

**Code Example:**

A partitioned table can be colocated with another partitioned table by using "COLOCATE_WITH" with atrribute in the table options. For example, in the code snippet below ORDERS table is colocated with CUSTOMER table. The complete source for this example can be found in file [CollocatedJoinExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CollocatedJoinExample.scala)

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
        "USING COLUMN OPTIONS (PARTITION_BY 'O_ORDERKEY', " +
        "COLOCATE_WITH 'CUSTOMER' )")
```

Now the following join query wil do a colocated join:

```
    // Selecting orders for all customers
    val result = snSession.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```

<a id="howto-jdbc"></a>
### How to connect using JDBC driver

You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. The locator will pass the information of all available servers based on which the driver automatically connects to one of the servers.

In order to connect to the SnappyData cluster using JDBC, use URL of the form `jdbc:snappydata://locatorHostName:locatorClientPort/`

**Code Example:**

The code snippet shows how to connect to a SnappyData cluster using JDBC on default clietnt port 1527. The complete source code of the example is at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)
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

// Inserting a record in PARTSUPP table via batch inserts
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
### How to store and query JSON objects

You may insert JSON data in SnappyData tables and execute queries on those tables.

**Code Example:**

The code snippet given below loads JSON data from a JSON file into a column table and executes query against it.
The source code for JSON example is in [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)

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
### How to store and query objects

You can use domain object to load the data into SnappyData tables and select the data by executing queries against the table.

**Code Example:**

The code snippet below inserts Person objects into a column table. The source code for this example is in [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala)

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
### How to access SnappyData store from existing Spark installation using split mode

If you have an existing Apache Spark installation, you can use SnappyData cluster as a store to create and store data in tables. In this case, Spark executors and SnappyStore form independent clusters. When SnappyData is used in split mode, the Apache Spark cluster gets access to the store catalog (thru the driver node). When accessing partitioned data, all access is automatically parallelized between Spark executors and SnappyData nodes.

For more information on Split mode, refer to the [deployment](http://snappydatainc.github.io/snappydata/deployment/#split-cluster-mode) section of documentation


**Code Example:**
The code example for split mode is in [SplitModeApplicationExample.scala](#https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/SplitModeApplicationExample.scala) The code below shows how to initialize a SparkSession to use it in split mode. Here the property `snappydata.store.locators` instructs the Spark to start SnappyData accessor process to read the SnappyData catalog.


```
    val spark: SparkSession = SparkSession
        .builder
        .appName("SplitModeApplicationExample")
        // It can be any master URL
        .master("local[4]")
        // snappydata.store.locators property enables the application to interact with SnappyData store
        .config("snappydata.store.locators", "localhost:10334")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)

```

Now you can create tables and fire queries in SnappyData store using you Apache Spark program

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
