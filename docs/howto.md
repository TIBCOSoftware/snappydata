This section provides you examples that demonstrate how to do common operations such as starting a cluster, create tables, ingest streaming data and run queries using SnappyData. You can follow the instructions given in examples source code to run those directly.

Source code for these examples is located at `quickstart/src/main/scala/org/apache/spark/examples/snappydata` directory of SnappyData product distribution.

These examples can be run either in local mode(in which case, it will spawn a single node SnappyData system) by using `bin/run-example` script or can be submitted as a [job](#howto-job) to an already running SnappyData cluster.

* [How to start SnappyData cluster](#howto-startCluster)
* [How to run Spark code inside cluster](#howto-job)
* [How to use snappy-shell](#howto-snappyShell)
* [How to create row tables and run queries](#howto-row)
* [How to create column tables and run queries](#howto-column)
* [How to do collacated join](#howto-collacatedJoin)
* [How to use SnappyData as SQL database using JDBC driver](#howto-jdbc)
* [How to store and query JSON objects](#howto-JSON)
* [How to store and query objects](#howto-objects)
* [How to access SnappyData store from existing Spark installation using split mode](#howto-splitmode)

<a id="howto-startCluster"></a>
### How to start SnappyData cluster

**DESCRIPTION: **

**Steps:**
```
Add details
Step 1
Step 2
```

<a id="howto-job"></a>
### How to run Spark code inside cluster

**DESCRIPTION: **

**Steps:**
```
Add details
Step 1
Step 2
```

<a id="howto-snappyShell"></a>
### How to use snappy-shell

**DESCRIPTION: **

**Steps:**
```
Add details
Step 1
Step 2
```

<a id="howto-row"></a>
### How to create row tables and run queries

**DESCRIPTION: **
Row tables in SnappyData are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location is determined by a hash function and hence very fast for point lookups or updates. A row table can either be replicated to all nodes or partitioned across nodes. A row table can be created by using DataFrame API or by using SQL.

Refer to the [Row and column tables](http://snappydatainc.github.io/snappydata/rowAndColumnTables/) documentation for complete list of attributes for row tables.

Full source code for examples to create and perform opeartions on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


**Create a row table using DataFrame API:**
The code snippet below shows how to create a replicated row table using API.

First get a SnappyContext:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)
    val snc = snSession.snappyContext
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
    // For complete list of attributes refer the documentation
    val props1 = Map("PERSISTENT" -> "asynchronous")
    // create a row table using createTable API
    snc.createTable("SUPPLIER", "row", schema, props1)
```

**Create a row table using SQL:**
The same table can be created using SQL as shown below
```
    // Create a row table using SQL
    // "PERSISTENT" that the table data should be persisted to disk asynchronously
    // For complete list of attributes refer the documentation
    snc.sql(
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
    snc.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")

    // printing the contents of the SUPPLIER table
    var tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    // update the table account balance for SUPPLIER4
    snc.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")

    // printing the contents of the SUPPLIER table after update
    tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    // delete the records for SUPPLIER2 and SUPPLIER3
    snc.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

    // printing the contents of the SUPPLIER table after delete
    tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```

<a id="howto-column"></a>
### How to create column tables and run queries

**DESCRIPTION: **
Column tables organize and manage data in columnar form such that modern day CPUs can traverse and run computations like a sum or an average really fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](http://snappydatainc.github.io/snappydata/rowAndColumnTables/) documentation for complete list of attributes for column tables.

Full source code for example to create and perform opeartions on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

**Create a column table using DataFrame API:**
The code snippet below shows how to create a column table using API.

First get a SnappyContext:

```
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)
    val snc = snSession.snappyContext
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
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
    // "BUCKETS" attribute specifies the smallest unit that can be moved around in
    // SnappyStore when the data migrates. Here we configure the table to have 11 buckets
    // For complete list of attributes refer the documentation
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY", "BUCKETS" -> "11")
    snc.createTable("CUSTOMER", "column", tableSchema, props1)

    // insert some data in it
    // loading data in CUSTOMER table from a text file with delimited columns
    val customerDF = snc.read.
        format("com.databricks.spark.csv").schema(schema = tableSchema).
        load(s"quickstart/src/resources/customer.csv")
    customerDF.write.insertInto("CUSTOMER")
```

**Create a column table using SQL:**
The same table can be created using SQL as shown below
```
    snc.sql("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY', BUCKETS '11' )")
```

You can execute select queries on column table, join the column table with other tables and append data to it.

<a id="howto-collacatedJoin"></a>
### How to do collacated join

**DESCRIPTION: **
When two tables are partitioned on columns and colocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData server. Colocating the data of two tables based on a partitioning column's value is a best practice if you will frequently perform queries on those tables that join on that column. When colocated tables are joined on the partitioning columns, the join happens locally on the node where data is present without the need of shuffling the data.

**Code Example:**
A partitioned table can be colocated with another partitioned table by using "COLOCATE_WITH" with atrribute in the table options. For example, in the code snippet below ORDERS table is colocated with CUSTOMER table. The complete source for this example can be found in file [CollocatedJoinExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/CollocatedJoinExample.scala)

```
    snc.sql("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY', BUCKETS '11' )")
        
    snc.sql("CREATE TABLE ORDERS  ( " +
        "O_ORDERKEY       INTEGER NOT NULL," +
        "O_CUSTKEY        INTEGER NOT NULL," +
        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
        "O_ORDERDATE      DATE NOT NULL," +
        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
        "O_CLERK          CHAR(15) NOT NULL," +
        "O_SHIPPRIORITY   INTEGER NOT NULL," +
        "O_COMMENT        VARCHAR(79) NOT NULL) " +
        "USING COLUMN OPTIONS (PARTITION_BY 'O_ORDERKEY', BUCKETS '11', " +
        "COLOCATE_WITH 'CUSTOMER' )")
```

Now the following join query wil do a colocated join:

```
    // Selecting orders for all customers
    val result = snc.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```

<a id="howto-jdbc"></a>
### How to connect using JDBC driver

**DESCRIPTION: **
You can connect to and execute queries against SnappyData cluster using JDBC driver. The connection URL typically points to one of the locators. Underneath the covers, the driver acquires the endpoints for all the servers in the cluster along with load information and automatically connects clients to one of the data servers directly. The driver provides HA by automatically swizzling underlying physical connections in case servers were to fail.

In order to connect to the SnappyData cluster using JDBC, use URL of the form `jdbc:snappydata://locatorHostName:locatorClientPort/`

**Code Example:**
The code snippet shows how to connect to a SnappyData cluster using JDBC on default clietnt port 1527. The complete source code of the example is at [JDBCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/JDBCExample.scala)
```
val url: String = s"jdbc:snappydata://localhost:1527/"
val conn1 = DriverManager.getConnection(url)

val stmt1 = conn1.createStatement()
println("Creating a table (PARTSUPP) using JDBC connection")
stmt1.execute("DROP TABLE IF EXISTS APP.PARTSUPP")
stmt1.execute("CREATE TABLE APP.PARTSUPP ( " +
     "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
     "PS_SUPPKEY     INTEGER NOT NULL," +
     "PS_AVAILQTY    INTEGER NOT NULL," +
     "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
    "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY', BUCKETS '11' )")

println("Inserting a record in PARTSUPP table via batch inserts")
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
**DESCRIPTION: **
You may insert JSON data in SnappyData tables and execute queries on those tables.

**Code Example:**
The code snippet given below loads JSON data from a JSON file into a column table and executes query against it.
The source code for JSON example is in [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala)

```
    val some_people_path = s"quickstart/src/main/resources/some_people.json"
    // Read a JSON file using Spark API
    val people = snc.jsonFile(some_people_path)
    people.printSchema()

    //Drop the table if it exists.
    snc.dropTable("people", ifExists = true)

    // Write the created DataFrame to a column table.
    people.write.format("column").saveAsTable("people")

    // Append more people to the column table
    val more_people_path = s"quickstart/src/main/resources/more_people.json"

    //Explicitly passing schema to handle record level field mismatch
    // e.g. some records have "district" field while some do not.
    val morePeople = snc.read.schema(people.schema).json(more_people_path)
    morePeople.write.insertInto("people")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snc.table("people").schema)

    // Query it like any other table
    val nameAndAddress = snc.sql("SELECT " +
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
**DESCRIPTION: **
You can use domain object to load the data into SnappyData tables and select the data by executing queries against the table.

**Code Example:**
The code snippet below inserts Person objects into a column table. The source code for this example is in [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/SNAP-1090/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala)

```
    //Import the implicits for automatic conversion between Objects to DataSets.
    import snc.implicits._

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
    println(snc.table("people").schema)
    

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
### How to Access SnappyData Store from Existing Spark Application (Split Mode)

**DESCRIPTION: **


**Code Example:**
```
ENTER CODE HERE

```

<a id="howto-queryscan"></a>
### Demonstrating Query/Scan Performances
**DESCRIPTION: **


**Code Example:**
```
ENTER CODE HERE

```


