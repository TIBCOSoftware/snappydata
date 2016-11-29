This section provides you examples that demonstrate how to do common operations such as create tables, load data and run queries using SnappyData. You can follow the instructions given in examples source code to run those directly.

Source code for these examples is located at `quickstart/src/main/scala/org/apache/spark/examples/snappydata` directory of SnappyData product distribution.

These examples can be run either in local mode(in which case, it will spawn a single node SnappyData system) by using `bin/run-example` script or can be submitted as a job to an already running SnappyData cluster.

* [How to create row tables](#howto-row)
* [How to create column tables](#howto-column)
* [How to do collacated join](#howto-collacatedJoin)
* [How to use SnappyData as SQL database using JDBC driver](#howto-jdbc)
* [Working with JSON](#howto-JSON)
* [Working with Objects](#howto-objects)
* [How to Access SnappyData Store from Existing Spark Application (Split Mode)](#howto-splitmode)
* [Demonstrating Query/Scan Performances](#howto-queryscan)

<a id="howto-row"></a>
### How to create row tables

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

Now create the table using API:
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
    println("Inserting data in SUPPLIER table")
    snc.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snc.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")

    println("Printing the contents of the SUPPLIER table")
    var tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    println("Update the table account balance for SUPPLIER4")
    snc.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")

    println("Printing the contents of the SUPPLIER table after update")
    tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)

    println("Delete the records for SUPPLIER2 and SUPPLIER3")
    snc.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

    println("Printing the contents of the SUPPLIER table after delete")
    tableData = snc.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(println)
```

<a id="howto-column"></a>
### How to create column tables

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

Now create the table using API and load data into it from CSV:

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
    println("Loading data in CUSTOMER table from a text file with delimited columns")
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


**Code Example:**
```
ENTER CODE HERE
asdadasdsdsdsfd
dsfsdf

```

<a id="howto-jdbc"></a>
### How to connect using JDBC driver

**DESCRIPTION: **


**Code Example:**
```
ENTER CODE HERE
asdadasdsdsdsfd
dsfsdf

```

<a id="howto-JSON"></a>
### Working with JSON
**DESCRIPTION: **


**Code Example:**
```
ENTER CODE HERE

```

<a id="howto-objects"></a>
### Working with Objects
**DESCRIPTION: **


**Code Example:**
```
ENTER CODE HERE

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


