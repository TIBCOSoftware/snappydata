<a id="howto-row"></a>
# How to Create Row Tables and Run Queries

Each record in a Row table is managed in contiguous memory, and therefore, optimized for selective queries (For example. key based point lookup ) or updates. 
A row table can either be replicated to all nodes or partitioned across nodes. It can be created by using DataFrame API or using SQL.

Refer to the [Row and column tables](../programming_guide/tables_in_snappydata.md#row-and-column-tables) documentation for complete list of attributes for row tables.

Full source code, for example, to create and perform operations on replicated and partitioned row table can be found in [CreateReplicatedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateReplicatedRowTable.scala) and [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala)


## Create a Row Table using DataFrame API:

The code snippet below shows how to create a replicated row table using API.

**Get a SnappySession**

```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("CreateReplicatedRowTable")
    .master("local[*]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
import org.apache.spark.sql.types._
```

**Create the Table using API**:
First, define the table schema and then create the table using createTable API

```pre
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
```pre
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

```pre
snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")
```

**To print the contents of the SUPPLIER table:** 

```pre
var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
tableData.foreach(println)
```

**To update the table account balance for SUPPLIER4:** 

```pre
snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")
```

**To print contents of the SUPPLIER table after update** 

```pre
tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
tableData.foreach(println)
```

**To delete the records for SUPPLIER2 and SUPPLIER3** 

```pre
snSession.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

```

**To print the contents of the SUPPLIER table after delete**

```pre
tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
tableData.foreach(println)
```
