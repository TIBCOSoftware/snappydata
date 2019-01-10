<a id="howto-column"></a>
# How to Create Column Tables and Run Queries

Column tables organize and manage data in a columnar form such that modern day CPUs can traverse and run computations like a sum or an average fast (as the values are available in contiguous memory).

Refer to the [Row and column tables](../programming_guide/tables_in_snappydata.md) documentation for the complete list of attributes for column tables.

Full source code, for example, to create and perform operations on column table can be found in [CreateColumnTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreateColumnTable.scala)

## Create a Column Table using DataFrame API

The code snippet below shows how to create a column table using DataFrame API.

**Get a SnappySession**:

```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("CreateColumnTable")
    .master("local[*]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

**Define the table schema**

```pre
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

```pre
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

## Create a Column Table using SQL

The same table can be created using SQL as shown below:

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

You can execute selected queries on a column table, join the column table with other tables, and append data to it.
