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