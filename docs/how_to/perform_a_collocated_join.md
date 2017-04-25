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

**Perform a Collocate join:**

```
    // Selecting orders for all customers
    val result = snSession.sql("SELECT C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS, O_ORDERDATE, " +
        "O_TOTALPRICE FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY").collect()
```
