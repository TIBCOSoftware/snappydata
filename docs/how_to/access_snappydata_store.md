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