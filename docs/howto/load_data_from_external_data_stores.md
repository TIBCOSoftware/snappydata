<a id="howto-external-source"></a>
# How to Load Data from External Data Stores (e.g. HDFS, Cassandra, Hive, etc) 

SnappyData comes bundled with the libraries to access HDFS (Apache compatible). You can load your data using SQL or DataFrame API.

## Example - Loading data from CSV file using SQL

```pre
// Create an external table based on CSV file
CREATE EXTERNAL TABLE CUSTOMER_STAGING_1 USING csv OPTIONS (path '../../quickstart/src/main/resources/customer_with_headers.csv', header 'true', inferSchema 'true');

// Create a SnappyData table and load data into CUSTOMER table
CREATE TABLE CUSTOMER using column options() as (select * from CUSTOMER_STAGING_1);
```

!!! Tip
	Similarly, you can create an external table for all data sources and use SQL "insert into" query to load data. For more information on creating external tables refer to, [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table/)


## Example - Loading CSV Files from HDFS using API

The example below demonstrates how you can read CSV files from HDFS using an API:

```pre
val dataDF=snc.read.option("header","true").csv ("hdfs://namenode-uri:port/path/to/customer_with_headers.csv")

// Drop table if it exists
snc.sql("drop table if exists CUSTOMER")

// Load data into table
dataDF.write.format("column").saveAsTable("CUSTOMER")
```

## Example - Loading and Enriching CSV Data from HDFS 

The example below demonstrates how you can load and enrich CSV Data from HDFS:
```pre
val dataDF = snappy.read.option("header", "true")
    .csv("hdfs://namenode-uri:port/path/to/customers.csv")

// Drop table if it exists and create it with only required fields
snappy.sql("drop table if exists CUSTOMER")
snappy.sql("create table CUSTOMER(C_CUSTKEY INTEGER NOT NULL" +
    ", C_NAME VARCHAR(25) NOT NULL," +
    " C_ADDRESS VARCHAR(40) NOT NULL," +
    " C_NATIONKEY INTEGER NOT NULL," +
    " C_PHONE VARCHAR(15) NOT NULL," +
    " C_ACCTBAL DECIMAL(15,2) NOT NULL," +
    " C_MKTSEGMENT VARCHAR(10) NOT NULL," +
    " C_COMMENT VARCHAR(117) NOT NULL) using column options()")

// Project and transform data from df and load it in table.
import snappy.implicits._
dataDF.select($"C_CUSTKEY",
  $"C_NAME",
  $"C_ADDRESS",
  $"C_NATIONKEY",
  $"C_PHONE",
  $"C_ACCTBAL" + 100,
  $"C_MKTSEGMENT",
  $"C_COMMENT".substr(1, 5).alias("SHORT_COMMENT")).write.insertInto("CUSTOMER")
```

## Example - Loading from Hive
As SnappyData manages the catalog at all times and it is not possible to configure an external Hive catalog service like in Spark when using a SnappySession. But, it is still possible to access Hive using the native SparkSession (with **enableHiveSupport** set to **true**). 
Here is an example using the SparkSession(spark object below) to access a Hive table as a DataFrame, then converted to an RDD so it can be passed to a SnappySession to store it in a SnappyData Table. 

```pre
val ds = spark.table("hiveTable")
val rdd = ds.rdd
val session = new SnappySession(sparkContext)
val df = session.createDataFrame(rdd, ds.schema)
df.write.format("column").saveAsTable("columnTable")
```

## Importing Data using JDBC from a relational DB

!!! Note
	Before you begin, you must install the corresponding JDBC driver. To do so, copy the JDBC driver jar file in **/jars** directory located in the home directory and then restart the cluster.

<!--**TODO: This is a problem- restart the cluster ? Must confirm package installation or at least get install_jar tested for this case. -- Jags**
-->

The example below demonstrates how to connect to any SQL database using JDBC:


1. Verify and load the SQL Driver:

	    Class.forName("com.mysql.jdbc.Driver")
    
2. Specify all the properties to access the database

        import java.util.Properties
        val jdbcUsername = "USER_NAME"
        val jdbcPassword = "PASSWORD"
        val jdbcHostname = "HOSTNAME"
        val jdbcPort = 3306
        val jdbcDatabase ="DATABASE"
        val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}&relaxAutoCommit=true"

        val connectionProperties = new Properties()
        connectionProperties.put("user", "USERNAME")
        connectionProperties.put("password", "PASSWORD")

3. Fetch the table meta data from the RDB and creates equivalent column tables 

        val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
        connection.isClosed()
        val md:DatabaseMetaData = connection.getMetaData();
        val rs:ResultSet = md.getTables(null, null, "%", null);
        while (rs.next()) {

        val tableName=rs.getString(3)
        val df=snc.read.jdbc(jdbcUrl, tableName, connectionProperties)
        df.printSchema
        df.show()
        // Create and load a column table with same schema as that of source table 
           df.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
        }

** Using SQL to access external RDB tables **
You can also use plain SQL to access any external RDB using external tables. Create external table on RDBMS table and query it directly from SnappyData as described below:

```pre     
snc.sql("drop table if exists external_table")
snc.sql(s"CREATE  external TABLE external_table USING jdbc OPTIONS (dbtable 'tweet', driver 'com.mysql.jdbc.Driver',  user 'root',  password 'root',  url '$jdbcUrl')")
snc.sql("select * from external_table").show
```

Refer to the [Spark SQL JDBC source access for how to parallelize access when dealing with large data sets](https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#jdbc-to-other-databases).


## Loading Data from NoSQL store (Cassandra)

The example below demonstrates how you can load data from a NoSQL store:

!!! Note
	Before you begin, you must install the corresponding Spark-Cassandra connector jar. To do so, copy the Spark-Cassandra connector jar file to the **/jars** directory located in the home directory and then restart the cluster.

<!--**TODO** This isn't a single JAR from what I know. The above step needs testing and clarity. -- Jags
-->

```pre
val df = snc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "CUSTOMER", "keyspace" -> "test")) .load
df.write.format("column").mode(SaveMode.Append).saveAsTable("CUSTOMER")
snc.sql("select * from CUSTOMER").show
```
