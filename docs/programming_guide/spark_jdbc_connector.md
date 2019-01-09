# Access SnappyData Tables from any Spark (2.1+) cluster

Spark applications can be run embedded inside the SnappyData cluster by submitting Jobs using **Snappy-Job.sh** or it can be run using the native [Smart Connector](/howto/spark_installation_using_smart_connector.md). However, from SnappyData 1.0.2 release the connector can only be used from a Spark 2.1 compatible cluster.
If you are using a Spark version or distribution that is based on a version higher than 2.1 then, you can use the **SnappyData JDBC Extension Connector** as described below. 

## How can Spark applications connect to SnappyData using Spark JDBC? 

Spark SQL supports reading and writing to databases using a built-in **JDBC data source**. Applications can configure and use JDBC like any other Spark data source queries return data frames and can be efficiently processed in Spark SQL or joined with other data sources. The JDBC data source is also easy to use from Java or Python.
All you need is a JDBC driver from the database vendor. Likewise, applications can use the Spark [DataFrameWriter](/reference/API_Reference/apireference_guide.md#dataframewriter) to insert, append, or replace a dataset in the database.

!!!Note
The usage model for the Spark JDBC data source is described [here](https://spark.apache.org/docs/2.1.1/sql-programming-guide.html#jdbc-to-other-databases). We strongly recommend you to go through this section in case you are not familiar with how Spark works with data sources.

### Pushing Entire Query into the Database
When Spark queries are executed against external data sources, the current Spark model can only push down filters and projections in the query down to the database. If you are running an expensive aggregation on a large data set, then the entire data set is fetched into the Spark partitions, and the query is executed inside your Spark cluster. 
However, when you use a JDBC data source, you can pass entire queries or portions of the query entirely to the database such as shown in the following sample:

```
val pushdownQuery = "(select x, sum(y), max(z) from largeTable group by x order by x) t1";
spark.read.jdbc(jdbcUrl, pushDownQuery, connectionProperties);
```

### Deficiencies in the Spark JDBC Connector <!---Advantages of SnappyData JDBC Extension Connector--->
Unfortunately, there are following limitations with Spark JDBC Connector which we address in the SnappyData JDBC Extension Connector. <!---Following are some advantages of SnappyData JDBC Extension Connector over Spark JDBC Connector--->

*	**Performance**</br>When an entire query is pushed down, Spark runs two queries:
	*	First it runs the query that is supplied to fetch the result set metadata so that it knows the structure of the data frame that is returned to the application.
	*	Secondly it runs the actual query.
The SnappyData connector internally figures out the structure of the result set without having to run multiple queries.

*	**Lack of connection pooling**</br> With no built-in support for pooling connections, every time a query is executed against a JDBC database, each of the partition in Spark has to set up a new connection which can be expensive. SnappyData internally uses an efficient pooled implementation with sensible defaults.

*	**Data manipulation**</br> While the Spark DataFrameWriter API can be used to append/insert a full dataset (dataframe) into the database, it is not simple to run the ad-hoc updates on the database including mass updates. The SnappyData JDBC Extension Connector makes this much simpler. 
*	**Usability**</br> With the SnappyData JDBC Extension Connector, it is easier to deal with all the connection properties. You need not impair your application with sensitive properties dispersed in your app code.

### Connecting to SnappyData using the JDBC Extension Connector
Following is a sample of Spark JDBC Extension setup and usage:

1.	Include the **snappydata-jdbc** package in the Spark job with spark-submit or spark-shell: 

			$SPARK_HOME/bin/spark-shell --packages SnappyDataInc:snappydata-jdbc:1.0.2.1-s_2.11
  
2.	Set the session properties.</br> The SnappyData connection properties (to enable auto-configuration of JDBC URL) and credentials can be provided in Spark configuration itself, or set later in SparkSession to avoid passing them in all the method calls. These properties can also be provided in **spark-defaults.conf** along with all the other Spark properties.  You can also set any of these properties in your app code. </br>Overloads of the above methods accepting **user+password** and **host+port **is also provided in case those properties are not set in the session or needs to be overridden. You can optionally pass additional connection properties similarly as in the **DataFrameReader.jdbc **method.</br> Following is a sample code of configuring the properties in **SparkConf**:

        	$SPARK_HOME/bin/spark-shell --packages SnappyDataInc:snappydata-jdbc:1.0.2.1-s_2.11 --conf spark.snappydata.connection=localhost:1527 --conf spark.snappydata.user=<user> --conf spark.snappydata.password=<password> 
  
3.	Import the required implicits in the job/shell code as follows:

			import io.snappydata.sql.implicits._
        
## Running Queries 
Your application must import the SnappyData SQL implicits when using Scala. 

```
import io.snappydata.sql.implicits._
```

Once the required session properties are set (connection and user/password as shown above), then one can run the required queries/DMLs without any other configuration.

### Scala Query Example

```
val spark = <create/access your spark session instance here >;
val dataset = spark.snappyQuery("select x, sum(y), max(z) from largeTable group by x order by x") // query pushed down to SnappyData data cluster
```

### Java Query Example

```
import org.apache.spark.sql.*;

JdbcExecute exec = new JdbcExecute(<your SparkSession instance>);
DataFrame df = exec.snappyQuery("select x, sum(y), max(z) from largeTable group by x order by x") // query pushed down to SnappyData data cluster
```

!!!Note
	Overloads of the above methods of accepting **user+password **and **host+port** is also provided in case those properties are not set in the session or need to be overridden. You can optionally pass additional connection properties such as in **DataFrameReader.jdbc** method.

<!---(IMPORTANT: NEED A LINK TO THE API DOCS ... SUMEDH/NEERAJ ? )--->

### Updating/Writing Data in SnappyData Tables

Your application can use the Spark [DataFrameWriter](/reference/API_Reference/apireference_guide.md#dataframewriter) API to either insert or append data.

Also, for convenience, the connector provides an implicit in scala that is **import io.snappydata.sql.implicits._** for the DataFrameWriter to simplify writing to SnappyData. Hence, there is no need to explicitly set the connection properties. 

After the required session properties are set (connection and user/password as shown above), then you can fire the required queries/DMLs without any other configuration.

Inserting a dataset from the job can also use the ***snappy*** extension to avoid passing in the URL and credentials explicitly:

```
df.write.snappy(“testTable1”)  // You can use all the Spark writer APIs when using the snappy implicit. 
Or using explicit wrapper in Java: new JdbcWriter(spark.write).snappy(“testTable”)
```

### Using SQL DML to Execute Ad-hoc SQL 

You can also use the **snappyExecute** method (see below) to run the arbitrary SQL DML statements directly on the database. You need not acquire/manage explicit JDBC connections or set properties.

```

// execute DDL
spark.snappyExecute("create table testTable1 (id long, data string) using column")
// DML
spark.snappyExecute("insert into testTable1 values (1, ‘data1’)")
// bulk insert from external table in embedded mode
spark.snappyExecute("insert into testTable1 select * from externalTable1")
```

When using Java, the wrapper has to be created explicitly as shown below:

```
import org.apache.spark.sql.*;

JdbcExecute exec = new JdbcExecute(spark);
exec.snappyExecute(“create table testTable1 (id long, data string) using column”);
exec.snappyExecute("insert into testTable1 values (1, ‘data1’)");
...
```

### Comparison with Current Spark APIs

There is no equivalent of **snappyExecute** and one has to explicitly use JDBC API. For **snappyQuery**, if you were to use Spark’s JDBC connector directly, then the equivalent code would appear as follows (assuming snappyExecute equivalent was done beforehand using JDBC API or otherwise):

```
val jdbcUrl = "jdbc:snappydata:pool://localhost:1527"
val connProps = new java.util.Properties()
connProps.setProperty("driver", "io.snappydata.jdbc.ClientPoolDriver")
connProps.setProperty("user", userName)
connProps.setProperty("password", password)

val df = spark.read.jdbc(jdbcUrl, “(select count(*) from testTable1) q”, connProps)

```
Besides being more verbose, this suffers from the problem of double query execution (first query to fetch query result metadata, followed by the actual query).


### API Reference
The following extensions are used to implement the Spark JDBC Connector:

*	**SparkSession.snappyQuery()**</br>This method creates a wrapper over SparkSession and is similar to **SparkSession.sql() API**. The only difference between the both is that the entire query is pushed down to the SnappyData cluster. Hence, a query cannot have any temporary or external tables/views that are not visible in the SnappyData cluster.
*	**SparkSession.snappyExecute()**</br> Similarly, with this method, the SQL (assuming it to be a DML) is pushed to SnappyData, using the JDBC API, and returns the update count.
*	**snappy**</br> An implicit for DataFrameWriter named snappy simplifies the bulk writes. Therefore a write operation such as **session.write.jdbc()** becomes **session.write.snappy()** with the difference that JDBC URL, driver, and connection properties are auto-configured using session properties, if possible.

## Performance Considerations

It should be noted that using the Spark Data Source Writer or the Snappy Implicit are both much slower as compared to SnappyData embedded job or Smart Connector. If the DataFrame that is to be written is medium or large sized, then it is better to ingest directly in an embedded mode. In case writing an embedded job is not an option, the incoming DataFrame can be dumped to an external table in a location accessible to both Spark and SnappyData clusters. After this, it can be ingested in an embedded mode using **snappyExecute**.