# Using SnappyData for Any Spark Distribution

The **snappydata-jdbc Spark **package adds extensions to Spark’s inbuilt JDBC data source provider to work better with SnappyData. This allows SnappyData to be treated as a regular JDBC data source with all versions of Spark which are greater or equal to 2.1, while also providing speed to direct SnappyData embedded cluster for many types of queries.

The Spark JDBC connector gives the following advantages:

*	Transparent push down of queries to the SnappyData cluster such as Spark’s JDBC connection.
*	Avoids the double execution problem with the Spark's JDBC connection.
*	Provides better usability and the ability to execute DML statements using SparkSession, which is missing in Spark’s JDBC connector.
*	Compatibility with all versions of Spark which are greater or equal to version 2.1.
*	Good performance of shorter queries using the new [ClientDriver with an in-built pool implementation](/howto/connect_using_jdbc_driver.md#jdbcpooldriverconnect).

## Extensions of Spark JDBC Connector

SnappyData provides the extensions for Spark JDBC Connector in the form of Scala implicits that are wrapped into a SparkSession. For other languages such as Java and Python, you must create the wrapper explicitly.

The following extensions are used to implement the Spark JDBC Connector:

*	**SparkSession.snappyQuery()** </br>This method creates a wrapper over SparkSession and is similar to **SparkSession.sql()** API. The only difference between the both is that the entire query is pushed down to the SnappyData cluster. Hence, a query cannot have any temporary or external tables/views that are not visible in the SnappyData cluster.

*	**SparkSession.snappyExecute()** </br>Similarly with this method, the SQL (assuming it to be a DML) is pushed to SnappyData, using the JDBC API, and returns the update count.

*	**snappy**</br>An implicit for DataFrameWriter named **snappy** simplifies the bulk writes. Therefore a write operation such as **session.write.jdbc()** becomes **session.write.snappy()** with the difference that JDBC URL, driver, and connection properties are auto-configured using session properties if possible.

Refer the instructions [here](../howto/using_snappydata_for_any_spark_dist.md) to set up the Spark JDBC connector and use SnappyData with any Spark distribution.

## Spark JDBC Extension Versus Current Spark APIs

In the following points, a comparison is provided for scenarios where you use Spark JDBC Connector without the SnappyData extensions: 

*	You must use JDBC API, since there is nothing equivalent to **snappyExecute**.

*	when you use Spark’s JDBC connector directly instead of **snappyQuery**, then the equivalent code (assuming snappyExecute equivalent was done beforehand using JDBC API or otherwise) is as follows:

        val jdbcUrl = "jdbc:snappydata:pool://localhost:1527"
    	val connProps = new java.util.Properties()
    	connProps.setProperty("driver", "io.snappydata.jdbc.ClientPoolDriver")
   	 	connProps.setProperty("user", userName)
    	connProps.setProperty("password", password)

   		 val df = spark.read.jdbc(jdbcUrl, “(select count(*) from testTable1) q”, connProps)

	This code has verbosity and also presents the problem of double query execution (one with LIMIT 1 for schema, and one without) thereby increasing the execution time for many types of queries.
 
*	Inserting a Dataset from the job can also use the **snappy** extension to avoid passing in URL and credentials explicitly:

        df.write.snappy(“testTable1”)
        Or using explicit wrapper in Java: new JdbcWriter(spark.write).snappy(“testTable”)

	Using the Spark’s JDBC connector for the same is as follows:

		df.write.jdbc(jdbcUrl, “testTable1”, connProps)

	However, both of these are much slower as compared to SnappyData embedded job or smart connector. If the DataFrame to be written is medium or large sized, then it is better to ingest directly in an embedded mode.
In case writing an embedded job is not an option, the incoming DataFrame can be dumped to an external table in a location accessible to both Spark and SnappyData clusters. After this, it can be ingested in an embedded mode using **snappyExecute**. </br>The most efficient inbuilt format in Spark is Parquet, so for large DataFrames a much better option is as follows:

        df.write.parquet(“<path>”)
        spark.snappyExecute(“create external table stagingTable1 using parquet options (path ‘<path>’)”)
        spark.snappyExecute(“insert into testTable1 select * from stagingTable1”)
        spark.snappyExecute(“drop table stagingTable1”)
        // delete staging <path> if required e.g using hadoop API
        // FileSystem.get(sc.hadoopConfiguration).delete(new Path(“<path>”), true)

*	Similarly, you can address the lack of **putInto** or **deleteFrom** APIs in Spark. For this, you must dump the incoming DataFrames into a temporary staging area. Then create external tables in an embedded cluster to point to those DataFrames. After this, invoke the ** PUT INTO / DELETE FROM** SQL in the embedded cluster by using **snappyExecute** in the Spark cluster. The **PUT INTO** flow is similar to the **insert** example that is shown above.
       
       df.write.parquet(“<path>”)
            spark.snappyExecute(“create external table stagingTable1 using parquet options (path ‘<path>’)”)
            spark.snappyExecute(“put into testTable1 select * from stagingTable1”)
            spark.snappyExecute(“drop table stagingTable1”)
            // delete staging <path> if required e.g using hadoop API
            // FileSystem.get(sc.hadoopConfiguration).delete(new Path(“<path>”), true)

	**DELETE FROM **is also identical except that you must rename the key columns to match those in the target table before doing the operation. </br>Alternatively you can alias the key column names, if different, when selecting from the temporary staging table (in third step below).

            df.selectExpr(“keyColumn1 as …”).write.parquet(“<path>”)
            spark.snappyExecute(“create external table stagingTable1 using parquet options (path ‘<path>’)”)
            spark.snappyExecute(“delete from testTable1 select * from stagingTable1”)
            spark.snappyExecute(“drop table stagingTable1”)
            // delete staging <path> if required


