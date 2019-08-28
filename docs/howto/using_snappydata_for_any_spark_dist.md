## How to use SnappyData for any Spark Distribution

The **snappydat-jdbc Spark** package adds extensions to Spark’s inbuilt JDBC data source provider to work better with SnappyData. This allows SnappyData to be treated as a regular JDBC data source with all versions of Spark which are greater or equal to 2.1, while also providing speed to direct SnappyData embedded cluster for many types of queries.

Following is a sample of Spark JDBC extension setup and usage: 

1. Include the **TIB_compute-jdbc** package in the Spark job with spark-submit or spark-shell:

		$SPARK_HOME/bin/spark-shell --jars snappydata-jdbc-2.11_1.1.1.jar
    
2. Set the session properties.</br>The SnappyData connection properties (to enable auto-configuration of JDBC URL) and credentials can be provided in Spark configuration itself, or set later in SparkSession to avoid passing them in all the method calls. These properties can also be provided in **spark-defaults.conf ** along with all the other Spark properties.</br> Following is a sample code of configuring the properties in **SparkConf**:

		$SPARK_HOME/bin/spark-shell --jars snappydata-jdbc-2.11_1.1.1.jar --conf spark.snappydata.connection=localhost:1527 --conf spark.snappydata.user=<user> --conf spark.snappydata.password=<password>

	Overloads of the above methods accepting *user+password* and *host+port* is also provided in case those properties are not set in the session or needs to be overridden. You can optionally pass additional connection properties similarly as in the **DataFrameReader.jdbc** method.

4.	Import the required implicits in the job/shell code as follows:

		import io.snappydata.sql.implicits._

4.	After the required session properties are set (connection and user/password etc.), you can run the queries/DMLs without any other configuration as shown here:

        // execute DDL
        spark.snappyExecute("create table testTable1 (id long, data string) using column")
        // DML
        spark.snappyExecute("insert into testTable1 values (1, ‘data1’)")
        // bulk insert from external table in embedded mode
        spark.snappyExecute("insert into testTable1 select * from externalTable1")
        // query
        val dataSet = spark.snappyQuery(“select count(*) from testTable1”)

**For Java**</br>
	When using Java, the wrapper must be created explicitly as shown in the following sample:

        import org.apache.spark.sql.*;

        JdbcExecute exec = new JdbcExecute(spark);
        exec.snappyExecute(“create table testTable1 (id long, data string) using column”);
        exec.snappyExecute("insert into testTable1 values (1, ‘data1’)");
        DataFrame df = exec.snappyQuery(...);
        ...
