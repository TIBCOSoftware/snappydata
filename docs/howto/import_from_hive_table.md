# How to Import Data from Hive Table into SnappyData Table


Using a Snappy session, you can read an existing hive tables that are defined in an external hive catalog, use hive tables as external tables from SnappySession for queries, including joins with tables defined in SnappyData catalog,  and also define new Hive table or view to be stored in external hive catalog. Using a SnappySession, you can also define a new hive table or view to be stored in an external hive catalog.

When working with Hive, one must instantiate Snappy session with Hive support, including connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions.

If the underlying storage for Hive is HDFS, you can configure Hive with Snappy session. For this, you must place **hive-site.xml**, **core-site.xml** (for security configuration) and **hdfs-site.xml**(for HDFS configuration) files in the **conf/** folder of Snappy. In addition to this, you must configure **spark-env.sh** file into the **conf/** folder.

The content in the **hadoop_spark-env.sh** file should be as follows:  
```
export SPARK_DIST_CLASSPATH=$(/home/user/hadoop-2.7.3/bin/hadoop classpath)
```

Snappy has been tested with default hive database i.e. embedded derby database. User can also use and configure the remote metastore as well like SQL.

In the **hive-site** xml, you must configure the parameters as per the requirement. With derby as database, the following are the ** hive-site.xml** configuration:

```
<property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
    <description>
      JDBC connect string for a JDBC metastore.
      To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection        URL.
      For example, jdbc:postgresql://myhost/db?ssl=true for postgres database.
    </description>
</property>
```

If you want to setup remote meta store instead of using default database derby, you can use the following configuration:

```
<property>
    <name>hive.metastore.uris</name>
    <value>thrift:/hostname:9083</value>
    <description>Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore.</description>
</property>

```

Run the following steps to test Snappy with Apache Hadoop:

1.	Start the Hadoop daemons.
2.	Start the Hive thrift server.
3.	Start the Snappy-shell. 
4.	After starting the Snappy Shell, you can do the following:

        # To point to external hive catalog from snappy session, set the below property.
        set spark.sql.catalogImplementation=hive.
        snappy-sql> set spark.sql.catalogImplementation=hive;
        This property can be set at the session level and global level.

        # To point to Snappy internal catalog from snappy session.
        set spark.sql.catalogImplementation=in-memory.
        snappy-sql> set spark.sql.catalogImplementation=in-memory;

        # To  access hive tables use below command.
        snappy-sql> show tables in default;
        Please note that it is mandatory to specify the schema ‘default’ if any other schema is not created.
        If any other schema is created then it is mandatory to use the created schema name.

        For example, if schema / database hiveDB created then use,
        snappy-sql> show tables in hiveDB;

        # To read the hive tables from snappy.
        snappy-sql> SELECT FirstName, LastName FROM default.hiveemployees ORDER BY LastName;

        # To join Snappy tables and Hive tables.
        snappy-sql> SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM default.hive_employees emp JOIN snappy_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate;

        # To create the hive table and insert the data into it from Snappy.
        snappy-sql> create table if not exists default.t1(id int) row format delimited fields terminated by ',';
        snappy-sql> insert into default.t1 select id, concat(id) from range(100);

If you have not configure any of the configuration files mentioned above( hive-site.xml, core-site.xml, hdfs-site.xml) and started the Hadoop and Hive daemons, you will see the following error in SnappyData:

```
No Datastore found in the Distributed System for 'execution on remote node null'.
```
!!! Note
	You must configure the configuration files **hive-site.xml** and **core-site.xml** as per your requirements.

If you have connected to Hive and Hadoop and in case the configuration files get removed or deleted, errors or exceptions will not be shown. Therefore, you cannot perform any DDL and DML operations in Hive.

For more details, refer the following links:

*	[https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)

*	[https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started)

<!---
**Option 1** 

If Hive tables have data stored in Apache Parquet format or Optimized Row Columnar (ORC) format the data can be copied directly into SnappyData tables.

For example,
```pre
CREATE EXTERNAL TABLE <hive_external_table_name> USING parquet OPTIONS(path path-to-parquet-or-orc)

CREATE TABLE <table_name> USING COLUMN AS (select * from hive_external_table_name)
```
For more information on creating an external table, refer to [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table.md).

**Option 2**

Take the RDD[Row] from Dataset of Hive Table and insert it into column table.

For example,
```pre
val ds = spark.table("Hive_Table_Name")
val df = snappy.createDataFrame(ds.rdd, ds.schema)
df.write.format("column").saveAsTable("Snappy_Table_Name")
```

In above example, 'spark' is of type SparkSession and 'snappy' is of type SnappySession.
-->
