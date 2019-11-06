# How to Import Data from Hive Table into TIBCO ComputeDB Table

When working with Hive, you can instantiate Snappy session with Hive support, including connectivity to a persistent Apache Hive metastore, support for Apache Hive SerDes, and Apache Hive user-defined functions. To instantiate the Snappy session with Hive support, you must do the following:

If the underlying storage for Apache Hive is HDFS, you can configure Apache Hive.  For achieving this, do the following:

1.	Place **hive-site.xml**file in the **conf/** folder. 
           
2. In the **hive-site.xml**, you must configure the parameters as per your requirement. With Derby as Metastore, the following are the **hive-site.xml** configurations:

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
	TIBCO ComputeDB is tested with default Apache Hive database, that is embedded Derby database.

Run the following steps to access Hive tables from TIBCO ComputeDB using Apache Hive Metastore:

1.	Start the Hadoop daemons.
2.	Start the Snappy Shell. 
3.	After starting the Snappy Shell, you can do the following:

	*	To point to external hive catalog from snappy session, set the following property. This property can be set at the session level and global level.
                
                snappy-sql> set spark.sql.catalogImplementation=hive;
        
	*	To point to Snappy internal catalog from snappy session.
     	        
                snappy-sql> set spark.sql.catalogImplementation=in-memory;

	*	To access hive tables use the following command:
        		
                snappy-sql> show tables in default;
                
        It is mandatory to specify the schema **default** if any other schema is not created.
        If any other schema is created then use the created schema name.
        For example, if schema / database hiveDB created then use,
                snappy-sql> show tables in hiveDB;

	*	To read the hive tables from snappy:
        		
                snappy-sql> SELECT FirstName, LastName FROM default.hiveemployees ORDER BY LastName;

	*	To join Snappy tables and Hive tables:
            
            	snappy-sql> SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID, o.OrderDate FROM default.hive_employees emp JOIN snappy_orders o ON (emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderDate;

	*	To create Hive tables and insert the data into it from Snappy:
                
                snappy-sql> create table if not exists default.t1(id int) row format delimited fields terminated by ',';
                snappy-sql> insert into default.t1 select id, concat(id) from range(100);

In case you start the Hadoop/Hive daemons, with incorrect configurations in **hive-site.xml** file, the following error is shown.

```
No Datastore found in the Distributed System for 'execution on remote node null'.
```
If you have connected to Apache Hive/Hadoop and in case the configuration files get removed or deleted, errors or exceptions are not shown. Moreover, you cannot perform any DDL and DML operations in Hive.

For more details, refer the following links:

*	[https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)

*	[https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started)

<!---
## Option 1

If Hive tables have data stored in Apache Parquet format or Optimized Row Columnar (ORC) format the data can be copied directly into TIBCO ComputeDB tables.

For example,
```pre
CREATE EXTERNAL TABLE <hive_external_table_name> USING parquet OPTIONS(path path-to-parquet-or-orc)

CREATE TABLE <table_name> USING COLUMN AS (select * from hive_external_table_name)
```
For more information on creating an external table, refer to [CREATE EXTERNAL TABLE](../reference/sql_reference/create-external-table.md).

## Option 2

Take the RDD[Row] from Dataset of Hive Table and insert it into column table.

For example,
```pre
val ds = spark.table("Hive_Table_Name")
val df = snappy.createDataFrame(ds.rdd, ds.schema)
df.write.format("column").saveAsTable("Snappy_Table_Name")
```

In above example, 'spark' is of type SparkSession and 'snappy' is of type SnappySession.
--->

