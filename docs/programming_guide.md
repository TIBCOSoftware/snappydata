# Overview
SnappyData bundles Spark and supports all the Spark APIs. You can create Object based RDDs and run transformations or use the higher level APIs (like Spark ML). 
All SnappyData managed tables are also accessible as DataFrame and the API extends Spark classes like SQLContext and DataFrames.  
We therefore recommend that you understand the [concepts in SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) 
and the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes). You can also store and manage arbitrary 
RDDs (or even Spark DataSets) through implicit or explicit transformation to a DataFrame. While, the complete SQL support is still 
evolving, the supported SQL is much richer than SparkSQL. The extension SQL supported by the SnappyStore can be referenced [here](#markdown_link_row_and_column_tables).

In Spark SQL, all tables are temporary and cannot be shared across different applications. While you can manage such temporary tables, SnappyData tables are automatically registered 
to a built-in persistent catalog. This is similar to how Spark SQL uses the Hive catalog to natively work with Hive clusters. 
Data in tables is primarily managed in-memory with one or more consistent copies across machines or racks, but it can also be reliably managed on disk.

The following topics are covered in this section:

* [SnappySession and SnappyStreamingContext](pgm_guide/snappysession_and_snappystreamingcontext.md)

* [SnappyData Jobs](pgm_guide/snappydata_jobs.md)

* [Managing JAR Files](pgm_guide/managing_jar_files.md)

* [Using SnappyData Shell](pgm_guide/using_snappydata_shell.md)

* [Using the Spark Shell and spark-submit](pgm_guide/spark_shell_spark_submit.md)

* [Using JDBC with SnappyData](pgm_guide/using_jdbc.md)

* [Multiple Language Binding using Thrift Protocol](pgm_guide/multiple_language_using_thrift.md)

* [Building SnappyData Applications using Spark API](pgm_guide/building_apps_spark_api.md)

* [Tables in SnappyData](pgm_guide/tables_in_snappydata.md)

* [Stream processing using SQL](pgm_guide/stream_processing_using_sql.md)

* [User Defined Functions (UDF) and User Defined Aggregate Functions (UDAF)](pgm_guide/user_defined_function.md)