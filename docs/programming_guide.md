# Programming Guide

SnappyData bundles Spark and supports all the Spark APIs. You can create Object based RDDs and run transformations or use the higher level APIs (like Spark ML). 
All SnappyData managed tables are also accessible as DataFrame and the API extends Spark classes like SQLContext and DataFrames.</br>
It is therefore recommended that you understand the [concepts in SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) 
and the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes). You can also store and manage arbitrary RDDs (or even Spark DataSets) through the implicit or explicit transformation to a DataFrame. While the complete SQL support is still evolving, the supported SQL is much richer than SparkSQL. The extension SQL supported by the SnappyStore can be referenced [here](/sql_reference.md).

In Spark SQL, all tables are temporary and cannot be shared across different applications. While you can manage such temporary tables, SnappyData tables are automatically registered to a built-in persistent catalog. This is similar to how Spark SQL uses the Hive catalog to natively work with Hive clusters.
Data in tables is primarily managed in-memory with one or more consistent copies across machines or racks, but it can also be reliably managed on disk.

The following topics are covered in this section:

* [SparkSession, SnappySession and SnappyStreamingContext](programming_guide/sparksession_snappysession_and_snappystreamingcontext.md)

* [SnappyData Jobs](programming_guide/snappydata_jobs.md)

* [Managing JAR Files](programming_guide/managing_jar_files.md)

* [Using SnappyData Shell](programming_guide/using_snappydata_shell.md)

* [Using the Spark Shell and spark-submit](programming_guide/using_the_spark_shell_and_spark-submit.md)

* [Working with Hadoop YARN Cluster Manager](programming_guide/working_with_hadoop_yarn_cluster_manager.md)

* [Using JDBC with SnappyData](programming_guide/using_jdbc_with_snappydata.md)

* [Multiple Language Binding using Thrift Protocol](programming_guide/multiple_language_binding_using_thrift_protocol.md)

* [Building SnappyData Applications using Spark API](programming_guide/building_snappydata_applications_using_spark_api.md)

* [Tables in SnappyData](programming_guide/tables_in_snappydata.md)

* [Stream Processing using SQL](programming_guide/stream_processing_using_sql.md)

* [User Defined Functions (UDF) and User Defined Aggregate Functions (UDAF)](programming_guide/udf_and_udaf.md)








