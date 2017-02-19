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
