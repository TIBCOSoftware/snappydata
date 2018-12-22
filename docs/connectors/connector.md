# Working with External Data Sources

SnappyData relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. Any data source or database that supports Spark to load or save state can be accessed from within SnappyData. 

There is built-in support for many data sources as well as data formats. Data can be accessed from S3, file system, HDFS, Hive, RDB, etc. And the loaders have built-in support to handle CSV, Parquet, ORC, Avro, JSON, Java/Scala Objects, etc as the data formats.

!!!Attention
	This section currently only details the advanced connectors that SnappyData introduced. Please refer to the [howto](../howto.md) section for a brief description about working with [external data sources](../howto/load_data_into_snappydata_tables.md) and [some examples](../howto/load_data_from_external_data_stores.md). 

SnappyData 1.0.2.1 provides a utility to deploy third party connectors using the SQL 'Deploy' command. Refer [Deployment of Third Party Connectors](/connectors/deployment_dependency_jar.md)

For more information see:

* [START HERE - How to load data into SnappyData Tables](../howto/load_data_into_snappydata_tables.md)
* [Data Loading examples using Spark SQL/Data Sources API](../howto/load_data_from_external_data_stores.md)
* [Using the SnappyData Change Data Capture (CDC) Connector](cdc_connector.md)
* [Using the SnappyData GemFire Connector](gemfire_connector.md)
