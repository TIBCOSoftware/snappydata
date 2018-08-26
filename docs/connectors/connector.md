# Using Data Source Connectors

SnappyData relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. Any data source or database that supports Spark to load or save state can be accessed from within SnappyData. 

There is built-in support for many data sources as well as data formats. Data can be accessed from S3, file system, HDFS, Hive, RDB, etc. And the loaders have built-in support to handle CSV, Parquet, ORC, Avro, JSON, Java/Scala Objects, etc as the data formats.

## Note
This section currently only details the advanced connectors that SnappyData introduced. Please refer to the [howto](../how-to) section for a terse description for working with [external data sources](../howto/load_data_into_snappydata_tables.md) and some examples [here](../howto/load_data_from_external_data_stores.md). 

## Note
SnappyData 1.0.2 also introduces a utility to deploy third party connectors using the SQL 'Deploy' command. Documentation and examples for how to use this command will soon follow. 


For more information see:
* [START HERE - How to load data into SnappyData Tables](../howto/load_data_into_snappydata_tables.md)
* [Data Loading examples using Spark SQL/Data Sources API](../howto/load_data_from_external_data_stores.md)
* [Using the SnappyData Change Data Capture (CDC) Connector](cdc_connector.md)
* [Using the SnappyData GemFire Connector](gemfire_connector.md)
