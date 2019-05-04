# Working with External Data Sources

TIBCO ComputeDB relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. Any data source or database that supports Spark to load or save state can be accessed from within TIBCO ComputeDB. 

There is built-in support for many data sources as well as data formats. You can access data from sources such as S3, file system, HDFS, Hive, and RDB. The loaders have built-in support to handle data formats such as CSV, Parquet, ORC, Avro, JSON, and Java/Scala Objects.

!!!Attention
	This section currently only details the advanced connectors that TIBCO ComputeDB introduced. Refer to the [howto](../howto.md) section for a brief description about working with [external data sources](../howto/load_data_into_snappydata_tables.md) and [some examples](../howto/load_data_from_external_data_stores.md). 

TIBCO ComputeDB provides a utility to deploy third-party connectors using the SQL `Deploy` command. Refer [Deployment of Third Party Connectors](/connectors/deployment_dependency_jar.md)

For more information see:

* [START HERE - How to load data into TIBCO ComputeDB Tables](../howto/load_data_into_snappydata_tables.md)
* [Data Loading examples using Spark SQL/Data Sources API](../howto/load_data_from_external_data_stores.md)
* [Using the TIBCO ComputeDB Change Data Capture (CDC) Connector](cdc_connector.md)
* [Using the TIBCO ComputeDB GemFire Connector](gemfire_connector.md)
