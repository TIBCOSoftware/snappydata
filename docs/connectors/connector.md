# Working with Data Sources

TIBCO ComputeDB relies on the Spark SQL Data Sources API to parallelly load data from a wide variety of sources. Any data source or database that supports Spark to load or save state can be accessed from within TIBCO ComputeDB. 

There is built-in support for many data sources as well as data formats. 
Built-in data sources include - Amazon S3, GCS (Google Cloud storage), Azure Blob store, file systems, HDFS, Hive metastore, RDB access using JDBC, TIBCO Data Virtualization and Pivotal GemFire. 

ComputeDB supports the following data formats: CSV, Parquet, ORC, Avro, JSON, XML and Text.

You can also deploy other third party connectors using the SQL `Deploy` command. Refer [Deployment of Third Party Connectors](deployment_dependency_jar.md). You will likely find a Spark connector for your data source via the [Spark packages portal](https://spark-packages.org/) or doing a web search. 

* [START HERE - for a quick overview of the concepts and some examples for loading data](../howto/load_data_into_snappydata_tables.md)
* [Data Loading examples using Spark SQL/Data Sources API](../howto/load_data_from_external_data_stores.md)
* [Supported Data Formats](../Data/data_formats.md)
* [Accessing Cloud Stores](access_cloud_data.md)
* [Connecting to External Hive Metastores](../Data/external_hive_support.md)
* [Using the TIBCO ComputeDB Change Data Capture (CDC) Connector](cdc_connector.md)
* [Using the TIBCO ComputeDB GemFire Connector](gemfire_connector.md)

