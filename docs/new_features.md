# New Features

## TIBCO ComputeDB 1.2.0

TIBCO ComputeDB 1.2.0 includes the following new features:

### Support for Cloud Storage and New Data Formats
Added support for external data sources such as  HDFS, S3, Azure Blob Storage (WASB, not ADLS), and GCS. Also, tested and certified support for file formats like CSV, Parquet, XML, JSON, Avro, ORC, text. 
Apache Zeppelin that is embedded with the product, is the easiest way to explore external data sources. Refer to the notebooks under the section **External Data Sources** and **Demos with Big Datasets** for configuring and demonstrations. 

### Structured Streaming User Interface
Introducing a new UI tab to monitor Structured Streaming applications statistics and progress.

### TIBCO ComputeDB Metrics now Fully Compatible with Apache Spark 
Apache Spark provides a flexible way to capture metrics and route these to a multitude of Sinks (JMX, HTTP, etc). TIBCO ComputeDB, besides capturing metrics in its native Statistics DB, also routes all system-wide metrics to any configured Spark Sink, enabling monitoring metrics through a wide selection of external tools. 

### Data Extractor Utility
Introducing the Data Extractor utility as a recovery service in case the cluster fails to come up in some extreme circumstances. For example, when the disk files are corrupted. The utility also permits users to extract their datasets from in-memory tables to cloud storage serving as a backup mechanism. 

### Multiline JSON Parsing Support
TIBCO ComputeDB now supports multiline JSON parsing. An existing Quickstart example has been extended to illustrate multi-line JSON file support.

### Accessing Hive tables through External Hive Metastore
The facility is provided to access data stored in Hive tables by connecting to the existing Hive metastore in local and remote modes.

### ODBC Driver Support

*	Added SSL support on the ODBC driver-side.
*	Added support for various provider type IDs in the .NET framework.
