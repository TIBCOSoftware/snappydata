## Introduction 
SnappyData (aka TIBCO ComputeDB community edition) is a distributed, in-memory optimized analytics database. SnappyData delivers high throughput, low latency, and high concurrency for unified analytics workload. By fusing an in-memory hybrid database inside Apache Spark, it provides analytic query processing, mutability/transactions, access to virtually all big data sources and stream processing all in one unified cluster.

One common use case for SnappyData is to provide analytics at interactive speeds over large volumes of data with minimal or no pre-processing of the dataset. For instance, there is no need to often pre-aggregate/reduce or generate cubes over your large data sets for ad-hoc visual analytics. This is made possible by smartly managing data in-memory, dynamically generating code using vectorization optimizations and maximizing the potential of modern multi-core CPUs.
SnappyData enables complex processing on large data sets in sub-second timeframes. 

![SnappyData Positioning](./Images/Snappy_intro.1.png)

!!!Note
	*SnappyData is not another Enterprise Data Warehouse (EDW) platform, but rather a high performance computational and caching cluster that augments traditional EDWs and data lakes.*

### Important Capabilities

*	**Easily discover and catalog big data sets**</br>
	You can connect and discover datasets in SQL DBs, Hadoop, NoSQL stores, file systems, or even cloud data stores such as S3 by using SQL, infer schemas automatically and register them in a secure catalog. A wide variety of data formats are supported out of the box such as JSON, CSV, text, Objects, Parquet, ORC, SQL, XML, and more.

*	**Rich connectivity**</br>
	SnappyData is built with Apache Spark inside. Therefore, any data store that has an Apache Spark connector can be accessed using SQL or by using the Apache Spark RDD/Dataset API. Virtually all modern data stores do have Apache Spark connector. See [Apache Spark Packages](https://spark-packages.org/). You can also dynamically deploy connectors to a running SnappyData cluster.

*	**Virtual or in-memory data**</br>
	You can decide which datasets need to be provisioned into distributed memory or left at the source. When the data is left at source, after being modeled as a virtual/external tables, the analytic query processing is parallelized, and the query fragments are pushed down wherever possible and executed at high speed.
When speed is essential, applications can selectively copy the external data into memory using a single SQL command.

*	**In-memory Columnar + Row store** </br>
	You can choose in-memory data to be stored in any of the following forms:
    *	**Columnar**: The form that is compressed and designed for scanning/aggregating large data sets.
    *	**Row store**: The form that has an extremely fast key access or highly selective access.
	The columnar store is automatically indexed using a skipping index. Applications can explicitly add indexes for the row store.

*	**High performance** </br>
	When data is loaded, the engine parallelizes all the accesses by carefully taking into account the available distributed cores, the available memory, and whether the source data can be partitioned to deliver extremely high-speed loading. Therefore, unlike a traditional warehouse, you can bring up SnappyData whenever required, load, process, and tear it down. Query processing uses code generation and vectorization techniques to shift the processing to the modern-day multi-core processor and L1/L2/L3 caches to the possible extent.

*	**Flexible rich data transformations** </br>
	External data sets when discovered automatically through schema inference will have the schema of the source. Users can cleanse, blend, reshape data using a SQL function library (Apache Spark SQL+) or even submit Apache Spark jobs and use custom logic. The entire rich Apache Spark API is at your disposal. This logic can be written in SQL, Java, Scala, or even Python.*

*	**Prepares data for data science**</br> 
	Through the use of apache Apache Spark API for statistics and machine learning, raw or curated datasets can be easily prepared for machine learning. You can understand the statistical characteristics such as correlation, independence of different variables and so on. You can generate distributed feature vectors from your data that is by using processes such as one-hot encoder, binarizer, and a range of functions built into the Apache Spark ML library. These features can be stored back into column tables and shared across a group of users with security and avoid dumping copies to disk, which is slow and error-prone.
 
*	**Stream ingestion and liveness** </br>
	While it is common to see query service engines today, most resort to periodic refreshing of data sets from the source as the managed data cannot be mutated — for example query engines such as Presto, HDFS formats like parquet, etc. Moreover, when updates can be applied pre-processing, re-shaping of the data is not necessarily simple. 
   In SnappyData, operational systems can feed data updates through Kafka to SnappyData. The incoming data can be CDC(Change-data-capture) events (insert, updates, or deletes) and can be easily ingested into in-memory tables with ease, consistency, and exactly-once semantics. The Application can apply custom logic to do sophisticated transformations and get the data ready for analytics. This incremental and continuous process is far more efficient than batch refreshes. Refer [Stream Processing with SnappyData](howto/use_stream_processing_with_snappydata.md) </br>  

*	**Approximate Query Processing(AQP)** </br>
	When dealing with huge data sets, for example, IoT sensor streaming time-series data, it may not be possible to provision the data in-memory, and if left at the source (say Hadoop or S3) your analytic query processing can take too long. In SnappyData, you can create one or more stratified data samples on the full data set. The query engine automatically uses these samples for aggregation queries, and a nearly accurate answer returned to clients. This can be immensely valuable when visualizing a trend, plotting a graph or bar chart. Refer [AQP](aqp.md)

*	**Access from anywhere** </br>
	You can use JDBC, ODBC, REST, or any of the Apache Spark APIs. The product is fully compatible with Apache Spark 2.1.1. SnappyData natively supports modern visualization tools such as [TIBCO Spotfire](howto/connecttibcospotfire.md), [Tableau](howto/tableauconnect.md), and [Qlikview](setting_up_jdbc_driver_qlikview.md). Refer 


## Downloading and Installing SnappyData
You can download and install the latest version of SnappyData from [github](https://github.com/SnappyDataInc/snappydata/releases) or you can download the enterprise version that is TIBCO ComputeDB from [here](https://edelivery.tibco.com/storefront/index.ep).
Refer to the [documentation](/install.md) for installation steps.

## Getting Started
Multiple options are provided to get started with SnappyData. Easiest way to get going with SnappyData is on your laptop. You can also use any of the following options:

*	On-premise clusters

*	AWS

*	Docker
*	Kubernetes

You can find more information on options for running SnappyData [here](/quickstart.md).

## Quick Test to Measure Performance of SnappyData vs Apache Spark

If you are already using Apache Spark, you can experience upto 20x speedup for your query performance with SnappyData. Try this [test](https://github.com/SnappyDataInc/snappydata/blob/master/examples/quickstart/scripts/Quickstart.scala) using the Spark Shell.

## Documentation
To understand SnappyData and its features refer to the [documentation](http://snappydatainc.github.io/snappydata/).

### Other Relevant content
- [Paper](http://cidrdb.org/cidr2017/papers/p28-mozafari-cidr17.pdf) on Snappydata (Community Edition of TIBCO ComputeDB) at Conference on Innovative Data Systems Research (CIDR) - Info on key concepts and motivating problems.
- [Another early Paper](https://www.snappydata.io/snappy-industrial) that focuses on overall architecture, use cases, and benchmarks. ACM Sigmod 2016.
- [TPC-H benchmark](https://www.snappydata.io/whitepapers/snappydata-tpch) comparing Apache Spark with SnappyData
- Checkout the [SnappyData blog](https://www.snappydata.io/blog) for developer content
-	[TIBCO community page](https://community.tibco.com/products/tibco-computedb) for the latest info.

## Community Support

We monitor the following channels comments/questions:

*	[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata)![Stackoverflow](http://i.imgur.com/LPIdp12.png)

*	[Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)

*	[Gitter](https://gitter.im/SnappyDataInc/snappydata)![Gitter](http://i.imgur.com/jNAJeOn.jpg)

*	[Mailing List](https://groups.google.com/forum/#!forum/snappydata-user)![Mailing List](http://i.imgur.com/YomdH4s.png)

*	[Reddit](https://www.reddit.com/r/snappydata)![Reddit](http://i.imgur.com/AB3cVtj.png)          

*	[JIRA](https://jira.snappydata.io/projects/SNAP/issues)![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData Distribution

### Using Maven Dependency

SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:

```
groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 1.1.1
```

### Using SBT Dependency

If you are using SBT, add this line to your **build.sbt** for core SnappyData artifacts:

```
libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "1.1.1"
```

For additions related to SnappyData cluster, use:

```
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.1.1"
```

You can find more specific SnappyData artifacts [here](http://mvnrepository.com/artifact/io.snappydata)

!!!Note
	If your project fails when resolving the above dependency (that is, it fails to download `javax.ws.rs#javax.ws.rs-api;2.1`), it may be due an issue with its pom file. </br> As a workaround, you can add the below code to your **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).


## Building from Source
If you would like to build SnappyData from source, refer to the [documentation on building from source](/install/building_from_source.md).


## How is SnappyData Different than Apache Spark?

Apache Spark is a general purpose parallel computational engine for analytics at scale. At its core, it has a batch design center and is capable of working with disparate data sources. While this provides rich unified access to data, this can also be quite inefficient and expensive. Analytic processing requires massive data sets to be repeatedly copied and data to be reformatted to suit Apache Spark. In many cases, it ultimately fails to deliver the promise of interactive analytic performance.
For instance, each time an aggregation is run on a large Cassandra table, it necessitates streaming the entire table into Apache Spark to do the aggregation. Caching within Apache Spark is immutable and results in stale insight.

### The SnappyData Approach

##### Snappy Architecture

![SnappyData Architecture](./Images/SnappyArchitecture.png)

SnappyData takes a different approach. SnappyData fuses a low latency, highly available in-memory transactional database ((Pivotal GemFire/Apache Geode) into Apache Spark with shared memory management and optimizations. Data can be managed in columnar form similar to Apache Spark caching or in a row oriented manner commonly used in popular relational databases like postgres). But, many query engine operators are significantly more optimized through better vectorization, code generation and indexing. </br>
The net effect is, an order of magnitude performance improvement when compared to native Apache Spark caching, and more than two orders of magnitude better performance when Apache Spark is used in conjunction with external data sources.
Apache Spark is turned into an in-memory operational database capable of transactions, point reads, writes, working with Streams (Apache Spark) and running analytic SQL queries without losing the computational richness in Apache Spark.


## Streaming Example - Ad Analytics
Here is a stream + Transactions + Analytics use case example to illustrate the SQL as well as the Apache Spark programming approaches in SnappyData - [Ad Analytics code example](https://github.com/SnappyDataInc/snappy-poc). Here is a [screencast](https://www.youtube.com/watch?v=bXofwFtmHjE) that showcases many useful features of SnappyData. The example also goes through a benchmark comparing SnappyData to a Hybrid in-memory database and Cassandra.

## Contributing to SnappyData

If you are interested in contributing, please visit the [community page](http://www.snappydata.io/community) for ways in which you can help.

