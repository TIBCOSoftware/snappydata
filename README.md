### SnappyData (aka TIBCO ComputeDB community edition), an distributed, in-memory optimized analytics database delivers high throughput, low latency, and high concurrency for unified analytic workloads.
By fusing a in-memory hybrid database inside Apache Spark it provides analytic query processing, mutability/transactions, access to virtually all big data sources and stream processing all in one "Unified" cluster. 

Its primary use case is to provide analytics at interactive speeds over large volumes of data with minimal or no pre-processing of the dataset. For instance, often there is no need to pre-aggregate/reduce or generate cubes over your large data sets for ad-hoc visual analytics. This is made possible by smartly managing data in-memory, dynamically generating code using vectorization optimizations and maximizing the potential of modern multi-core CPUs. It enables complex processing on large data sets in sub-second timeframes. 

![SnappyData Positioning](docs/Images/Snappy_intro.1.png)

**Note:** SnappyData is not meant to be yet another EDW platform, but, rather a nimble computational cluster that augments traditional EDWs and data lakes.  


### Important Capabilities

<TODO> - we could shorten the description of each capability and retain the description in the new concepts chapter later. 

> **Easily discover and catalog big data sets:** You can connect and discover datasets in SQL DBs, hadoop, NoSQL stores, file systems or even cloud data stores like S3 easily using SQL, infer schemas automatically and register them in a secure catalog. A wide variety of data formats are supported out of the box - JSON, CSV, text, Objects, Parquet, ORC, SQL, XML and more. 

> **Rich connectivity:** As ComputeDB is built with Apache Spark inside, any data store that has a Spark connector (and virtually all modern stores do (link to spark-packages.org) ) can be accessed using SQL or using the Spark RDD/Dataset API. You can dynamically deploy connectors to a running computeDB cluster. 

> **Virtual or in-memory data:** you can decide which datasets need to be provisioned into distributed memory or left at the source. When data is left at source (modeled as a virtual/external tables) the analytic query processing is completely parallelized, query fragments pushed down where possible and executed at high speed. 
When speed is important, applications can selectively copy the external data into memory using a single SQL command. 

> **In-memory columnar + Row store:** You can choose in-memory data to be stored in columnar form - compressed and designed for scanning/aggregating large data sets or in row oriented form - very fast key or highly selective access. The columnar store is automatically indexed using a skipping index and applications can explicitly add indexes for the row store. 

> **High performance:** When data is loaded, the engine parallelizes all access by carefully taking into account - the available distributed cores, whether the source data can be partitioned and the available memory, to deliver very high speed loading. So, unlike a traditional warehouse, you can just bring up a ComputeDB just when required, load, process and tear it down. Query processing used code generation, and vectorization techniques to shift the processing to the modern day multi-core processor and L1/L2/L3 caches to the extent possible. 

> **Flexible, rich data transformations:** External data sets when discovered automatically through schema inference will have the schema of the source. Users can cleanse, blend, reshape data using a SQL function library (Spark SQL+) or even submit Spark jobs and use custom logic. The entire, rich Spark API is at your disposal. This logic can be written in SQL, Java, Scala or even Python.

> **Preparing data for data science:** Through the use Spark API for statistics and machine learning raw or curated datasets can be prepared for machine learning very easily. You can understand the statistical characteristics - correlations, independence of different variables, etc. You can generate distributed feature vectors from your data - using things like one-hot encoder, binarizer, and a range of functions built into Spark ML library. These features can be stored back into column tables and shared across a group of users with security and avoid having to dump copies to disk (slow and error prone).
 
> **Stream ingestion and liveness:** While it is common to see query service engines today, most resort to periodic refreshing of data sets from the source as the managed data cannot be mutated. E.g. query engines like Presto, HDFS formats like parquet, etc. And, when updates can be applied pre-processing, re-shaping of the data is not necessarily simple. 
In ComputeDB, operational systems can feed updates to data through Kafka to ComputeDB. The incoming data can be CDC events (i.e. insert, updates or deletes) and can be easily ingested into respective in-memory tables with ease, consistency and exactly-once semantics. The Application can apply smart logic to reduce incoming streams, apply transformations, etc using Spark structured streaming APIs. 

> **Approximate Query Processing(AQP):** When dealing with very large data sets (IoT sensor streaming time series data, for instance) it may not be possible to provision the data in-memory and if left at the source (say hadoop or S3) your analytic query processing will likely take too long. In ComputeDB, you can create one or more stratified data samples on the full data set. These samples will automatically be used by the query engine for aggregation queries and a nearly accurate answer returned to clients. This can be immensely valuable when visualizing a trend, plotting a graph or bar chart, etc.  

> **Access from anywhere:** You can use JDBC, ODBC, REST or any of the Spark API. The product is fully compatible with Spark 2.1.1. ComputeDB natively supports modern visualization tools - TIBCO Spotfire, Tableau and Qlikview. 

----
BELOW THE FLOW SHOULD BE ....
... download and install
... getting started in 10 mins
... docs
... blog
... community  (This should be there in the main docs too)
... Link with snappydata

Add the following:
Early published papers highlighting the innovations and motivations
-- link to the CIDR paper
 -- link to the Sigmod paper

Also add:
-- Performance comparisons to Apache Spark, etc (blog links)

Then retain the section ... delta with spark ... adAnalytics, etc

----



If you are already using Spark, experience 20x speed up for your query performance. Try out this [test](https://github.com/SnappyDataInc/snappydata/blob/master/examples/quickstart/scripts/Quickstart.scala).

## Getting Started
We provide multiple options to get going with SnappyData. The easiest option is, if you are already using Spark 2.1.1.
You can simply get started by adding SnappyData as a package dependency. You can find more information on options for running SnappyData [here](docs/quickstart.md).

## Downloading and Installing SnappyData
You can download and install the latest version of SnappyData from the [SnappyData Download Page](https://www.snappydata.io/download).
Refer to the [documentation](docs/install.md) for installation steps.

If you would like to build SnappyData from source, refer to the [documentation on building from source](docs/install/building_from_source.md).

## SnappyData in 5 Minutes!
Refer to the [5 minutes guide](docs/quickstart.md) which is intended for both first time and experienced SnappyData users. It provides you with references and common examples to help you get started quickly!

## Documentation
To understand SnappyData and its features refer to the [documentation](http://snappydatainc.github.io/snappydata/).

## Community Support

We monitor channels listed below for comments/questions.

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [Mailing List](https://groups.google.com/forum/#!forum/snappydata-user) ![Mailing List](http://i.imgur.com/YomdH4s.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          [JIRA](https://jira.snappydata.io/projects/SNAP/issues) ![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData Distribution

**Using Maven Dependency**

SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:

```
groupId: io.snappydata
artifactId: snappydata-core_2.11
version: 1.1.0

groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 1.1.0
```

**Using SBT Dependency**

If you are using SBT, add this line to your **build.sbt** for core SnappyData artifacts:

`libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "1.1.0"`

For additions related to SnappyData cluster, use:

`libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.1.0"`

You can find more specific SnappyData artifacts [here](http://mvnrepository.com/artifact/io.snappydata)

**Note:** If your project fails when resolving the above dependency (that is, it fails to download javax.ws.rs#javax.ws.rs-api;2.1), it may be due an issue with its pom file. </br> As a workaround, you can add the below code to your **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).


## What is the delta between SnappyData and Apache Spark ? 

Apache Spark is a general purpose parallel computational engine for analytics at scale. At its core, it has a batch design center and is capable of working with disparate data sources. While this provides rich unified access to data, this can also be quite inefficient and expensive. Analytic processing requires massive data sets to be repeatedly copied and data to be reformatted to suit Spark. In many cases, it ultimately fails to deliver the promise of interactive analytic performance.
For instance, each time an aggregation is run on a large Cassandra table, it necessitates streaming the entire table into Spark to do the aggregation. Caching within Spark is immutable and results in stale insight.

### The SnappyData Approach

##### Snappy Architecture
![SnappyData Architecture](docs/Images/SnappyArchitecture.png)

At SnappyData, we take a very different approach. SnappyData fuses a low latency, highly available in-memory transactional database (GemFireXD) into Spark with shared memory management and optimizations. Data in the highly available in-memory store is laid out using the same columnar format as Spark (Tungsten). All query engine operators are significantly more optimized through better vectorization and code generation. </br>
The net effect is, an order of magnitude performance improvement when compared to native Spark caching, and more than two orders of magnitude better Spark performance when working with external data sources.

Essentially, we turn Spark into an in-memory operational database capable of transactions, point reads, writes, working with Streams (Spark) and running analytic SQL queries. Or, it is an in-memory scale out Hybrid Database that can execute Spark code, SQL or even Objects.


## A Streaming example - Ad Analytics
Here is a stream + Transactions + Analytics use case example to illustrate the SQL as well as the Spark programming approaches in SnappyData - [Ad Analytics code example](https://github.com/SnappyDataInc/snappy-poc). Here is a [screencast](https://www.youtube.com/watch?v=bXofwFtmHjE) that showcases many useful features of SnappyData. The example also goes through a benchmark comparing SnappyData to a Hybrid in-memory database and Cassandra.

## Contributing to SnappyData

If you are interested in contributing, please visit the [community page](http://www.snappydata.io/community) for ways in which you can help.

