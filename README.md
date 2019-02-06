### SnappyData fuses Apache Spark with an in-memory database to deliver a data engine capable of processing streams, transactions and interactive analytics in a single cluster.

### The Challenge with Spark and Remote Data Sources
Apache Spark is a general purpose parallel computational engine for analytics at scale. At its core, it has a batch design center and is capable of working with disparate data sources. While this provides rich unified access to data, this can also be quite inefficient and expensive. Analytic processing requires massive data sets to be repeatedly copied and data to be reformatted to suit Spark. In many cases, it ultimately fails to deliver the promise of interactive analytic performance.
For instance, each time an aggregation is run on a large Cassandra table, it necessitates streaming the entire table into Spark to do the aggregation. Caching within Spark is immutable and results in stale insight.

### The SnappyData Approach
At SnappyData, we take a very different approach. SnappyData fuses a low latency, highly available in-memory transactional database (GemFireXD) into Spark with shared memory management and optimizations. Data in the highly available in-memory store is laid out using the same columnar format as Spark (Tungsten). All query engine operators are significantly more optimized through better vectorization and code generation. </br>
The net effect is, an order of magnitude performance improvement when compared to native Spark caching, and more than two orders of magnitude better Spark performance when working with external data sources.

Essentially, we turn Spark into an in-memory operational database capable of transactions, point reads, writes, working with Streams (Spark) and running analytic SQL queries. Or, it is an in-memory scale out Hybrid Database that can execute Spark code, SQL or even Objects.


If you are already using Spark, experience 20x speed up for your query performance. Try out this [test](https://github.com/SnappyDataInc/snappydata/blob/master/examples/quickstart/scripts/Quickstart.scala).

##### Snappy Architecture
![SnappyData Architecture](docs/Images/SnappyArchitecture.png)

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
version: 1.0.2.1

groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 1.0.2.1
```

**Using SBT Dependency**

If you are using SBT, add this line to your **build.sbt** for core SnappyData artifacts:

`libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "1.0.2.1"`

For additions related to SnappyData cluster, use:

`libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.0.2.1"`

You can find more specific SnappyData artifacts [here](http://mvnrepository.com/artifact/io.snappydata)

**Note:** If your project fails when resolving the above dependency (that is, it fails to download javax.ws.rs#javax.ws.rs-api;2.1), it may be due an issue with its pom file. </br> As a workaround, you can add the below code to your **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).

## Ad Analytics using SnappyData
Here is a stream + Transactions + Analytics use case example to illustrate the SQL as well as the Spark programming approaches in SnappyData - [Ad Analytics code example](https://github.com/SnappyDataInc/snappy-poc). Here is a [screencast](https://www.youtube.com/watch?v=bXofwFtmHjE) that showcases many useful features of SnappyData. The example also goes through a benchmark comparing SnappyData to a Hybrid in-memory database and Cassandra.

## Contributing to SnappyData

If you are interested in contributing, please visit the [community page](http://www.snappydata.io/community) for ways in which you can help.

