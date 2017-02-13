#Architecture Overview

This section presents a high-level overview of SnappyData’s core components. It also explains how our data pipeline (as streams) are ingested into our in-memory store and subsequently interacted with, and analyzed.

## Core Components
The following depicts the core components of SnappyData, where Spark’s original components are highlighted in gray. To simplify, we have omitted standard components, such as security and monitoring.

![Core components](CoreComponents.png)

The storage layer is primarily in-memory and manages data in either row or column formats. The column format is derived from Spark’s RDD caching implementation and allows for compression. Row-oriented tables can be indexed on keys or secondary columns, supporting fast reads and writes on index keys. Refer to the [Row/Column table](programming_guide.md#tables-in-snappydata) section for details on the syntax and available features. 

We support two primary programming models — SQL and Spark’s API. SQL access is through JDBC/ODBC and it supports the Spark SQL dialect with several extensions, to make the language compatible with the SQL standard. One could perceive SnappyData as an SQL database that uses Spark API as its language for stored procedures. Our [stream processing](programming_guide.md#stream-processing-using-sql) is primarily through Spark Streaming, but it is integrated and runs within our store.

The OLAP scheduler and job server coordinate all OLAP and Spark jobs and are capable of working with external cluster managers, such as YARN or Mesos (not yet supported). We route all OLTP operations immediately to appropriate data partitions without incurring any scheduling overhead.

To support replica consistency, fast point updates, and instantaneous detection of failure conditions in the cluster, we use a P2P (peer-to-peer) cluster membership service that ensures view consistency and virtual synchrony in the cluster. Any of the in-memory tables can be synchronously replicated using this P2P cluster.

In addition to the “exact” Dataset, data can also be summarized using probabilistic data structures, such as stratified samples and other forms of synopses. Using our API, applications can choose to trade accuracy for performance. SnappyData’s query engine has built-in support for Synopsis Data Engine (SDE) and exploits appropriate probabilistic data structures to meet the user’s requested level of accuracy or performance.

To understand the data flow architecture, we first walk through a real-time use case that involves stream processing, ingesting into an in-memory store and interactive analytics.

## Data Ingestion Pipeline
The data pipeline involving analytics while streams are being ingested and subsequent interactive analytics will be the pervasive architecture for real-time applications. The steps to support these tasks are depicted in the following figure and explained below.

![Data Ingestion Pipeline](DataIngestionPipeline.png)

1. Once the SnappyData cluster is started and before any live streams can be processed, we can ensure that the historical and reference datasets are readily accessible. The data sets may come from HDFS, enterprise relational databases (RDB), or disks managed by SnappyData. Immutable batch sources (for example, HDFS) can be loaded in parallel into a columnar format table with or without compression. Reference data that is often mutating can be managed as row tables.

1. We rely on Spark Streaming’s parallel receivers to consume data from multiple sources. These receivers produce a DStream, whereby the input is batched over small time intervals and emitted as a stream of RDDs. This batched data is typically transformed, enriched and emitted as one or more additional streams. The raw incoming stream may be persisted into HDFS for batch analytics.

2. Next, we use SQL to analyze these streams. As DStreams (RDDs) use the same processing and data model as data stored in tables (DataFrames), we can seamlessly combine these data structures in arbitrary SQL queries (referred to as continuous queries as they execute each time the stream emits a batch). When faced with complex analytics or high-velocity streams, SnappyData can still provide answers in real time by resorting to approximation.

3. The stream processing layer can interact with the storage layer in a variety of ways. The enriched stream can be efficiently stored in a column table. The results of continuous queries may result in several point updates in the store (for example, maintaining counters). The continuous queries may join, correlate, and aggregate with other streams, history or reference data tables. When records are written into column tables one (or a small batch) at a time, data goes through stages, arriving first into a delta row buffer that is capable of high write rates, and then aging into a columnar form. Our query sub-system (which extends Spark’s Catalyst optimizer) merges the delta row buffer during query execution.

4. To prevent running out of memory, tables can be configured to evict or overflow to disk using an LRU strategy. For instance, an application may ingest all data into HDFS while preserving the last day’s worth of data in memory.

5. Once ingested, the data is readily available for interactive analytics using SQL. Similar to stream analytics, SnappyData can again use Synopsis Data Engine to ensure interactive analytics on massive historical data in accordance with users’ requested accuracy.


## Hybrid Cluster Manager

Spark applications run as independent processes in the cluster, coordinated by the application’s main program, called the driver program. Spark applications connect to cluster managers (for example, YARN and Mesos) to acquire executors on nodes in the cluster. Executors are processes that run computations and store data for the running application. The driver program owns a singleton (SparkContext) object which it uses to communicate with its set of executors. This is represented in the following figure.

![Hybrid Cluster](Images/hybrid_cluster.png)

While Spark’s approach is appropriate and geared towards compute-heavy tasks that scan large datasets, SnappyData must meet the following additional requirements as an operational database.

1. **High Concurrency**: SnappyData use cases involve a mixture of compute-intensive workloads and low latency (sub-millisecond) OLTP operations such as point lookups (index-based search), and insert/update of a single record. The fair scheduler of Spark is not designed to meet the low latency requirements of such operations.

2. **State Sharing**: Each application submitted to Spark works in isolation. State sharing across applications requires an external store, which increases latency and is not viable for near real-time data sharing.

4. ** High Availability (HA)**: As a highly concurrent distributed system that offers low latency access to data, we must protect applications from node failures (caused by software bugs and hardware/network failures). High availability of data and transparent handling of failed operations, therefore, become an important requirement for SnappyData.

4. ** Consistency**: As a highly available system that offers concurrent data access, it becomes important to ensure that all applications have a consistent view of data.
After an overview of our cluster architecture, we explain how SnappyData meets each of these requirements in the subsequent sections.

### SnappyData Cluster Architecture
A SnappyData cluster is a peer-to-peer (P2P) network comprised of three distinct types of members as represented in the below figure.

1. Locator: Locator members provide discovery service for the cluster. They inform a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.

2. Lead Node: The lead node member acts as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.

3. Data Servers: A data server member hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node or pass it to the lead node for execution by Spark SQL.

![ClusterArchitecture](GettingStarted_Architecture.png)

SnappyData also has multiple deployment options. For more information refer to, [Deployment Options](./deployment.md).

### Interacting with SnappyData

!!! Note
	For the section on the Spark API, we assume some familiarity with [core Spark, Spark SQL and Spark Streaming concepts](http://spark.apache.org/docs/latest/).
And, you can try out the Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). All the commands and programs listed in the Spark guides work in SnappyData as well.
For the section on SQL, no Spark knowledge is necessary.</Note>

To interact with SnappyData, we provide interfaces for developers familiar with Spark programming as well as SQL. JDBC can be used to connect to the SnappyData cluster and interact using SQL. On the other hand, users comfortable with the Spark programming paradigm can write jobs to interact with SnappyData. Jobs can be like a self-contained Spark application or can share the state with other jobs using the SnappyData store.

Unlike Apache Spark, which is primarily a computational engine, the SnappyData cluster holds mutable database state in its JVMs and requires all submitted Spark jobs/queries to share the same state (of course, with schema isolation and security as expected in a database). This required extending Spark in two fundamental ways.

1. **Long running executors**: Executors are running within the SnappyData store JVMs and form a p2p cluster.  Unlike Spark, the application Job is decoupled from the executors - submission of a job does not trigger launching of new executors.

2. **Driver runs in HA configuration**: Assignment of tasks to these executors are managed by the Spark Driver.  When a driver fails, this can result in the executors getting shut down, taking down all cached state with it. Instead, we leverage the [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver) to manage Jobs and queries within a "lead" node.  Multiple such leads can be started and provide HA (they automatically participate in the SnappyData cluster enabling HA).

In this document, we showcase mostly the same set of features via the Spark API or using SQL. If you are familiar with Scala and understand Spark concepts you can choose to skip the SQL part go directly to the [Spark API section](./programming_guide.md#snappysession-and-snappystreamingcontext).

### High Concurrency in SnappyData
Thousands of concurrent ODBC and JDBC clients can simultaneously connect to a SnappyData cluster. To support this degree of concurrency, SnappyData categorizes incoming requests from these clients into low latency requests and high latency ones.

For low latency operations, we completely bypass Spark’s scheduling mechanism and directly operate on the data. We route high latency operations (for example, compute intensive queries) through Spark’s fair scheduling mechanism. This makes SnappyData a responsive system, capable of handling multiple low latency short operations as well as complex queries that iterate over large datasets simultaneously.

### State Sharing in SnappyData
A SnappyData cluster is designed to be a long-running clustered database. The state is managed in tables that can be shared across any number of connecting applications. Data is stored in memory and replicated to at least one other node in the system. Data can be persisted to disk in shared nothing disk files for quick recovery. Nodes in the cluster stay up for a long time and their lifecycle is independent of application lifetimes. SnappyData achieves this goal by decoupling its process startup and shutdown mechanisms from those used by Spark.
