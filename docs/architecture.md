## Architecture overview
This section presents a high level overview of SnappyData’s core components, as well as our data pipeline as streams are ingested into our in-memory store and subsequently interacted with and analyzed.

### Core components
Figure 1 depicts the core components of SnappyData, where Spark’s original components are highlighted in gray. To simplify, we have omitted standard components, such as security and monitoring.

![Core components](CoreComponents.png = 300x400)

The storage layer is primarily in-memory and manages data in either row or column formats. The column format is derived from Spark’s RDD caching implementation and allows for compression. Row oriented tables can be indexed on keys or secondary columns, supporting fast reads and writes on index keys. See [Row/Column table](rowAndColumnTables.md) section for details on the syntax and available features. 

We support two primary programming models — SQL and Spark’s API. SQL access is through JDBC/ODBC and is supports the Spark SQL dialect with several extensions to make the language compatible to the SQL standard. One could perceive SnappyData as a SQL database that uses Spark API as its language for stored procedures.Our [stream processing](streamingWithSQL.md) is primarily through Spark Streaming, but it is integrated and runs in-situ with our store .

The OLAP scheduler and job server coordinate all OLAP and Spark jobs and are capable of working with external cluster managers, such as YARN or Mesos(not yet supported). We route all OLTP operations immediately to appropriate data partitions without incurring any scheduling overhead.

To support replica consistency, fast point updates, and instantaneous detection of failure conditions in the cluster, we use a P2P (peer-to-peer) cluster membership service that en- sures view consistency and virtual synchrony in the cluster. Any of the in-memory tables can be synchronously replicated using this P2P cluster.

In addition to the “exact” dataset, data can also be summarized using probabilistic data structures, such as stratified samples and other forms of synopses. Using our API, applications can choose to trade accuracy for performance. SnappyData’s query engine has built-in support for approximate query processing (AQP) and will exploit appropriate probabilistic data structures to meet the user’s requested level of accuracy or performance.

To understand the data flow architecture, we first walk through a real time use case that involves stream processing, ingesting into a in-memory store and interactive analytics. 

### Use case
#### Location based services from telco network providers
The global proliferation of mobile devices has created a growing market for location based services. In addition to locality-aware search and navigation, network providers are increasingly relying on location-based advertising, emergency call positioning, road traffic optimization, efficient call routing, triggering preemptive maintenance of cell towers, roaming analytics, and tracking vulnerable people in real time. Telemetry events are delivered as Call Detail Records (CDR), containing hundreds of attributes about each call. Ingested CDRs are cleansed and transformed for consumption by various applications. Not being able to correlate customer support calls with location specific network congestion information is a problem that frustrates customers and network technicians alike. The ability to do this in real time may involve expensive joins to history, tower traffic data and subscriber profiles. Incoming streams generate hundreds of aggregate metrics and KPIs (key performance indicators) grouped by subscriber, cell phone type, cell tower, and location. This requires continuous updates to counters accessed through primary keys (such as the subscriberID). While the generated data is massive, it still needs to be interactively queried by a data analyst for network performance analysis. Location-based services represent another common problem among our customers that involves high concurrency, continuous data updates, complex queries, time series data, and a source that cannot be throttled.

### Data ingestion pipeline
The data pipeline involving analytics while streams are being ingested and subsequent interactive analytics will be the pervasive architecture for real-time applications. The steps to support these tasks are depicted in Figure 2, and explained below.

![Data Ingestion Pipeline](DataIngestionPipeline.png = 200x600 )

1. Once the SnappyData cluster is started and before any live streams can be processed, we can ensure that the historical and reference datasets are readily accessible. The data sets may come from HDFS, enterprise relational databases (RDB), or disks managed by SnappyData. Immutable batch sources (e.g., HDFS) can be loaded in parallel into a colum- nar format table with or without compression. Reference data that is often mutating can be managed as row tables.

2. We rely on Spark Streaming’s parallel receivers to consume data from multiple sources. These receivers produce a DStream, whereby the input is batched over small time intervals and emitted as a stream of RDDs. This batched data is typically transformed, enriched and emit- ted as one or more additional streams. The raw incoming stream may be persisted into HDFS for batch analytics.

3. Next, we use SQL to analyze these streams. As DStreams (RDDs) use the same processing and data model as data stored in tables (DataFrames), we can seamlessly combine these data structures in arbitrary SQL queries (referred to as continuous queries as they execute each time the stream emits a batch). When faced with complex analytics or high velocity streams, SnappyData can still provide answers in real time by resorting to approximation.

4. The stream processing layer can interact with the storage layer in a variety of ways. The enriched stream can be efficiently stored in a column table. The results of continuous queries may result in several point updates in the store (e.g., maintaining counters). The continuous queries may join, correlate, and aggregate with other streams, history or reference data tables. When records are written into column tables one (or a small batch) at a time, data goes through stages, arriving first into a delta row buffer that is capable of high write rates, and then aging into a columnar form. Our query sub-system (which extends Spark’s Catalyst optimizer) merges the delta row buffer during query execution.

5. To prevent running out of memory, tables can be configured to evict or overflow to disk using an LRU strategy. For instance, an application may ingest all data into HDFS while preserving the last day’s worth of data in memory.

6. Once ingested, the data is readily available for interactive analytics using SQL. Similar to stream analytics, SnappyData can again use approximate query processing to ensure interactive analytics on massive historical data in accordance to users’ requested accuracy.


### Hybrid Cluster Manager

As shown in Figure above, spark applications run as independent processes in the cluster, coordinated by the application’s main program, called the driver program. Spark applications connect to cluster managers (e.g., YARN and Mesos) to acquire executors on nodes in the cluster. Executors are processes that run computations and store data for the running application. The driver program owns a sin- gleton (SparkContext) object which it uses to communicate with its set of executors.

While Spark’s approach is appropriate and geared towards compute-heavy tasks that scan large datasets, SnappyData must meet additional requirements (1–4) as an operational database.

1. __High concurrency__ : SnappyData use cases involve a mixture of compute-intensive workloads and low latency (sub-millisecond) OLTP operations such as point lookups (index-based search), and insert/update of a single record. The fair scheduler of Spark is not designed to meet the low latency requirements of such operations.

2. __State sharing__ : Each application submitted to Spark works in isolation. State sharing across applications requires an external store, which increases latency and is not viable for near real time data sharing.

3. High availability (HA) — As a highly concurrent distributed system that offers low latency access to data, we must protect applications from node failures (caused by soft- ware bugs and hardware/network failures). High availability of data and transparent handling of failed operations there- fore become an important requirement for SnappyData.

4. Consistency — As a highly available system that of- fers concurrent data access, it becomes important to ensure that all applications have a consistent view of data.
After an overview of our cluster architecture in section 5.1, we explain how SnappyData meets each of these require- ments in the subsequent sections.

#### SnappyData Cluster Architecture
A SnappyData cluster is a peer-to-peer (P2P) network comprised of three distinct types of members (see figure 4).
1. Locator. Locator members provide discovery service for the cluster. They inform a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.
2. Lead Node. The lead node member acts as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.
3. Data Servers. A data server member hosts data, em- beds a Spark executor, and also contains a SQL engine ca- pable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node, or pass it to the lead node for execution by Spark SQL.

#### High Concurrency in SnappyData
Thousands of concurrent ODBC and JDBC clients can si- multaneously connect to a SnappyData cluster. To support this degree of concurrency, SnappyData categorizes incom- ing requests from these clients into (i) low latency requests and (ii) high latency ones. For low latency operations, we completely bypass Spark’s scheduling mechanism and di- rectly operate on the data. We route high latency opera- tions (e.g., compute intensive queries) through Spark’s fair scheduling mechanism. This makes SnappyData a respon- sive system, capable of handling multiple low latency short operations as well as complex queries that iterate over large datasets simultaneously.

#### State Sharing in SnappyData
A SnappyData cluster is designed to be a long running clustered database. State is managed in tables that can be shared across any number of connecting applications. Data is stored in memory and replicated to at least one other node in the system. Data can be persisted to disk in shared nothing disk files for quick recovery. (See section 4 for more details on table types and redundancy.) Nodes in the cluster stay up for a long time and their life-cycle is independent of application lifetimes. SnappyData achieves this goal by decoupling its process startup and shutdown mechanisms from those used by Spark.



