## Architecture overview
This section presents a high level overview of Snappy- Data’s core components, as well as our data pipeline as streams are ingested into our in-memory store and subse- quently interacted with and analyzed.

### Core components
Figure 1 depicts the core components of SnappyData, where Spark’s original components are highlighted in gray. To sim- plify, we have omitted standard components, such as security and monitoring.
![Core components](CoreComponents.png)

The storage layer is primarily in-memory and manages data in either row or column formats. The column format is derived from Spark’s RDD caching implementation and allows for compression. Row oriented tables can be indexed on keys or secondary columns, supporting fast reads and writes on index keys (sections 4.1).
We support two primary programming models—SQL and Spark’s API. SQL access is through JDBC/ODBC and is based on Spark SQL dialect with several extensions. One could perceive SnappyData as a SQL database that uses Spark API as its language for stored procedures. We pro- vide a glimpse over our SQL and programming APIs (sec- tion 4.2). Our stream processing is primarily through Spark Streaming, but it is integrated and runs in-situ with our store (section 4.3).
The OLAP scheduler and job server coordinate all OLAP and Spark jobs and are capable of working with external cluster managers, such as YARN or Mesos. We route all OLTP operations immediately to appropriate data parti- tions without incurring any scheduling overhead (sections 5 and 7).
To support replica consistency, fast point updates, and in- stantaneous detection of failure conditions in the cluster, we use a P2P (peer-to-peer) cluster membership service that en- sures view consistency and virtual synchrony in the cluster. Any of the in-memory tables can be synchronously repli- cated using this P2P cluster (section 5).
In addition to the “exact” dataset, data can also be sum- marized using probabilistic data structures, such as strati- fied samples and other forms of synopses. Using our API, applications can choose to trade accuracy for performance. SnappyData’s query engine has built-in support for approx- imate query processing (AQP) and will exploit appropriate probabilistic data structures to meet the user’s requested level of accuracy or performance (section 6).

### Data ingestion pipeline
The use cases explored in section 2.1 share a common theme of stream ingestion and interactive analytics with transactional updates. The steps to support these tasks are depicted in Figure 2, and explained below.
![Data Ingestion Pipeline](DataIngestionPipeline.png)

1) Step 1. Once the SnappyData cluster is started and before any live streams can be processed, we ensure that the histor- ical and reference datasets are readily accessible. The data sets may come from HDFS, enterprise relational databases (RDB), or disks managed by SnappyData. Immutable batch sources (e.g., HDFS) can be loaded in parallel into a colum- nar format table with or without compression. Reference data that is often mutating can be managed as row tables.

2) Step 2. We rely on Spark Streaming’s parallel receivers to consume data from multiple sources. These receivers pro- duce a DStream, whereby the input is batched over small time intervals and emitted as a stream of RDDs. This batched data is typically transformed, enriched and emit- ted as one or more additional streams. The raw incoming stream may be persisted into HDFS for batch analytics.

3) Step 3. Next, we use SQL to analyze these streams. As DStreams (RDDs) use the same processing and data model as data stored in tables (DataFrames), we can seamlessly combine these data structures in arbitrary SQL queries (re- ferred to as continuous queries as they execute each time the stream emits a batch). When faced with complex ana- lytics or high velocity streams, SnappyData can still provide answers in real time by resorting to approximation.

4) Step 4. The stream processing layer can interact with the storage layer in a variety of ways. The enriched stream can be efficiently stored in a column table. The results of con- tinuous queries may result in several point updates in the store (e.g., maintaining counters). The continuous queries may join, correlate, and aggregate with other streams, his- tory or reference data tables. When records are written into column tables one (or a small batch) at a time, data goes through stages, arriving first into a delta row buffer that is capable of high write rates, and then aging into a columnar form. Our query sub-system (which extends Spark’s Cat- alyst optimizer) merges the delta row buffer during query execution.

5) Step 5. To prevent running out of memory, tables can be configured to evict or overflow to disk using an LRU strategy. For instance, an application may ingest all data into HDFS while preserving the last day’s worth of data in memory.

6) Step 6. Once ingested, the data is readily available for interactive analytics using SQL. Similar to stream analytics, SnappyData can again use approximate query processing to ensure interactive analytics on massive historical data in accordance to users’ requested accuracy.

