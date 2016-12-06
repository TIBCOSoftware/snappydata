## Table of Contents
* [Introduction](#introduction)
* [Key Features](#key-features)
* [Spark Challenges for Mixed Workloads (OLTP, OLAP)](#SparkChallenges)

## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP (online transaction processing) and OLAP (online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD (as an in-memory transactional store with scale-out SQL semantics).

<p style="text-align: center;"><img alt="SnappyDataOverview" src="./Images/SnappyDataOverview.png"></p>

Conceptually, you could think of SnappyData as an **in-memory database that embeds Spark as its computational engine** - to process streams, work with myriad data sources like HDFS, and process data through a rich set of higher level abstractions. While the SnappyData engine is primarily designed for SQL processing, applications can work with Objects through Spark RDDs and the newly introduced Spark DataSets.

Any Spark DataFrame can be easily managed as a SnappyData Table or conversely any table can be accessed as a DataFrame.

By default, when the cluster is started, the data store is bootstrapped and when any Spark Jobs/OLAP queries are submitted Spark executors are automatically launched within the Snappy process space (JVMs). There is no need to connect and manage external data store clusters. The Snappy store can synchronously replicate for HA with strong consistency and store/recover from disk for additional reliability.


## Key Features
- **100% compatible with Spark** - Use SnappyData as a database but also use any of the Spark APIs - ML, Graph, etc
- **In-memory row and column stores**: Run the store collocated in Spark executors or in its own process space (i.e. a computational cluster and a data cluster)
- **SQL standard compliance**: Spark SQL + several SQL extensions: DML, DDL, indexing, constraints.
- **SQL based extensions for streaming processing**: Use native Spark streaming, Dataframe APIs or declaratively specify your streams and how you want it processed. You do not need to learn Spark APIs to get going with stream processing or its subtleties when processing in parallel.
- **Interactive analytics using Synopsis Data Engine (SDE)**: We introduce multiple synopses techniques through data structures like count-min-sketch and stratified sampling to dramatically reduce the in-memory space requirements and provide true interactive speeds for analytic queries. These structures can be created and managed by developers with little to no statistical background and can be completely transparent to the SQL developer running queries. Error estimators are also integrated with simple mechanisms to get to the errors through built-in SQL functions.
- **Mutate, transact on data in Spark**: You can use SQL to insert, update, delete data in tables as one would expect. We also provide extensions to Spark’s context so you can mutate data in your Spark programs. Any tables in SnappyData is visible as DataFrames without having to maintain multiples copies of your data: cached RDDs in Spark and then separately in your data store.
- **Optimizations - Indexing**: You can index your RowStore and the GemFire SQL optimizer automatically uses in-memory indexes when available.
- **Optimizations - colocation**: SnappyData implements several optimizations to improve data locality and avoid shuffling data for queries on partitioned data sets. All related data can be collocated using declarative custom partitioning strategies(e.g. common shared business key). Reference data tables can be modeled as replicated tables when tables cannot share a common key. Replicas are always consistent. 
- **High availability not just Fault tolerance**: Data can be instantly replicated (one at a time or batch at a time) to other nodes in the cluster and is deeply integrated with a membership based distributed system to detect and handle failures instantaneously providing applications continuous HA.
- **Durability and recovery:** Data can also be managed on disk and automatically recovered. Utilities for backup and restore are bundled.

<a id="SparkChallenges"></a>

## Extensions to the Spark Runtime

SnappyData makes the following contributions to deliver a unified and optimized runtime.  
1. __Integrating an operational in-memory data store with Spark’s computational model:__ We introduce a number of extensions to fuse our runtime with that of Spark. Spark executors run in the same process space as our store’s execution threads, sharing the same pool of memory. When Spark executes tasks in a partitioned manner, it is designed to keep all the available CPU cores busy. We extend this design by allowing low latency and fine grained operations to interleave and get higher priority, without involving the scheduler. Furthermore, to support high concurrency, we extend the runtime with a “Job Server” that decouples applications from data servers, operating much in the same way as a traditional database, whereby state is shared across many clients and applications.

2. __Unified API for OLAP, OLTP, and Streaming:__ Spark builds on a common set of abstractions to provide a rich API for a diverse range of applications, such as MapReduce, Machine learning, stream processing, and SQL.
While Spark deserves much of the credit for being the first of its kind to offer a unified API, we further extend its API to: 
	
	* Allow for OLTP operations, e.g., transactions and mutations (inserts/updates/deletions) on tables  
	* Be conformant with SQL standards, e.g., allowing tables alterations, constraints, indexes, and   
	* Support declarative stream processing in SQL

3. __Optimizing Spark application execution times:__ Our goal is to eliminate the need for yet another external store (e.g., a KV store) for Spark applications. With a deeply integrated store, SnappyData improves overall performance by minimizing network traffic and serialization costs. In addition, by promoting colocated schema designs (tables and streams) where related data is colocated in the same process space, SnappyData eliminates the need for shuffling altogether in several scenarios.

4. __Synopsis Data Engine support built into Spark:__ To deliver analytics at truly interactive speeds, we have equipped SnappyData with state-of-the-art SDE techniques, as well as a number of novel features. 

	SnappyData is the first SDE engine to :

	-	Provide automatic bias correction for arbitrarily complex SQL queries  
	-	Provide an intuitive means for end users to express their accuracy requirements as high-level accuracy contracts (HAC), without overwhelming them with numerous statistical concepts  
	-	Provide error estimates for arbitrarily complex queries on streams (Unlike traditional load shedding techniques that are restricted to simple queries)

## Spark Challenges for Mixed Workloads (OLTP, OLAP)
Spark is designed as a computational engine for processing batch jobs. Each Spark application (e.g., a Map-reduce job) runs as an independent set of processes (i.e., executor JVMs) on the cluster. These JVMs are re- used for the lifetime of the application. While, data can be cached and reused in these JVMs for a single application, sharing data across applications or clients requires an ex- ternal storage tier, such as HDFS. We, on the other hand, target a real-time, “always-on”, operational design center— clients can connect at will, and share data across any number of concurrent connections. This is similar to any operational database on the market today. Thus, to manage data in the same JVM, our first challenge is to alter the life cycle of these executors so that they are long-lived and de-coupled from individual applications.

A second but related challenge is Spark’s design for how user requests (i.e., jobs) are handled. A single driver orchestrates all the work done on the executors. Given our need for high concurrency and a hybrid OLTP-OLAP workload, this driver introduces

1. a single point of contention for all requests, and 
2. a barrier for achieving high availability (HA). Executors are shutdown if the driver fails, requiring a full refresh of any cached state.

Spark’s primary usage of memory is for caching RDDs and for shuffling blocks to other nodes. Data is managed in blocks and is immutable. On the other hand, we need to manage more complex data structures (along with indexes) for point access and updates. Therefore, another challenge is merging these two disparate storage systems with little impedance to the application. This challenge is exacerbated by current limitations of Spark SQL—mostly related to mu- tability characteristics and conformance to SQL.

Finally, Spark’s strong and growing community has zero tolerance for incompatible forks. This means that no changes can be made to Spark’s execution model or its semantics for existing APIs. In other words, our changes have to be an extension.

