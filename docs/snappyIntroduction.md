## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP(online transaction processing) and OLAP(online analytical processing) in a single integrated cluster**. We realize this platform through a seamless deep integration of Apache Spark (as a big data computational engine) with GemFire XD(as an in- memory transactional store with scale-out SQL semantics) 

![SnappyDataOverview](./Images/SnappyDataOverview.png)


Conceptually, you could **think of SnappyData as a in-memory database that embeds Spark as its computational engine** - to process streams, work with myriad data sources like HDFS, and process data through a rich set of higher level abstractions. While the SnappyData engine is primarily designed for SQL processing, applications can work with Objects through Spark RDDs and the newly introduced Spark DataSets. 

Any Spark DataFrame can be easily managed as a SnappyData Table or conversely any table can be accessed as a DataFrame. 

By default, when the cluster is started, the data store is bootstrapped and when any Spark Jobs/OLAP queries are submitted Spark executors are automatically launched within the Snappy process space (JVMs). There is no need to connect and manage external data store clusters. The Snappy store can synchronously replicate for HA with strong consistency and store/recover from disk for additional reliability.

-----

## Extensions to the Spark Runtime

SnappyData makes the following contributions to deliver a unified and optimized runtime.  
1. __Integrating an operational in-memory data store with Spark’s computational model:__ We introduce a number of extensions to fuse our runtime with that of Spark. Spark executors run in the same process space as our store’s execution threads, sharing the same pool of memory. When Spark executes tasks in a partitioned manner, it is designed to keep all the available CPU cores busy. We extend this design by allowing low latency and fine grained operations to interleave and get higher priority, without involving the scheduler. Furthermore, to support high concurrency, we extend the runtime with a “Job Server” that decouples applications from data servers, operating much in the same way as a traditional database, whereby state is shared across many clients and applications. 

2. __Unified API for OLAP, OLTP, and Streaming:__ Spark builds on a common set of abstractions to provide a rich API for a diverse range of applications, such as MapReduce, Machine learning, stream processing, and SQL. 
While Spark deserves much of the credit for being the first of its kind to offer a unified API, we further extend its API to  
  1. allow for OLTP operations, e.g., transactions and mutations (inserts/updates/deletions) on tables  
  2. be conformant with SQL standards, e.g., allowing tables alterations, constraints, indexes, and   
  3. support declarative stream processing in SQL

3. __Optimizing Spark application execution times:__ Our goal is to eliminate the need for yet another external store (e.g., a KV store) for Spark applications. With a deeply integrated store, SnappyData improves overall performance by minimizing network traffic and serialization costs. In addition, by promoting colocated schema designs (tables and streams) where related data is colocated in the same process space, SnappyData eliminates the need for shuffling altogether in several scenarios. 

4. __Synopsis Data Engine support built into Spark:__ To deliver analytics at truly interactive speeds, we have equipped SnappyData with state-of-the-art SDE techniques, as well as a number of novel features. 
SnappyData is the first SDE engine to  
  1. Provide automatic bias correction for arbitrarily complex SQL queries  
  2. Provide an intuitive means for end users to express their accuracy requirements as high-level accuracy contracts (HAC), without overwhelming them with numerous statistical concepts  
  3. Provide error estimates for arbitrarily complex queries on streams (Unlike traditional load shedding techniques that are restricted to simple queries)

-----



