### The Challenge with Spark and Remote Data Sources
Apache Spark is a general purpose parallel computational engine for analytics at scale. At its core, it has a batch design center and can access disparate data sources in a highly parallelized manner for its distributed computations. Typically, data is fetched lazily as a result of SQL query or a Dataset (RDD) getting materialized. This can be quite inefficient and expensive if the data set has to be repeatedly processed. Caching within Spark is immutable and still requires the application to periodically refresh the data set, let alone having to bear the burden of duplicating the dataset. 

Analytic processing requires massive data sets to be repeatedly copied and data to be reformatted to suit Spark. In many cases, it ultimately fails to deliver the promise of interactive analytic performance. For instance, each time an aggregation is run on a large Cassandra table, it necessitates streaming the entire table into Spark to do the aggregation. Caching within Spark is immutable and results in stale insight.

<a id="approach"></a>
# The SnappyData Approach
At SnappyData, we take a very different approach. SnappyData fuses a low latency, highly available in-memory transactional database (GemFireXD) 
into Spark with shared memory management and optimizations. Data in the highly available in-memory store is laid out using the same columnar 
format as Spark. All query engine operators are more optimized through better vectorization and code generation. 
The net effect is, an order of magnitude performance improvement when compared to native Spark caching, better Spark performance when working with external data sources.

Essentially, we turn Spark into an in-memory operational database capable of transactions, point reads, writes, working with Streams (Spark) and running analytic SQL queries.

![SnappyData Architecture](Images/SnappyArchitecture.png)

Conceptually, you can think of SnappyData as an **in-memory database that uses Spark's API and SQL as its interface and computational engine** - to process streams, work with myriad data sources like HDFS, and process data through a rich set of higher level abstractions. While the SnappyData engine is primarily designed for SQL processing, applications can work with Objects through Spark RDDs and the newly introduced Spark Datasets.

Any Spark DataFrame can be easily managed as a SnappyData Table or conversely any table can be accessed as a DataFrame.

By default, when the cluster is started, the data store is bootstrapped and when any Spark Jobs/OLAP queries are submitted, Spark executors are automatically launched within the SnappyData process space (JVMs). There is no need to connect and manage external data store clusters. The SnappyData store can synchronously replicate for high availability (HA) with strong consistency and store/recover from disk for additional reliability.

**Related Topics:**
* Challenges
* SnappyData Cluster Architecture
* SnappyData Core Components
* Key Benefits



