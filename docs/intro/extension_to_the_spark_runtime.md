## Extensions to the Spark Runtime

SnappyData makes the following contributions to deliver a unified and optimized runtime.

* **Integrating an operational in-memory data store with Spark’s computational model**: We introduce a number of extensions to fuse our runtime with that of Spark. Spark executors run in the same process space as our store’s execution threads, sharing the same pool of memory. When Spark executes tasks in a partitioned manner, it is designed to keep all the available CPU cores busy. <br/> We extend this design by allowing low latency and fine- grained operations to interleave. Furthermore, to support high concurrency, we extend the runtime with a “Job Server” that decouples applications from data servers, operating much in the same way as a traditional database, whereby the state is shared across many clients and applications. <br/>

* **Unified API for OLAP, OLTP, and Streaming**: Spark builds on a common set of abstractions to provide a rich API for a diverse range of applications, such as MapReduce, Machine learning, stream processing, and SQL.
While Spark deserves much of the credit for being the first of its kind to offer a unified API, we further extend its API to: 
	
	* Allow for OLTP operations, for example, transactions and mutations (inserts/updates/deletions) on tables 
 
	* Confirm with SQL standards, for example, allowing tables alterations, constraints, indexes, and   

	* Support declarative stream processing in SQL

* **Optimizing Spark application execution times**: Our goal is to eliminate the need for yet another external store (for example, a KV store) for Spark applications. With a deeply integrated store, SnappyData improves overall performance by minimizing network traffic and serialization costs. In addition, by promoting collocated schema designs (tables and streams) where related data is collocated in the same process space, SnappyData eliminates the need for shuffling altogether in several scenarios.

* **Synopsis Data Engine support built into Spark**: The SnappyData Synopsis Data Engine (SDE) offers a novel and scalable system to analyze large data sets. SDE uses statistical sampling techniques and probabilistic data structures to answer analytic queries with sub-second latency. There is no need to store or process the entire data set. The approach trades off query accuracy for fast response time. <br>The SDE engine enables you to:

	- Intelligently sample the data set on frequently accessed dimensions so we have a good representation across the entire data set (stratified sampling). Queries can execute on samples and return answers instantly.

	- Compute estimates for any ad hoc query from the sample(s). It can also provide error estimates for arbitrarily complex queries on streams.

	- Provide simple knobs for the user to trade off speed for accuracy, i.e. simple SQL extensions so the user can specify the error tolerance for all queries. When query error is higher than tolerance level, the system automatically delegates the query to the source.

	-	Express their accuracy requirements as high-level accuracy contracts (HAC), without overwhelming them with numerous statistical concepts.
