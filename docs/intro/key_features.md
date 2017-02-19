## Key Features

* **100% compatible with Spark** - Use SnappyData as a database, but also use any of the Spark APIs - ML, Graph, etc.

* **In-memory row and column stores**: Run the store collocated in Spark executors or in its own process space (i.e. a computational cluster and a data cluster)

* **SQL standard compliance**: Spark SQL + several SQL extensions: DML, DDL, indexing, constraints.

* **SQL based extensions for streaming processing**: Use native Spark streaming, DataFrame APIs or declaratively specify your streams and how you want it processed. You do not need to learn Spark APIs to get going with stream processing or its subtleties when processing in parallel.

* **Not-Only SQL**: Use either as a SQL database or work with JSON or even arbitrary Application Objects. Essentially, any Spark RDD/DataSet can also be persisted into SnappyData tables (type system same as Spark DataFrames). 

* **Interactive analytics using Synopsis Data Engine (SDE)**: We introduce multiple synopses techniques through data structures like count-min-sketch and stratified sampling to dramatically reduce the in-memory space requirements and provide true interactive speeds for analytic queries. These structures can be created and managed by developers with little to no statistical background and can be completely transparent to the SQL developer running queries. Error estimators are also integrated with simple mechanisms to get to the errors through built-in SQL functions.

* **Mutate, transact on data in Spark**: You can use SQL to insert, update, delete data in tables as one would expect. We also provide extensions to Spark’s context so you can mutate data in your Spark programs. Any tables in SnappyData is visible as DataFrames without having to maintain multiples copies of your data: cached RDDs in Spark and then separately in your data store.

* **Optimizations - Indexing**: You can index your RowStore and the GemFire SQL optimizer, which automatically uses in-memory indexes when available.

* **Optimizations - collocation**: SnappyData implements several optimizations to improve data locality and avoid shuffling data for queries on partitioned data sets. All related data can be collocated using declarative custom partitioning strategies (for example, common shared business key). Reference data tables can be modeled as replicated tables when tables cannot share a common key. Replicas are always consistent.

* **High availability not just Fault tolerance**: Data can be instantly replicated (one at a time or batch at a time) to other nodes in the cluster. It is deeply integrated with a membership-based distributed system to detect and handle failures, instantaneously providing applications continuous HA.

* **Durability and recovery:** Data can also be managed on disk and automatically recovered. Utilities for backup and restore are bundled.