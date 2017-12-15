# Designing your Database and Schema

The key design goal for achieving linear scaling is to use a partitioning strategy that allows most data access (queries) to be pruned to a single partition. This avoids expensive locking operations across multiple partitions during query execution.

In a highly concurrent system that has thousands of connections, multiple queries are generally spread uniformly across the entire data set (and therefore across all partitions). Therefore, increasing the number of data stores enables linear scalability. Given sufficient network performance, additional connections can be supported without degrading the response time for queries.

The following topics are covered in this section:

* [Design Principles of Scalable, Partition-Aware Databases](design_principles.md)

* [Optimizing Query Latency: Partitioning and Replication Strategies](optimizing_query_latency.md)