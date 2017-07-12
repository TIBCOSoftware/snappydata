# Design Principles of Scalable, Partition-Aware Databases

The key design goal for achieving linear scaling is to use a partitioning strategy that allows most data access (queries) to be pruned to a single partition. This avoids expensive locking operations across multiple partitions during query execution.

In a highly concurrent system that has thousands of connections, multiple queries are generally spread uniformly across the entire data set (and therefore across all partitions). Therefore, increasing the number of data stores enables linear scalability. Given sufficient network performance, additional connections can be supported without degrading the response time for queries.

The general strategy for designing a SnappyData database is to identify the tables to partition or replicate in the SnappyData cluster, and then to determine the correct partitioning key(s) for partitioned tables. This usually requires an iterative process to produce the optimal design:

1. Read [Identify Entity Groups and Partitioning Keys](entity_groups.md) and [Replicate Dimension Tables](partitioned_vs_replicated_row.md) to understand the basic rules for defining partitioned or replicated tables.

2. Evaluate your data access patterns to define those entity groups that are candidates for partitioning. Focus your efforts on commonly-joined entities. Remember that all join queries must be performed on data that is colocated. SnappyData only supports joins where the data is colocated. Colocated data is also important for transaction updates, because the transaction can execute without requiring distributed locks in a multi-phase commit protocol.

3. Identify all of the tables in the entity groups.

4. Identify the “partitioning key” for each partitioned table. The partitioning key is the column or set of columns that are common across a set of related tables. Look for parent-child relationships in the joined tables. The primary key of a root entity is generally also the best choice for partitioning key.

	!!! Note:
		SnappyData supports distributed queries by parallelizing the query execution across data stores. However, each query instance on a partition can only join rows that are colocated with the partitioned data. This means that queries can join rows between a partitioned table and any number of replicated tables hosted on the data store with no restrictions. But queries that join multiple, partitioned tables have to be filtered based on the partitioning key. 
    
5. Identify all of the tables that are candidates for replication. You can replicate table data for high availability, or to colocate table data that is necessary to execute joins.
