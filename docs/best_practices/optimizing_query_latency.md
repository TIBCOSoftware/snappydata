# Optimizing Query Latency: Partitioning and Replication Strategies
The following topics are covered in this section:

* [Using Column vs Row Table](#column-row)

* [Using Partitioned vs Replicated Row Table](#partition-replicate)

* [Applying Partitioning Scheme](#partition-scheme)

* [Using Redundancy](#redundancy)

* [Overflow Configuration](#overflow)

<a id="column-row"></a>
## Using Column Versus Row Table

A columnar table data is stored in a sequence of columns, whereas, in a row table it stores table records in a sequence of rows.

<a id="column-table"></a>
### Using Column Tables

**Analytical Queries**: A column table has distinct advantages for OLAP queries and therefore large tables involved in such queries are recommended to be created as columnar tables. These tables are rarely mutated (deleted/updated).
For a given query on a column table, only the required columns are read (since only the required subset columns are to be scanned), which gives a better scan performance. Thus, aggregation queries execute faster on a column table compared to a  row table.

**Compression of Data**: Another advantage that the column table offers is that it allows highly efficient compression of data which reduces the total storage footprint for large tables.

Column tables are not suitable for OLTP scenarios. In this case, row tables are recommended.

<a id="row-table"></a>
### Using Row Tables

**OLTP Queries**: Row tables are designed to return the entire row efficiently and are suited for OLTP scenarios when the tables are required to be mutated frequently (when the table rows need to be updated/deleted based on some conditions). In these cases, row tables offer distinct advantages over the column tables.

**Point queries**: Row tables are also suitable for point queries (for example, queries that select only a few records based on certain where clause conditions). 

**Small Dimension Tables**: Row tables are also suitable to create small dimension tables as these can be created as replicated tables (table data replicated on all data servers).

**Create Index**: Row tables also allow the creation of an index on certain columns of the table which improves performance.

<a id="partition-replicate"></a>
## Using Partitioned Versus Replicated Row Table

In SnappyData, row tables can be either partitioned across all servers or replicated on every server. For row tables, large fact tables should be partitioned whereas dimension tables can be replicated.

The SnappyData architecture encourages you to denormalize “dimension” tables into fact tables when possible, and then replicate remaining dimension tables to all data stores in the distributed system.

Most databases follow the [star schema](http://en.wikipedia.org/wiki/Star_schema) design pattern where large “fact” tables store key information about the events in a system or a business process. For example, a fact table would store rows for events like product sales or bank transactions. Each fact table generally has foreign key relationships to multiple “dimension” tables, which describe further aspects of each row in the fact table.

When designing a database schema for SnappyData, the main goal with a typical star schema database is to partition the entities in fact tables. Slow-changing dimension tables should then be replicated on each data store that hosts a partitioned fact table. In this way, a join between the fact table and any number of its dimension tables can be executed concurrently on each partition, without requiring multiple network hops to other members in the distributed system.

<a id="partition-scheme"></a>
## Applying Partitioning Scheme

<a id="colocated-joins"></a>
### Colocated Joins
In SQL, a JOIN clause is used to combine data from two or more tables, based on a related column between them. JOINS have traditionally been expensive in distributed systems because the data for the tables involved in the JOIN may reside on different physical nodes and the operation has first to move/shuffle the relevant data to one node and perform the operation. </br>
SnappyData offers a way to declaratively "co-locate" tables to prevent or reduce shuffling to execute JOINS. When two tables are partitioned on columns and colocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData member. Therefore, in a join query, the join operation is performed on each node's local data. Eliminating data shuffling improves performance significantly.</br>
For examples refer to, [How to colocate tables for doing a colocated join](../howto/perform_a_colocated_join.md).

<a id="buckets"></a>
### Buckets
A bucket is the smallest unit of in-memory storage for SnappyData tables. Data in a table is distributed evenly across all the buckets. For more information on BUCKETS, refer to [BUCKETS](important_settings.md#buckets).</br>

The default number of buckets in SnappyData cluster mode is 128. In the local mode, it is cores x 2, subject to a maximum of 64 buckets and a minimum of 8 buckets.

The number of buckets has an impact on query performance, storage density, and ability to scale the system as data volumes grow.
Before deciding the total number of buckets in a table, you must consider the size of the largest bucket required for a table. The total time taken by a query is greater than the scheduling delay combined with the time needed to scan the largest bucket by a thread. In the case of high concurrency, scheduling delay can be more even if the time to scan the bucket is less. In case of an extremely large bucket, even if the scheduling delay is less, the time to scan the bucket can be large.

The following principles should be considered when you set the total number of buckets for a table:
*	Ensure that data is evenly distributed among the buckets.
*	If a query on the table is frequent with low latency requirement and those queries scan all the buckets,  then ensure that the total number of the buckets is either greater than or equal to the total number of physical cores in a cluster.
*	If the query is frequent and the query gets pruned to some particular buckets, that is if the query has partitioning columns in predicate, then the total number of buckets can be low if the concurrency is high. 

When a new server joins or an existing server leaves the cluster, buckets are moved around to ensure that data is balanced across the nodes where the table is defined.

The  [-rebalance](../configuring_cluster/property_description.md#rebalance) option on the startup command-line triggers bucket rebalancing and the new server becomes the primary for some of the buckets (and secondary for some if REDUNDANCY>0 has been specified). </br>
You can also set the system procedure [call sys.rebalance_all_buckets()](../reference/inbuilt_system_procedures/rebalance-all-buckets.md#sysrebalance_all_buckets) to trigger rebalance.

<a id="dimension"></a>
### Criteria for Column Partitioning
It is recommended to use a relevant dimension for partitioning so that all partitions are active and the query is executed concurrently.</br>
If only a single partition is active and is used mainly by queries (especially concurrent queries), it means a significant bottleneck where only a single partition is active all the time, while others are idle. This serializes execution into a single thread handling that partition. Therefore, it is not recommended to use DATE/TIMESTAMP as partitioning.

<a id="redundancy"></a>
## Using Redundancy

REDUNDANCY clause of [CREATE TABLE](../reference/sql_reference/create-table.md) specifies the number of secondary copies you want to maintain for your partitioned table. This allows the table data to be highly available even if one of the SnappyData members fails or shuts down. 

A REDUNDANCY value of 1 is recommended to maintain a secondary copy of the table data. A large value for REDUNDANCY clause has an adverse impact on performance, network usage, and memory usage.

For an example on the REDUNDANCY clause refer to [Tables in SnappyData](../programming_guide/tables_in_snappydata.md).

<a id="overflow"></a>
## Overflow Configuration

In SnappyData, row and column tables by default overflow to disk (which is equivalent to setting OVERFLOW to 'true'), based on EVICTION_BY criteria. Users cannot set OVERFLOW to 'false', except when EVICTION_BY is not set, in which case it disables the eviction.

For example, setting EVICTION_BY to `LRUHEAPPERCENT` allows table data to be evicted to disk based on the current memory consumption of the server.

Refer to [CREATE TABLE](../reference/sql_reference/create-table.md) link to understand how to configure [OVERFLOW](../reference/sql_reference/create-table.md#overflow) and [EVICTION_BY](../reference/sql_reference/create-table.md#eviction-by) clauses.

!!! Tip
	By default, the eviction is set to `overflow-to-disk`.

## Known Limitation

### Change NOT IN queries to use NOT EXISTS if possible

Currently, all `NOT IN` queries use an unoptimized plan and lead to a nested-loop-join which can take long if both sides are large ([https://issues.apache.org/jira/browse/SPARK-16951](https://issues.apache.org/jira/browse/SPARK-16951)). Change your queries to use `NOT EXISTS` which uses an optimized anti-join plan. 

For example a query like:

```
select count(*) from T1 where id not in (select id from T2)
```

can be changed to:

``` 
select count(*) from T1 where not exists (select 1 from T2 where T1.id = T2.id)
```

Be aware of the different null value semantics of the two operators as noted here for [Spark](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html).

In a nutshell, the `NOT IN` operator is null-aware and skips the row if the sub-query has a null value, while the `NOT EXISTS` operator ignores the null values in the sub-query. In other words, the following two are equivalent when dealing with null values:

```
select count(*) from T1 where id not in (select id from T2 where id is not null)
select count(*) from T1 where not exists (select 1 from T2 where T1.id = T2.id)
```
