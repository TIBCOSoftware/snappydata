# Overview
The following topics are covered in this section:

* [Using Column vs Row Table](#column-row)
* [Using Partitioned vs Replicated Row Table](#partition-replicate)
* [Applying Partitioning Scheme](#partition-scheme)
* [Using Redundancy](#redundancy)
* [Overflow Configuration](#overflow)

<a id="column-row"></a>
## Using Column vs Row Table

A columnar table data is stored in a sequence of columns, whereas, in a row table it stores table records in a sequence of rows.

<a id="column-table"></a>
### Using Column Tables

**Analytical Queries**: A column table has distinct advantages for OLAP queries and therefore large tables involved in such queries are recommended to be created as columnar tables. These tables are rarely mutated (deleted/updated).
For a given query on a column table, only the required columns are read (since only the required subset columns are to be scanned), which gives a better scan performance. Thus, aggregation queries execute faster on a column table compared  to a  row table.

**Compression of Data**: Another advantage that the column table offers is it allows highly efficient compression of data which reduces the total storage footprint for large tables.

Column tables are not suitable for OLTP scenarios. In this case, row tables are recommended.

<a id="row-table"></a>
### Using Row Tables

**OLTP Queries**: Row tables are designed to return the entire row efficiently and are suited for OLTP scenarios when the tables are required to be mutated frequently (when the table rows need to be updated/deleted based on some conditions). In these cases, row tables offer distinct advantages over the column tables.

**Point queries**: Row tables are also suitable for point queries (for example, queries that select only a few records based on certain where clause conditions). 

**Small Dimension Tables**: Row tables are also suitable to create small dimension tables as these can be created as replicated tables (table data replicated on all data servers).

**Create Index**: Row tables also allow the creation of an index on certain columns of the table which improves  performance.

!!! Note
	In the current release of SnappyData, updates and deletes are not supported on column tables. This feature will be added in a future release.

<a id="partition-replicate"></a>
## Using Partitioned vs Replicated Row Table

In SnappyData, row tables can be either partitioned across all servers or replicated on every server. For row tables, large fact tables should be partitioned whereas, dimension tables can be replicated.

The SnappyData architecture encourages you to denormalize “dimension” tables into fact tables when possible, and then replicate remaining dimension tables to all datastores in the distributed system.

Most databases follow the [star schema](http://en.wikipedia.org/wiki/Star_schema) design pattern where large “fact” tables store key information about the events in a system or a business process. For example, a fact table would store rows for events like product sales or bank transactions. Each fact table generally has foreign key relationships to multiple “dimension” tables, which describe further aspects of each row in the fact table.

When designing a database schema for SnappyData, the main goal with a typical star schema database is to partition the entities in fact tables. Slow-changing dimension tables should then be replicated on each data store that hosts a partitioned fact table. In this way, a join between the fact table and any number of its dimension tables can be executed concurrently on each partition, without requiring multiple network hops to other members in the distributed system.

<a id="partition-scheme"></a>
## Applying Partitioning Scheme

<a id="collocated-joins"></a>
**Collocated Joins**</br>
Collocating frequently joined partitioned tables is the best practice to improve the performance of join queries. When two tables are partitioned on columns and collocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData member. Therefore, in a join query, the join operation is performed on each node's  local data. 

If the two tables are not collocated, partitions with same column values for the two tables can be on different nodes thus requiring the data to be shuffled between nodes causing the query performance to degrade.

For an example on collocated joins, refer to [How to collocate tables for doing a collocated join](../howto.md#how-to-perform-a-collocated-join).

<a id="buckets"></a>
**Buckets**</br>
The total number of partitions is fixed for a table by the BUCKETS option. By default, there are 113 buckets. The value should be increased for a large amount of data that also determines the number of Spark RDD partitions that are created for the scan. For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data.</br>
Unit of data movement is a bucket, and buckets of collocated tables move together. When a new server joins, the  [-rebalance](../configuring_cluster/property_description.md#rebalance) option on the startup command-line triggers bucket rebalancing and the new server becomes the primary for some of the buckets (and secondary for some if REDUNDANCY>0 has been specified). </br>
There is also a system procedure [call sys.rebalance_all_buckets()](../reference/inbuilt_system_procedures/rebalance-all-buckets.md#sysrebalance_all_buckets) that can be used to trigger rebalance.
For more information on BUCKETS, refer to [BUCKETS](capacity_planning.md#buckets).

<a id="dimension"></a>
**Criteria for Column Partitioning**</br>
SnappyData partition is mainly for distributed and collocated joins. It is recommended to use a relevant dimension for partitioning so that all partitions are active and the query are executed concurrently.</br>
If only a single partition is active and is used largely by queries (especially concurrent queries) it means a significant bottleneck where only a single partition is active all the time, while others are idle. This serializes execution into a single thread handling that partition. Therefore, it is not recommended to use DATE/TIMESTAMP as partitioning.

<a id="redundancy"></a>
## Using Redundancy

REDUNDANCY clause of [CREATE TABLE](../reference/sql_reference/create-table.md) specifies the number of secondary copies you want to maintain for your partitioned table. This allows the table data to be highly available even if one of the SnappyData members fails or shuts down. 

A REDUNDANCY value of 1 is recommended to maintain a secondary copy of the table data. A large value for REDUNDANCY clause has an adverse impact on performance, network usage, and memory usage.

For an example of the REDUNDANCY clause refer to [Tables in SnappyData](../programming_guide.md#tables-in-snappydata).

<a id="overflow"></a>
## Overflow Configuration

In SnappyData, column tables by default overflow to disk.  For row tables, the use EVICTION_BY clause to evict rows automatically from the in-memory table based on different criteria.  

This allows table data to be either evicted or destroyed based on the current memory consumption of the server. Use the `OVERFLOW` clause to specify the action to be taken upon the eviction event. 

For persistent tables, setting this to 'true' will overflow the table evicted rows to disk based on the EVICTION_BY criteria. Setting this to 'false' will cause the evicted rows to be destroyed in case of eviction event.

Refer to [CREATE TABLE](../reference/sql_reference/create-table.md) link to understand how to configure OVERFLOW and EVICTION_BY clauses.

!!! Note: 
	The default action for OVERFLOW is to destroy the evicted rows.
