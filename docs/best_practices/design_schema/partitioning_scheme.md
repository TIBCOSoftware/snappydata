<a id="partition-scheme"></a>
## Applying Partitioning Scheme

<a id="collocated-joins"></a>
**Colocated Joins**</br>
Colocating frequently joined partitioned tables is the best practice to improve the performance of join queries. When two tables are partitioned on columns and colocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData member. Therefore, in a join query, the join operation is performed on each node's  local data. 

If the two tables are not colocated, partitions with same column values for the two tables can be on different nodes thus requiring the data to be shuffled between nodes causing the query performance to degrade.

For an example on colocated joins, refer to [How to colocate tables for doing a colocated join](../../howto.md#how-to-perform-a-collocated-join).

<a id="buckets"></a>
**Buckets**</br>
The total number of partitions is fixed for a table by the BUCKETS option. By default, there are 113 buckets. The value should be increased for a large amount of data that also determines the number of Spark RDD partitions that are created for the scan. For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data.</br>
Unit of data movement is a bucket, and buckets of colocated tables move together. When a new server joins, the  [-rebalance](../../configuring_cluster/property_description.md#rebalance) option on the startup command-line triggers bucket rebalancing and the new server becomes the primary for some of the buckets (and secondary for some if REDUNDANCY>0 has been specified). </br>
There is also a system procedure [call sys.rebalance_all_buckets()](../../reference/inbuilt_system_procedures/rebalance-all-buckets.md#sysrebalance_all_buckets) that can be used to trigger rebalance.
For more information on BUCKETS, refer to [BUCKETS](../setting_up_cluster/important_setting.md#buckets).

<a id="dimension"></a>
**Criteria for Column Partitioning**</br>
SnappyData partition is mainly for distributed and colocated joins. It is recommended to use a relevant dimension for partitioning so that all partitions are active and the query are executed concurrently.</br>
If only a single partition is active and is used largely by queries (especially concurrent queries) it means a significant bottleneck where only a single partition is active all the time, while others are idle. This serializes execution into a single thread handling that partition. Therefore, it is not recommended to use DATE/TIMESTAMP as partitioning.

