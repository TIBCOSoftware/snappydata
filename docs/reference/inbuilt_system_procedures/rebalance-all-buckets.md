# SYS.REBALANCE_ALL_BUCKETS

Rebalance partitioned table data on available SnappyData members.

## Syntax

```pre
SYS.REBALANCE_ALL_BUCKETS()
```

Rebalancing is a SnappyData member operation that affects partitioned tables created in the cluster. Rebalancing performs two tasks:

1.  If the partitioned table's redundancy setting is not satisfied, rebalancing does what it can to recover redundancy. <!-- See <mark>RowStore Link - To be confirmed [Making a Partitioned Table Highly Available](http://rowstore.docs.snappydata.io/docs/data_management/partitioning-ha.html)</mark>.-->

2.  Rebalancing moves the partitioned table's data buckets between host members as needed to establish the best balance of data across the distributed system.

For efficiency, when starting multiple members, trigger the rebalance a single time, after you have added all members.
<!--
<mark>[Rebalancing Partitioned Data on SnappyData Members](http://rowstore.docs.snappydata.io/docs/data_management/rebalancing_pr_data.html) </mark> provides additional information about the rebalancing operation.-->

## Example

```pre
snappy> call sys.rebalance_all_buckets();
```


