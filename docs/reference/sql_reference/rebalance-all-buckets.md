# SYS.REBALANCE_ALL_BUCKETS

Rebalance partitioned table data on available SnappyData members.

##Syntax

``` pre
SYS.REBALANCE_ALL_BUCKETS()
```

Rebalancing is a SnappyData member operation that affects partitioned tables created created in the cluster. Rebalancing performs two tasks:

1.  If the a partitioned table's redundancy setting is not satisfied, rebalancing does what it can to recover redundancy. See <a href="../../data_management/partitioning-ha.html#concept_6467556AE47D4C6E9D14007CEBA5092E" class="xref" title="Use the REDUNDANCY clause to specify a number of redundant copies of a table for each partition to maintain.">Making a Partitioned Table Highly Available</a>.
2.  Rebalancing moves the partitioned table's data buckets between host members as needed to establish the best balance of data across the distributed system.

For efficiency, when starting multiple members, trigger the rebalance a single time, after you have added all members.

<a href="../../data_management/rebalancing_pr_data.html#rebalancing_pr_data" class="xref" title="You can use rebalancing to dynamically increase or decrease your SnappyData cluster capacity, or to improve the balance of data across the distributed system.">Rebalancing Partitioned Data on SnappyData Members</a> provides additional information about the rebalancing operation.

##Example

``` pre
snappy> call sys.rebalance_all_buckets();
```


