# SYS.CREATE_ALL_BUCKETS

Pre-allocate buckets for partitioned table data.

##Syntax

``` pre
SYS.CREATE_ALL_BUCKETS
(
IN TABLENAME LONG VARCHAR
)
```

**TABLENAME  **
The name of the partitioned table for which SnappyData will pre-allocate buckets.

By default, SnappyData assigns partitioned table buckets to individual data store members lazily, as rows are inserted to a partitioned table. However, if you rapidly load data into a partitioned table using concurrent threads, data can be inserted to the table before new bucket assignments are distributed to available data stores. This can result in partitioned table data that is skewed among data stores in your cluster.

As a best practice, use SYS.CREATE\_ALL\_BUCKETS(*'table-name*') before you load partitioned table data to ensure that the data is evenly distributed among available data stores.

##Example

``` pre
snappy> call sys.create_all_buckets('mynewpartitionedtable');
```


