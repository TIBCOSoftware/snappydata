#SYS.SET_EVICTION_OFFHEAP_PERCENTAGE


Sets the percentage threshold of off-heap memory usage that triggers a SnappyData data store to perform LRU eviction on tables that are configured for LRU\_HEAP eviction. This procedure executes only on the local SnappyData data store member.

This procedure sets the percentage threshold for evicting table data from off-heap memory for the local SnappyData data store. When the off-heap memory usage reaches the percentage, SnappyData begins to evict rows, using a LRU algorithm, from tables that are configured with LRU\_HEAP eviction. <a href="../../overflow/configuring_data_eviction.html#configuring_data_eviction" class="xref" title="Use eviction settings to keep your table within a specified limit, either by removing evicted data completely or by creating an overflow table that persists the evicted data to a disk store.">Create a Table with Eviction Settings</a> describes the eviction process. The default eviction offheap percentage is 80% of the critical offheap percentage value.

##Syntax

``` pre
SYS.SET_EVICTION_OFFHEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

##PERCENTAGE   
The percentage of used off-heap memory that triggers eviction on SnappyData data stores for all tables configured with LRU\_HEAP eviction.

##Example

This command triggers eviction on any SnappyData member when the local member's off-heap memory usage reaches 90%:

``` pre
call sys.set_eviction_offheap_percentage (90);
```


