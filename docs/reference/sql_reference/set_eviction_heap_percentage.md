# SYS.SET_EVICTION_HEAP_PERCENTAGE


Sets the percentage threshold of Java heap memory usage that triggers a RowStore data store to perform LRU eviction on tables that are configured for LRU\_HEAP eviction. This procedure executes only on the local RowStore data store member.

This procedure sets the percentage threshold for evicting table data from the Java heap for the local RowStore data store. When the used heap reaches the percentage, RowStore begins to evict rows, using a LRU algorithm, from tables that are configured with LRU\_HEAP eviction. <a href="../../overflow/configuring_data_eviction.html#configuring_data_eviction" class="xref" title="Use eviction settings to keep your table within a specified limit, either by removing evicted data completely or by creating an overflow table that persists the evicted data to a disk store.">Create a Table with Eviction Settings</a> describes the eviction process. The default eviction heap percentage is 80% of the critical heap percentage value.

##Syntax

``` pre
SYS.SET_EVICTION_HEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

**PERCENTAGE**   
The percentage of used heap space that triggers eviction on the local RowStore data store.

##Example

This command triggers eviction on any RowStore member when the local member's heap usage reaches 90%:

``` pre
call sys.set_eviction_heap_percentage (90);
```


