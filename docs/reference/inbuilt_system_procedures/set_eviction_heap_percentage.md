# SYS.SET_EVICTION_HEAP_PERCENTAGE


Sets the percentage threshold of Java heap memory usage that triggers a SnappyData data store to perform LRU eviction on tables that are configured for LRU_HEAP eviction. This procedure executes only on the local SnappyData data store member.

This procedure sets the percentage threshold for evicting table data from the Java heap for the local SnappyData data store. When the used heap reaches the percentage, SnappyData begins to evict rows, using a LRU algorithm, from tables that are configured with LRU_HEAP eviction. The default eviction heap percentage is 81% of the critical heap percentage value.

## Syntax

```pre
SYS.SET_EVICTION_HEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

**PERCENTAGE**   
The percentage of used heap space that triggers eviction on the local SnappyData data store.

## Example

This command triggers eviction on any SnappyData member when the local member's heap usage reaches 90%:

```pre
call sys.set_eviction_heap_percentage (90);
```


