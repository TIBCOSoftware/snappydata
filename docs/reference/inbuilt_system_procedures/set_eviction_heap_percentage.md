# SYS.SET_EVICTION_HEAP_PERCENTAGE


Sets the percentage threshold of Java heap memory usage that triggers a TIBCO ComputeDB data store to perform LRU eviction on tables that are configured for LRU_HEAP eviction. This procedure executes only on the local TIBCO ComputeDB data store member.

This procedure sets the percentage threshold for evicting table data from the Java heap for the local TIBCO ComputeDB data store. When the used heap reaches the percentage, TIBCO ComputeDB begins to evict rows, using a LRU algorithm, from tables that are configured with LRU_HEAP eviction. The default eviction heap percentage is 81% of the critical heap percentage value.

## Syntax

```pre
SYS.SET_EVICTION_HEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

**PERCENTAGE**   
The percentage of used heap space that triggers eviction on the local TIBCO ComputeDB data store.

## Example

This command triggers eviction on any TIBCO ComputeDB member when the local member's heap usage reaches 90%:

```pre
call sys.set_eviction_heap_percentage (90);
```


