# SYS.SET_CRITICAL_HEAP_PERCENTAGE

Sets the percentage threshold of Java heap memory usage that triggers `LowMemoryException`s on a TIBCO ComputeDB data store. This procedure executes only on the local TIBCO ComputeDB data store member.

This procedure sets the percentage threshold of critical Java heap memory usage on the local TIBCO ComputeDB data store. If the amount of heap memory being used exceeds the percentage, the member will report LowMemoryExceptions during local or client put operations into heap tables. The member will also inform other members in the distributed system that it has reached the critical threshold. When a data store is started with the `-heap-size` option, the default critical threshold is 90%.

## Syntax

```pre
SYS.SET_CRITICAL_HEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

**PERCENTAGE**   
The percentage of used heap space that triggers `LowMemoryException`s on the local TIBCO ComputeDB data store.

## Example

This command sets the critical threshold for heap memory usage on the local TIBCO ComputeDB member to 99.9%:

```pre
snappy>call sys.set_critical_heap_percentage (99.9);
```


