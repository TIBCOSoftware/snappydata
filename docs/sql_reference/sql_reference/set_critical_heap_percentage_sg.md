# SYS.SET_CRITICAL_HEAP_PERCENTAGE_SG


Sets the percentage threshold of Java heap memory usage that triggers `LowMemoryException`s on one or more RowStore server groups.

This procedure sets the percentage threshold of critical Java heap memory usage for all members of one or more server groups, or for all data stores in the RowStore cluster. If the amount of heap memory being used exceeds the percentage, the data stores will report `LowMemoryException`s during local or client put operations into heap tables. The affected members will also inform other members in the distributed system that they have reached the critical threshold. When a data store is started with the `-heap-size` option, the default critical threshold is 90%.

##Syntax

``` pre
SYS.SET_CRITICAL_HEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
IN SERVER_GROUPS VARCHAR(32762)
)
```

**PERCENTAGE**   
The percentage of used heap space that triggers `LowMemoryException`s on the RowStore data store.

**SERVER\_GROUPS   **
A comma-separated list of server groups on which to apply the heap percentage setting. If you specify NULL, the command is distributed to all data stores (irrespective of defined server groups).

##Example

This command sets the critical threshold for heap memory usage on all RowStore members to 99.9%:

``` pre
call sys.set_critical_heap_percentage_sg (99.9, null);
```


