
# SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE_SG

Sets the percentage threshold of off-heap memory usage that triggers `LowMemoryException`s on one or more SnappyData server groups.

This procedure sets the percentage threshold of critical off-heap memory usage for all members of one or more server groups, or for all data stores in the SnappyData cluster. If the amount of off-heap memory being used exceeds the percentage, the data stores will report `LowMemoryException`s during local or client put operations into off-heap tables. The affected members will also inform other members in the distributed system that they have reached the critical threshold. When a data store is started with the `-off-heap-size` option, the default critical threshold is 90%.

##Syntax

``` pre
SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
IN SERVER_GROUPS VARCHAR(32762)
)
```

**PERCENTAGE**   
The percentage of used off-heap space that triggers `LowMemoryException`s on the local SnappyData data store.

**SERVER\_GROUPS   **
A comma-separated list of server groups on which to apply the off-heap percentage setting. If you specify NULL, the command is distributed to all data stores (irrespective of defined server groups).

##Example

This command sets the critical threshold for off-heap memory usage on all SnappyData members to 99.9%:

``` pre
call sys.set_critical_offheap_percentage_sg (99.9, null);
```


