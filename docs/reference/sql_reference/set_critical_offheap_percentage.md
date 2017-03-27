# SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE

Sets the percentage threshold of off-heap memory usage that triggers `LowMemoryException`s on a SnappyData data store. This procedure executes only on the local SnappyData data store member.

This procedure sets the percentage threshold of critical off-heap memory usage on the local SnappyData data store. If the amount of off-heap memory being used exceeds the percentage, the member will report `LowMemoryException`s during local or client put operations into off-heap tables. The member will also inform other members in the distributed system that it has reached the critical threshold. When a data store is started with the `-off-heap-size` option, the default critical threshold is 90%.

##Syntax

``` pre
SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE (
IN PERCENTAGE REAL NOT NULL
)
```

**PERCENTAGE**   
The percentage of used off-heap space that triggers `LowMemoryException`s on the local SnappyData data store.

##Example

This command sets the critical threshold for off-heap memory usage on the local SnappyData member to 99.9%:

``` pre
call sys.set_critical_offheap_percentage (99.9);
```


