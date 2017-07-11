<a id="heap"></a>
## Memory Management: Heap and Off-Heap 

SnappyData is a Java application and by default supports on-heap storage. It also supports off-heap storage, to improve the performance for large blocks of data (eg, columns stored as byte arrays).

It is recommended to use off-heap storage for column tables. Row tables are always stored on on-heap. The [memory-size and heap-size](../configuring_cluster/property_description.md) properties control the off-heap and on-heap sizes of the SnappyData server process. 

![On-Heap and Off-Heap](../../Images/on-off-heap.png)

The memory pool (off-heap and on-heap) available in SnappyData's cluster is divided into two parts – Execution and Storage memory pool. The storage memory pool as the name indicates is for the table storage. 

The amount of memory that is available for storage is 50% of the total memory but it can grow to 90% (or `eviction-heap-percentage` store property for heap memory if set) if the execution memory is unused.
This can be altered by specifying the `spark.memory.storageFraction` property. But, it is recommend to not change this setting. 

A certain fraction of heap memory is reserved for JVM objects outside of SnappyData storage and eviction. If the `critical-heap-percentage` store property is set then SnappyData uses memory only until that limit is reached and the remaining memory is reserved. If no critical-heap-percentage has been specified then it defaults to 90%. There is no reserved memory for off-heap.

SnappyData tables are by default configured for eviction which means, when there is memory pressure, the tables are evicted to disk. This impacts performance to some degree and hence it is recommended to size your VM before you begin. 