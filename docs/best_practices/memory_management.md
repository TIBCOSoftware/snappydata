# Overview
Spark executors and SnappyData in-memory store share the same memory space. SnappyData extends the Spark's memory manager providing a unified space for spark storage, execution and Snappydata column and row tables. This Unified MemoryManager smartly keeps track of memory allocations across Spark and the Store, elastically expanding one to make room for the other. Rather than pre-allocation strategy where Spark memory is independent of the store, SnappyData uses a unified strategy where all allocations come from a common pool.

SnappyData also monitors the JVM memory pools and avoids running into Out-of-memory conditions in most cases. You can configure the threshold for when data evicts to disk and the critical threshold for heap utilization. When the usage exceeds this critical threshold memory allocations within SnappyData fail, and an LowMemoryException error is thrown. This, however, safeguards the server from crashing due to OutOfMemoryException.

!!! Note: 
 **SnappyUnifiedMemoryManager** is used only in the embedded mode. Spark’s default memory manager is used for local mode and split cluster(smart connector) mode.

## Estimating memory size for column and row tables
Column tables uses compression by default and the amount of compression is quite dependent on the data itself. While, we commonly see compression of 50%, it is also possible to achieve much higher compression ratios when the data has many repeated strings or text. 
Row tables on the other hand consume more space than the original data size. There is a per row overhead in SnappyData. While this overhead varies and is dependent on the options configured on the Row table as a simple guideline we suggest you assume 100 bytes per row as overhead. 
Given this above description, it is clear that it isn't very straightforward to compute the memory requirements. What we recommend is to take a sample of the data set (as close as possible to your production data) and populate each of the tables. Make sure you create any required indexes also. Then, jot down the size estimates (in bytes) in the SnappyData Pulse dashboard. Simply extrapolate this number given the total number of records you anticipate to load or grow into for the memory requirements for your table. 

## Estimating memory size for execution
Spark and SnappyData also need room for execution. This includes memory for sorting, joining data sets, spark execution, application managed objects (e.g. a UDF allocating memory), etc. Most of these allocations will automatically overflow to disk. But, we strongly recommend allocating at least 5GB per DataServer/lead node for production systems that will run large scale analytic queries. 
(TODO: get more specific ??)

SnappyData uses JVM heap memory for most of its allocations. Only column tables can use off-heap storage (if configured). We suggest going throught the following options and configuring them appropriately based on the sizing estimates from above. 

## SnappyData Heap Memory
You can set the following heap memory configuration parameters:

|Parameter Name |Default Value|Description|
|--------|--------|--------|
|heap-size|4GB in Snappy embedded mode cluster|Max heap size which can be used by the JVM|
|critical-heap-percentage|90%|(TODO: I don't understand this descrip- Jags) This value suggests how much heap memory one needs to reserve for miscellaneous usage like UDFs, temporary garbage data. Beyond this point, SnappyData starts cancelling all jobs and queries. Critical percentage of 90 means, beyond 90% of heap usage jobs and queries will get cancelled. |
|eviction-heap-percentage|81|This percent determined when in memory table data would be evicted to disk. Beyond this Table rows are evicted in LRU fashion.|
|spark.memory.fraction|0.92|(TODO: I don't understand this descrip- Jags)A buffer area before critical heap memory is reached. |

SnappyData heap memory regions are divided into two parts called pools. Sizes of each pool are determined by the config parameters provided at boot time to each server(TODO: I don't understand this descrip .. thought each will elastically expand into the other- Jags).
The two pools are as below:

### Heap Storage Pool:
Most of Java heap allocation is accounted here. In most cases, the heap objects are long lived and survived young generation collections.
For example, when a row is inserted into a table or deleted, this pool accounts the memory size of that row. By default table data goes into heap memory.
Naturally, objects that are temporary in nature are not considered here. Also, it is difficult and costly  to do a precise estimation. Hence, this pool is an approximation of heap memory for objects which are going to be long lived.
Since precise estimation of heap memory is difficult, there is a health monitor thread running in the background. If the total heap as seen by JVM (and not SnappyUnifiedMemoryManager) exceeds `critical-heap-percentage` the database engine starts canceling jobs and queries and a LowMemoryException is reported. This also is an indication of heap pressure on the system.

### Heap Execution Pool:
During query execution, if some costly structure like an in memory map or an array is required, this pool is used for memory allocation. Some of the queries like HashJoin and aggregate queries creates expensive in memory maps. This pool is used to allocate such memory.

Two different memory pools are assigned for storage and execution fraction. The basic idea is that both can occupy the other's memory pool if the other pool has some capacity subject to following rules:

* The storage pool can grow to execution pool if execution pool is having some capacity, but not beyond max_storage_size.

* If the storage pool cannot borrow from the executor pool, it can evict some of its own blocks to make space for incoming blocks.

* If storage pool has already grown into execution pool, execution pool will evict blocks from storage pool until it's earlier limit, that is, 50% demarcation is reached. Beyond that, the executor threads cannot evict blocks from the storage pool. If sufficient memory is not available, either they can fall back on disk overflow or wait until sufficient memory is available.

* If storage pool has some free memory execution pool can borrow that memory from storage pool during execution. The borrowed memory is returned back once execution is over. 

![Heap-Size](/Images/heap_size.png)

Below is a typical heap setting for SnappyData:

```
-heap-size = 20g -critical-heap-percentage=90 -eviction-heap-percentage=81
```

These values derive different memory region sizes.

```
Reserved_Heap_Memory => 20g * (1 - 0.9) = 2g ( 0.9 being derived from critical_heap_percentage)
Heap_Memory_Fraction => (20g - Reserved_Memory) *(0.92) = 16.5
Heap_Storage_Pool_Size => 16.5 * (0.5) = 8.25
Heap_Execution_Pool_Size => 16.5 * (0.5) = 8.25
Heap_Max_Storage_pool_Size => 16.5 * 0.81 = 13.4 ( 0.81 derived from eviction_heap_percentage)
```

## SnappyData Off-Heap Memory 
Alongside heap memory, SnappyData can also be configured with off-heap memory. If configured, column table data, as well as many of the execution structures use off-heap memory. For a serious installation, off-heap setting is recommended. However several artifacts in the product need heap memory, so some minimum heap size is also required for this.

| Parameter Name | Default Value | Description	 |
|--------|--------|--------|
|memory-size|0 ( OFF_HEAP not used by default)	|Total off-heap memory which will be used by SnappyData.|

Similar to heap pools, off-heap pools are also divided between off-heap storage pool and off-heap execution pool. The rules of borrowing memory from each other also remains same.

![Off-Heap](/Images/off_heap_size.png)
A typical off-heap setting for SnappyData will look as below: 

```
-heap-size = 4g -memory-size=16g -critical-heap-percentage=90 -eviction-heap-percentage=81
```

These values derive different memory region sizes.

```
Reserved_Memory ( Heap Memory) => 4g * (1 - 0.9) = 400m ( 0.9 being derived from critical_heap_percentage)
Memory_Fraction ( Heap Memory) => (4g - Reserved_Memory) *(0.92) = 3.3g
Heap Storage_Pool_Size => 3.3 * (0.5) = 1.65
Heap Execution_Pool_Size => 3.3 * (0.5) = 1.65
Max_Heap_Storage_pool_Size => 3.3g * 0.81 = 2.6 ( 0.81 derived from eviction_heap_percentage)


Off-Heap Storage_Pool_Size => 16g * (0.5) = 8g
Heap Execution_Pool_Size => 16g * (0.5) = 8g
Max_Off_Heap_Storage_pool_Size => 16g * 0.9 = 14.4 ( 0.9 System default)
```
