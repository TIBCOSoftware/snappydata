## Capacity Planning

The first rule to observe when planning is to know that no one size fits all capacity planning, as different customers have different requirement and the best solution is to create a plan that is most suitable for your environment. Therefore, capacity planning has been a critical component of successful implementations.

### Concurrency and Management of Cores

**Cores**

A processor core is a processing unit which receives and processes the assigned tasks. Processors can have a single core or multiple cores. The number of cores available in the processor determines the number of concurrent tasks that can run at the same time. For example, one single-core CPU can only perform one task at a time.

**Number of cores = Concurrent tasks as executor can run** </br>
Thus, the performance of the tasks are processor (CPU) and processor core capacity bound.

**Cores recommendation**</br>
We recommend the following core settings:
The default setting is **CORES = 2 X number of cores on a machine**. 

Ensure that the number of cores available on all the processers are the same. `spark-executor-cores` is same for all servers.

**Concurrency**</br>
Within SnappyData, multiple tasks may be running concurrently if they were submitted by different threads. This is common if your application is serving requests over the network. SnappyData includes a fair scheduler to schedule resources within each SnappyContext.

**Fair scheduling**</br> 
The Fair scheduling method manages the available resources such that all the assigned tasks are completed. It schedules jobs based on the availablity of resources (cores) and memory.
When a single task is assigned, the entire memory is available. When multiple taks are assiged, the tasks that are submitted first are executed and the tasks that are in the queue are assigned when the current tasks are completed.

For example: In the image below, 6 cores are available on 3 systems, and there are 8 tasks. The tasks (1-6) are first assigned to the available cores. New tasks (task 5-6) have to wait for the completion of the current tasks and are assigned to the core is first available/completes current task.

The Fair Scheduler contains the following configuration:

### Memory Management: Heap and Off-Heap 
SnappyData supports two distinct data eviction policies memory management is the process of allocating new objects and removing unused objects, to make space for those new object allocations. Java objects reside in an area called the heap. The heap is created when the JVM starts up and may increase or decrease in size while the application runs. Off-Heap memory is bounded only by the physcial memory. 

**Heap**</br>
Heap is managed memory and is more useful when _____________ . Row tables are stored in the heap memory. The JVM allocates Java heap memory from the OS and then manages the heap for the Java application. When an application access data, the JVM sub-allocates a contiguous area of heap memory to store it.
Data that is referenced by a tasks remains in the heap as long as it is in use, and when no longer referenced, the garbage collection (GC) removes these objects making more space in the heap for relevant data.

**Off-Heap**</br>
Off-Heap memory is bounded only by the physcial memory. It allows the cache to overcome lengthy JVM Garbage Collection (GC) pauses when working with large heap sizes by caching data outside of main Java Heap space, but still in RAM. Column tables are stored in the off-heap memory.

**Garbage Collection**</br>
The term garbage collection implies a collection of data that is no longer required. 
The main purpose of garbage collection is to delete all the objects that are either not in use, thus making more heap memory available.
In other words, when the heap becomes full, garbage is collected, and objects that are no longer used are cleared, thus making space for new objects.

Total available memory (Total Heap) is the most important factor affecting GC performance.


The heap size can be controlled with the following properties:

| Property | Description |
|--------|--------|
| Property | TEXT|

### HA Considerations
Tables can have redundant copies. So you need to decide how many redundant copies you want. In the case of server HA, there is always another server that can provide you data.

**Lead HA** - A secondary lead. Becomes primary when the first one dies. Currently, the running query is not re-tried and has to be resubmitted. But the future queries will be taken care of. 
When the lead node goes down, query routing and job submission cannot be done and currently running jobs will need to be re-executed. When there is a secondary lead node in the system, it takes over, the executors on the data servers restart and the secondary transparently resubmits the last set of jobs and query routing resumes. Driver HA is highly recommended when using SnappyData. With HA, the only thing the user would notice is that some partially completed jobs are re-run.

**Locator **- If a locator dies, the cluster continues, but the new members won't be able to join. 
The locator provides metadata about servers to ODBC and JDBC clients. Clients do not remain connected to the locator once they have the metadata. As a result of this design, locator failure does not affect the running system in any way. It does prevent new clients from joining the cluster. 
With HA turned on, clients notice nothing since failover is completely transparent. The use of secondary locators is highly recommended when using SnappyData

**DataServer**:
Adding a data server is supported from the snappy-shell. The new data server joins the cluster, and if the system is low on redundancy (not enough copies of data exist in memory), it automatically picks up storage for the tables it has defined in it. It starts an executor, connects to the lead node and new Snappy jobs recognize and utilize the new server.
Removing a data server is supported from the snappy-shell. 	
When a server is removed and if there is redundancy, the partitions on the removed server are automatically retried on the other servers where they are hosted. The running jobs pass eventually. Jobs running on the server are automatically restarted by the lead node.

## Sizing Process
Size recommendation based on queries and data is being taken care of by Volume testing. 

<mark>Hemant to follow up. Will have to talk about how much data should be only on disk.<mark>

## GC settings 

<mark>Hemant to follow up</mark>

**Lead sizing** 
Lead (better to have it on a single node) does not need memory for data but it should have enough memory to collect query results. To handle this, blocks of results are pushed on disk. select * from a table can cause lead OOM. 

Lead is a scheduler. 

For long queries, the lead would be free most of the time. For short queries (for e.g. row table point updates. Column table queries that can be pruned to a single partition), lead is relatively busy.
**Locator sizing **
Not compute heavy. Not memory heavy.		