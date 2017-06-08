## Capacity Planning

The first rule to observe when planning is to know that no one size fits all capacity planning, as different customers have different requirement and the best solution is to create a plan that is most suitable for your environment. Therefore, capacity planning has been a critical component of successful implementations.

### Concurrency and Management of Cores

**Cores**

Within SnappyData, multiple queries of SnappyData jobs may be running concurrently if they were submitted by different threads.

A query is executed using one or more Spark jobs. Each Spark job has multiple tasks. The number of tasks is determined by the number of partitions of the data.  The number of cores available in the cluster determines the number of concurrent tasks that can run at the same time. For example, two servers with 4 cores each can execute 16 tasks, by default.

The default setting is **CORES = 2 X number of cores on a machine**. 

`spark.executor.cores` is used to specify number of cores per server.


SnappyData includes a fair scheduler to schedule resources within each SnappyContext.

The Fair scheduling method manages the available resources such that all the queries get fair resources. 

For example: In the image below, 6 cores are available on 3 systems, and there are 8 tasks. The tasks (1-6) are first assigned to the available cores. New tasks (task 5-6) have to wait for the completion of the current tasks and are assigned to the core is first available/completes current task.

As you add more servers to SnappyData, the processing capacity of the system increases in terms of available cores. More cores are available so more tasks can concurrently execute.

We recommend using 2 * number of cores on a machine, if there is a single SnappyData server running on this machine.

### Memory Management: Heap and Off-Heap 
SnappyData is a java application and supports on-heap storage. At the same time, to improve the performance for large chunks of data, it also support off-heap storage. If memory-size is specified, the column tables stores their data on off-heap and execution memory also goes to off heap. The row tables would always goes to on-heap.

Off-Heap memory is bounded by the memory-size parameter.

Total available memory (Total Heap) is the most important factor affecting GC performance.


The heap size can be controlled with the following properties:

heap-size
memory-size 
| Property | Description |
|--------|--------|
| Property | TEXT|



### HA Considerations

For all SnappyData components, high availability options are available. 

**Lead HA** - SnappyData supports a secondary lead nodes one of which takes over immediately when the primary lead dies. Secondary lead node is highly recommended when using SnappyData. Currently, the running query is not re-tried and has to be resubmitted.

**Locator**- SnappyData supports multiple locators in the cluster for high availability. If a locator dies, the cluster continues, but the new members won't be able to join and that is why we recommend having multiple locatos. With multiple locators, clients notice nothing since failover is completely transparent.

**DataServer**:
SnappyData supports redundant copies of data for fault tolerance. 
When a server dies, and if there is redundant copy available, the tasks are automatically retried on those. Transparent.

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