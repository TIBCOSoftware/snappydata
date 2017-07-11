<a id="manage-cores"></a>
## Concurrency and Management of Cores

Executing queries or code in SnappyData results in the creation of one or more Spark jobs. Each Spark job has multiple tasks. The number of tasks is determined by the number of partitions of the underlying data.
Concurrency in SnappyData is tightly bound with the capacity of the cluster, which means, the number of cores available in the cluster determines the number of concurrent tasks that can be run. 

The default setting is **CORES = 2 X number of cores on a machine**.

It is recommended to use 2 X number of cores on a machine. If more than one server is running on a machine, the cores should be divided accordingly and specified using the `spark.executor.cores` property.
`spark.executor.cores` is used to override the number of cores per server.

For example, for a cluster with 2 servers running on two different machines with  4 CPU cores each, a maximum number of tasks that can run concurrently is 16. </br> 
If a table has 17 partitions (buckets, for row or column tables), a scan query on this table creates 17 tasks. This means, 16 tasks runs concurrently and the last task will run when one of these 16 tasks has finished execution.

SnappyData uses an optimization method which clubs multiple partitions on a single machine to form a single partition when there are fewer cores available. This reduces the overhead of scheduling partitions. 

In SnappyData, multiple queries can be executed concurrently, if they are submitted by different threads or different jobs. For concurrent queries, SnappyData uses fair scheduling to manage the available resources such that all the queries get fair distribution of resources. 
 
For example: In the image below, 6 cores are available on 3 systems, and 2 jobs have 4 tasks each. Because of fair scheduling, both jobs get 3 cores and hence three tasks per job execute concurrently.

Pending tasks have to wait for completion of the current tasks and are assigned to the core that is first available.

When you add more servers to SnappyData, the processing capacity of the system increases in terms of available cores. Thus, more cores are available so more tasks can concurrently execute.

![Concurrency](../../Images/core_concurrency.png)