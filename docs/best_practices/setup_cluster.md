# Handling concurrency: Computing the CPU Cores Required

## How are low latency vs Analytic jobs handled?
Unlike Spark, SnappyData is able to distinguish requests that are very cheap (low latency) vs requests that require a lot of computational resources (high latency) and best done by a resource scheduler that can balance the needs of many contending users/threads. 

For instance, when a SQL client executes a ‘fetch by primary key’ query there is no need to involve any scheduler or spawn many tasks for such a simple request. The request is immediately delegated to the data node (single thread) and response directly sent to the requesting client (probably within a few milliseconds). In the current version of the product, all query requests that filter on a primary key, a set of keys or can directly filter using an index will be executed without routing to the SnappyData scheduler. Only Row tables can have primary keys or indexes. All DML (updates, inserts, deletes) executed from a SQL client (JDBC, ODBC) are directly routed to the responsible data node (partition) and do not involve the scheduler. 

When the above conditions are not met, the request is routed to the ‘Lead’ node where the Spark plan is generated and ‘jobs’ are scheduled for execution. The scheduler uses a FAIR scheduling algorithm for higher concurrency. i.e. all concurrent jobs will be executed in a round robin manner. 

Here is something that is important to understand - Each Job is made up of one or more stages and the planning phase computes the number of  parallel tasks for the stage. Tasks from scheduled Jobs are then allocated to the logical cores available (see below for how it is computed) until all cores are allocated. A round-robin algorithm picks a task from Job1, a task from Job2 and so on. If more cores are available, the second task from Job1 is picked and the cycle continues. But, there are circumstances a single Job can completely consume all cores. 

For instance, when all cores are available if a large loading job gets scheduled it receives all available cores. And, it is possible that each of the task is long running. During this time, if other concurrent jobs show up, none of the executing tasks is pre-empted. 
(todo: graphic to depict this … links to Spark scheduling docs)

!!! Note 
	This above scheduling logic is applicable only when queries are fully managed by SnappyData cluster. When running your application using the smart connector, each task running in the spark cluster will directly access the store partitions. 

## How is the Number of cores for a Job Computed?

Executing queries or code in SnappyData results in the creation of one or more Spark jobs. Each Spark job has multiple tasks. The number of tasks is determined by the number of partitions of the underlying data.
Concurrency in SnappyData is tightly bound with the capacity of the cluster, which means, the number of cores available in the cluster determines the number of concurrent tasks that can be run. 

The default setting is **CORES = 2 X number of cores on a machine**.

It is recommended to use 2 X number of cores on a machine. If more than one server is running on a machine, the cores should be divided accordingly and specified using the `spark.executor.cores` property.
`spark.executor.cores` is used to override the number of cores per server.

For example, for a cluster with 2 servers running on two different machines with  4 CPU cores each, a maximum number of tasks that can run concurrently is 16. </br> 
If a table has 17 partitions (buckets, for row or column tables), a scan query on this table creates 17 tasks. This means, 16 tasks run concurrently and the last task will run when one of these 16 tasks has finished execution.

SnappyData uses an optimization method which clubs multiple partitions on a single machine to form a single partition when there are fewer cores available. This reduces the overhead of scheduling partitions. 

In SnappyData, multiple queries can be executed concurrently, if they are submitted by different threads or different jobs. For concurrent queries, SnappyData uses fair scheduling to manage the available resources such that all the queries get a fair distribution of resources. 
 
For example: In the image below, 6 cores are available on 3 systems, and 2 jobs have 4 tasks each. Because of fair scheduling, both jobs get 3 cores and hence three tasks per job execute concurrently.

Pending tasks have to wait for completion of the current tasks and are assigned to the core that is first available.

When you add more servers to SnappyData, the processing capacity of the system increases in terms of available cores. Thus, more cores are available so more tasks can concurrently execute.

![Concurrency](../Images/core_concurrency.png)

<TODO: When we list properties it should link to section on how/where such properties are specified …e.g. through command line, in config file, etc>

### Configuring the scheduler pools for concurrency
SnappyData out of the box comes configured with two execution pools. See above section ‘How are low latency vs Analytic jobs handled?’ first to understand how SnappyData handles scheduling. 
1) Default pool: This is the pool that is used for all high latency requests
2) Low-latency pool: This pool is automatically used when SnappyData determines a request to be “low latency”. Applications can explicitly configure the use of this pool using a SQL command ‘set snappydata.scheduler.pool=lowlatency’. 
This can also be set on a Spark SnappySession instance or using the config files in <product>/conf directory. 
TODO: is this pool notion described else where? if not, we need to mention how much resources each pool is allocated and how does one turn OFF the low latency pool (so all resources can be utilized)? Set this option in any perf tests that are built-in (e.g. the gettingStarted Perf test).

### Using a partitioning strategy to increasing concurrency
The best way to increasing concurrency is to design your schema such that you minimize the need to run your queries across many partitions. The common strategy is to understand your application patterns and choose a partitioning strategy such that queries often target a specific partition. Such queries will be pruned to a single node and SnappyData automatically optimizes such queries to use a single task. 
See section on ‘How to design your schema’ for more details. 


**TODO**: Shyja, do the following:
(1) some of the stuff in concurrency and management of cores is relevant. Needs to go into above. 
(5) Table memory reqs ... already covered elsewhere

