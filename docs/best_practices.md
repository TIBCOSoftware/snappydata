# Best Practices for Deploying and Managing SnappyData

<!--
The following topics are covered in this section:
* [Using Index](best_practices/using_index.md)

* [Using Collocated joins](best_practices/collocated_joins.md)

* [Physical Designing with SnappyData](best_practices/physical_design.md)

* [Using Row vs Column Table](best_practices/use_row_column_table.md)

* [Right Affinity Mode to Use](best_practices/affinity_mode_to_use.md)

* [Capacity Planning](best_practices/capacity_planning.md)

* [Preventing Disk Full Errors](best_practices/prevent_disk_full_errors.md)

* [Design Database and Schema](best_practices/design_schema.md)

-->

## Overview
This section presents advice on the optimal way to use SnappyData. It also gives you practical advice to assist you in configuring SnappyData and efficiently analysing your data.

## Architectural Considerations

To determine the configuration that is best suited for your environment some experimentation and testing is required involving representative data and workload.

Before we start, we have to make some assumptions:

1. Data will be in memory: <mark>How much in memory.</mark>

2. Concurrency:  <mark>Short running queries or long running queries.</mark>

## Capacity Planning

The first rule to observe when planning is to know that no one size fits all capacity planning, as different customers have different requirement and the best solution is to create a plan that is most suitable for your environment. Therefore, capacity planning has been a critical component of successful implementations.

Capacity planning involves:

* The volume of your data

* The retention policy of the data (how much you hang on to)

* The kinds of workloads you have (data science is CPU intensive, whereas generalized analytics is heavy on I/O)

* The storage mechanism for the data (whether you’re using compression, containers, etc.)

* How many nodes are required

* The capacity of each node in terms of CPU

* The capacity of each node in terms of memory

### Management of cores

Number of cores = Concurrent tasks as executor can run 

#### Cores recommendation 
We recommend using **2 X number of cores** on a machine. If you are starting 3 servers on a machine, divide 2 X number of cores between those machines.

`spark-executor-cores` is same for all servers.

Concurrency is fair scheduled.

#### Number of tasks per node

First, let's figure out the number of tasks per node.

Usually, the count is 1 core per task. If the job is not too heavy on the CPU, the number of tasks can be greater than the number of cores.

Example: 12 cores, jobs use ~75% of CPU

Let's assign free slots= 14 (slightly greater than the number of cores is a good rule of thumb), 

maxMapTasks=8, maxReduceTasks=6.

### Off-Heap and On-Heap

SnappyData supports two distinct data eviction policies
Memory management is the process of allocating new objects and removing unused objects, to make space for those new object allocations. Java objects reside in an area called the heap. The heap is created when the JVM starts up and may increase or decrease in size while the application runs.

When the heap becomes full, garbage is collected. During the garbage collection, objects that are no longer used are cleared, thus making space for new objects.

The JVM allocates Java heap memory from the OS and then manages the heap for the Java application. When an application creates a new object, the JVM sub-allocates a contiguous area of heap memory to store it. An object in the heap that is referenced by any other object is "live," and remains in the heap as long as it continues to be referenced. Objects that are no longer referenced are garbage and can be cleared out of the heap to reclaim the space they occupy. The JVM performs a garbage collection (GC) to remove these objects, reorganizing the objects remaining in the heap.

<mark>Column table data is on off-heap. Row table data on-heap.

Temporary memory for execution will also be off-heap. There are settings to control. On-Heap is limited by JVM size. Off-heap grows until system memory is exhausted. 
</mark>

Off-Heap memory allows your cache to overcome lengthy JVM Garbage Collection (GC) pauses when working with large heap sizes by caching data outside of main Java Heap space, but still in RAM.

On-heap caching is useful for scenarios when you do a lot of cache reads on server nodes that work with cache entries in the binary form or invoke cache entries' deserialization. For instance, this might happen when a distributed computation or deployed service gets some data from caches for further processing.

The heap size can be controlled with the following properties:

| Property | Description |
|--------|--------|
|`-heap-size`|Sets the heap size for the Java VM, using SnappyData default resource manager settings.</br>
For example, -heap-size=1024m. </br>If you use the -heap-size option, by default SnappyData sets the critical-heap-percentage to 90% of the heap size, and the eviction-heap-percentage to 80% of the 'critical-heap-percentage' (which when using defaults, corresponds to 72% of the heap size). </br>SnappyData also sets resource management properties for eviction and garbage collection if they are supported by the JVM.</br>**Note**: <mark> (TO BE CONFIRMED)</mark> </br>The -initial-heap and -max-heap parameters, used in the earlier SQLFire product, are no longer supported. </br>Use -heap-size instead.|
|`-off-heap-size`|Specifies the amount of off-heap memory to allocate on the server.</br>You can specify the amount in bytes (b), kilobytes (k), megabytes (m), or gigabytes (g). </br>For example, -off-heap-size=2g.|

<mark>Follow up with rishi/sumedh to find out exact settings for offheap and on heap and also recommendations.</mark>

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

### Encoding of column tables
We have dictionary encoding.

<mark>Hemant will check if integers are delta encoded. We are not efficient as parquet.</mark>

### Concurrency
Within SnappyData, multiple “jobs” may be running concurrently if they were submitted by different threads. This is common if your application is serving requests over the network. SnappyData includes a fair scheduler to schedule resources within each SnappyContext.

**Fair scheduler**: 
Fair scheduling is a method of assigning resources to jobs such that all jobs get, on average, an equal share of resources over time. When there is a single job running, that job uses the entire cluster. When other jobs are submitted, tasks slots that free up are assigned to the new jobs, so that each job gets roughly the same amount of CPU time.
When a query is executed, the query is routed to the lead node which then communicates with the executors and completes the action. The second function of the lead node is to accept Snappy jobs via the REST API and maintain a queue of such jobs and execute them using the specified scheduling mechanism (fair scheduler or FIFO?)

<mark>The Fair Scheduler contains the following configuration</mark>

## Sizing Process
Size recommendation based on queries and data is being taken care of by Volume testing. 

<mark>Hemant to follow up! Will have to talk about how much data he would like to be only on disk.<mark>

## GC settings 

<mark>Hemant to follow up</mark>

**Lead sizing** 
Lead (better to have it on a single node) does not need memory for data but it should have enough memory to collect query results. To handle this, blocks of results are pushed on disk. select * from a table can cause lead OOM. 

Lead is a scheduler. 

For long queries, the lead would be free most of the time. For short queries (for e.g. row table point updates. Column table queries that can be pruned to a single partition), lead is relatively busy.
**Locator sizing **
Not compute heavy. Not memory heavy.

## Database design (schema definition):

### Table structure

### Redundancy

### Row table/column table/replicated tables

### Persistent

### Overflow configuration

## RECOVERY/RESTART
ORDER OF RESTART 

## Affinity Mode to Use
The real labor of the big data resource involved collecting and organizing complex data so that the resource would be ready for your query.

## Loading data from External Repositories

## Back up and Restore

<mark>
ASSESS 

PLAN 

DESIGN 

IMPLEMENT
</mark>

<mark>Jags Comment </mark>

Loading data from external repositories like S3, CSV, HDFS, RDBMS and NoSQL
 (Should be a separate section if we have enough content ... )
  - how to load from S3, CSV, HDFS using SQL external tables
     -- How to deal with missing data or ignoring errors (CSV)
  - How to manage parallelism for loading
  - how to transform datatypes before loading (using SQL Cast, expressions)
  - How to transform and enrich while loading (Using spark program)
  - How to load an entire RDB (from a schema)? Example that reads all table names from catalog and then execute select from external JDBC source table on each table?
  - How to load data from NoSQL stores like cassandra (give Cassandra example using Spark-cassandra package)
  
  Snapshotting state from SnappyData to external repositories like S3,CSV/parquet, RDBMS and NoSQL
   - We recommend backing up state from SnappyData into a repository like HDFS or S3 in Parquet format. 
   - Repeat the How-tos from the above section
   
 Optimizing Query performance
   - Explaining 'Explain plan' 
   - 'Where is the time spent' using SnappyData Pulse
   - Tuning data shuffling costs and configuring /tmp for optimal performance
   - Tuning memory and eviction settings
   - Tuning JVM heap settings and GC settings
   - Tuning Offheap settings
   - Tuning broadcast , hash join thresholds
   - Tips for optimizing query performance when using the Smart connector
     -- e.g. when to use JDBC connection to route entire query vs only applying filters and projections    
   
 How to plan for large data volumes (maybe the same as capacity planning)
   - Sumedh, Hemant, etc are working on this
   - Computing heap requirements for different table types
   - a step-by-step guide to configuring heap and GC settings
   - Why we recommend SSD for disk or at least for /tmp
      -- crucial to speed up recovery when the cluster goes down , for instance. 
   - Figuring out GC problems and tuning pre-production
   - When to use additional compute nodes when resources are scarse or to manage concurrency
     -- i.e. suggest Apps use the smart connector and run very expensive batch jobs in a compute cluster
  
 When should I use Smart connector? (same as 'Right affinity mode' above)
 
 Managing the cluster
   - Starting, stopping the cluster .. configuring ssh and local disks?
   - backup, recovery of SnappyData disk stores, Snapshots to external repos.
   - Expanding the snappyData cluster at runtime -- rebalancing, etc
   - Linux level configuration: ulimit settings, etc. 

MISC
- Backups and Recovery
- online/offline backup and recovery -- needs testing
- redundancy and ensuring data availability for failures
- recovering from failures, revoking safely ensuring no loss of data
- last ditch recovery using data extractor; data redundancy implications
Troubleshooting and analysis
getting thread dumps from GUI or detailed stacks using SIGURG
observing logs for warning/severe/errors and decoding log messages
collating logs and stats using collect-debug-artifacts.sh
using VSD (can we make use of GemFire VSD docs?)
enabling/disabling logging levels/traces on-the-fly
 
