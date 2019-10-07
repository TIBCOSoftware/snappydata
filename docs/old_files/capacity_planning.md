<a id="cores"></a>
# Overview

The following topics are covered in this section:

* [Concurrency and Management of Cores](#manage-cores)

* [Memory Management: Heap and Off-Heap](#heap)

* [HA Considerations](#ha-consideration)

* [Important Settings](#buckets)

* [Operating System Settings](#os_setting)

* [Table Memory Requirements](#able-memory)

* [JVM Settings for SnappyData Smart Connector mode and Local mode](#jvm-settings)

<a id="manage-cores"></a>
## Concurrency and Management of Cores

Executing queries or code in SnappyData results in the creation of one or more Spark jobs. Each Spark job has multiple tasks. The number of tasks is determined by the number of partitions of the underlying data.
Concurrency in SnappyData is tightly bound with the capacity of the cluster, which means, the number of cores available in the cluster determines the number of concurrent tasks that can be run. 

The default setting is **CORES = 2 X number of cores on a machine**.

It is recommended to use 2 X number of cores on a machine. If more than one server is running on a machine, the cores should be divided accordingly and specified using the `spark.executor.cores` property.
`spark.executor.cores` is used to override the number of cores per server.

For example, for a cluster with 2 servers running on two different machines with  4 CPU cores each, a maximum number of tasks that can run concurrently is 16. </br> 
If a table has 16 partitions (buckets, for row or column tables), a scan query on this table creates 16 tasks. This means, 16 tasks runs concurrently and the last task will run when one of these 16 tasks has finished execution.

SnappyData uses an optimization method which clubs multiple partitions on a single machine to form a single partition when there are fewer cores available. This reduces the overhead of scheduling partitions. 

In SnappyData, multiple queries can be executed concurrently, if they are submitted by different threads or different jobs. For concurrent queries, SnappyData uses fair scheduling to manage the available resources such that all the queries get fair distribution of resources. 
 
For example: In the image below, 6 cores are available on 3 systems, and 2 jobs have 4 tasks each. Because of fair scheduling, both jobs get 3 cores and hence three tasks per job execute concurrently.

Pending tasks have to wait for completion of the current tasks and are assigned to the core that is first available.

When you add more servers to SnappyData, the processing capacity of the system increases in terms of available cores. Thus, more cores are available so more tasks can concurrently execute.

![Concurrency](../Images/core_concurrency.png)


<a id="heap"></a>
## Memory Management: Heap and Off-Heap 

SnappyData is a Java application and by default supports on-heap storage. It also supports off-heap storage, to improve the performance for large blocks of data (eg, columns stored as byte arrays).

It is recommended to use off-heap storage for column tables. Row tables are always stored on on-heap. The [memory-size and heap-size](../configuring_cluster/property_description.md) properties control the off-heap and on-heap sizes of the SnappyData server process. 

![On-Heap and Off-Heap](../Images/on-off-heap.png)

The memory pool (off-heap and on-heap) available in SnappyData's cluster is divided into two parts – Execution and Storage memory pool. The storage memory pool as the name indicates is for the table storage. 

The amount of memory that is available for storage is 50% of the total memory but it can grow to 90% (or `eviction-heap-percentage` store property for heap memory if set) if the execution memory is unused.
This can be altered by specifying the `spark.memory.storageFraction` property. But, it is recommend to not change this setting. 

A certain fraction of heap memory is reserved for JVM objects outside of SnappyData storage and eviction. If the `critical-heap-percentage` store property is set then SnappyData uses memory only until that limit is reached and the remaining memory is reserved. If no critical-heap-percentage has been specified then it defaults to 90%. There is no reserved memory for off-heap.

SnappyData tables are by default configured for eviction which means, when there is memory pressure, the tables are evicted to disk. This impacts performance to some degree and hence it is recommended to size your VM before you begin. 

<!-- Default values for sizing the VM <mark> Sumedh</mark>-->

<a id="ha-consideration"></a>
## HA Considerations

High availability options are available for all the SnappyData components. 

**Lead HA** </br> 
SnappyData supports secondary lead nodes. If the primary lead becomes unavailable, one of  the secondary lead nodes takes over immediately. 
Setting up the secondary lead node is highly recommended because the system cannot function if the lead node is unavailable. Currently, the queries that are executing when the primary lead becomes unavailable, are not re-tried and have to be resubmitted.

**Locator**</br>  
SnappyData supports multiple locators in the cluster for high availability. 
It is recommended to set up multiple locators as, if a locator becomes unavailable, the cluster continues to be available. New members can however not join the cluster.
With multiple locators, clients notice nothing and the failover recovery is completely transparent.

**DataServer**</br> 
SnappyData supports redundant copies of data for fault tolerance. A table can be configured to store redundant copies of the data.  So, if a server is unavailable, and if there is a redundant copy available on some other server, the tasks are automatically retried on those servers. This is totally transparent to the user. 
However, the redundant copies double the memory requirements. If there are no redundant copies and a server with some data goes down, the execution of the queries fail and PartitionOfflineException is reported. The execution does not begin until that server is available again. 

##  Important Settings 
<a id="buckets"></a>
### Buckets

Bucket is the unit of partitioning for SnappyData tables. The data is distributed evenly across all the buckets. When a new server joins or an existing server leaves the cluster, the buckets are moved around for rebalancing. 

The number of buckets should be set according to the table size. 

The default number of buckets in the SnappyData cluster mode is 128. In the local mode it is cores*2, subject to a maximum of 64 buckets and a minumum of 8 buckets.

If there are more buckets in a table than required, it means there is fewer data per bucket. For column tables, this may result in reduced compression that SnappyData achieves with various encodings. 
Similarly, if there are not enough buckets in a table, not enough partitions are created while running a query and hence cluster resources are not used efficiently.
Also, if the cluster is scaled at a later point of time rebalancing may not be optimal.

For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data.  

### member-timeout

SnappyData efficiently uses CPU for running OLAP queries. In such cases, due to the amount of garbage generated, JVMs garbage collection can result in a system pause. These pauses are rare and can also be minimized by setting the off-heap memory. </br>
For such cases, it is recommended that the `member-timeout` should be increased to a higher value. This ensures that the members are not thrown out of the cluster in case of a system pause.</br>
The default value of `member-timeout` is: 5 sec. 

### spark.local.dir  

SnappyData writes table data on disk.  By default, the disk location that SnappyData uses is the directory specified using `-dir` option, while starting the member. 
SnappyData also uses temporary storage for storing intermediate data. The amount of intermediate data depends on the type of query and can be in the range of the actual data size. </br>
To achieve better performance, it is recommended to store temporary data on a different disk than the table data. This can be done by setting the `spark.local.dir` parameter.

<a id="os_setting"></a>
##  Operating System Settings 

For best performance, the following operating system settings are recommended on the lead and server nodes.

**Ulimit** </br> 
Spark and SnappyData spawn a number of threads and sockets for concurrent/parallel processing so the server and lead node machines may need to be configured for higher limits of open files and threads/processes. </br>
</br>A minimum of 8192 is recommended for open file descriptors limit and nproc limit to be greater than 128K. 
</br>To change the limits of these settings for a user, the /etc/security/limits.conf file needs to be updated. A typical limits.conf used for SnappyData servers and leads looks like: 

```
ec2-user          hard    nofile      163840 
ec2-user          soft    nofile      16384
ec2-user          hard    nproc       unlimited
ec2-user          soft    nproc       524288
ec2-user          hard    sigpending  unlimited
ec2-user          soft    sigpending  524288
```
* `ec2-user` is the user running SnappyData.	


**OS Cache Size**</br> 
When there is lot of disk activity especially during table joins and during eviction, the process may experience GC pauses. To avoid such situations, it is recommended to reduce the OS cache size by specifying a lower dirty ratio and less expiry time of the dirty pages.</br> 
The following are the typical configuration to be done on the machines that are running SnappyData processes. 

```
sudo sysctl -w vm.dirty_background_ratio=2
sudo sysctl -w vm.dirty_ratio=4
sudo sysctl -w vm.dirty_expire_centisecs=2000
sudo sysctl -w vm.dirty_writeback_centisecs=300
```

**Swap File** </br> 
Since modern operating systems perform lazy allocation, it has been observed that despite setting `-Xmx` and `-Xms` settings, at runtime, the operating system may fail to allocate new pages to the JVM. This can result in process going down.</br>
It is recommended to set swap space on your system using the following commands.

```
# sets a swap space of 32 GB
sudo dd if=/dev/zero of=/var/swapfile.1 bs=1M count=32768
sudo chmod 600 /var/swapfile.1
sudo mkswap /var/swapfile.1
sudo swapon /var/swapfile.1
```
<a id="table-memory"></a>
## Table Memory Requirements

SnappyData column tables encodes data for compression and hence require memory that is less than or equal to the on-disk size of the uncompressed data. If the memory-size is configured (i.e. off-heap is enabled), the entire column table is stored in off-heap memory. 

SnappyData row tables memory requirements have to be calculated by taking into account row overheads. Row tables have different amounts of heap memory overhead per table and index entry, which depends on whether you persist table data or configure tables for overflow to disk.

| TABLE IS PERSISTED?	 | OVERFLOW IS CONFIGURED?	 |APPROXIMATE HEAP OVERHEAD |
|--------|--------|--------|
|No|No|64 bytes|
|Yes|No|120 bytes|
|Yes|Yes|152 bytes|

!!! Note
	For a persistent, partitioned row table, SnappyData uses an additional 16 bytes per entry used to improve the speed of recovering data from disk. When an entry is deleted, a tombstone entry of approximately 13 bytes is created and maintained until the tombstone expires or is garbage-collected in the member that hosts the table. (When an entry is destroyed, the member temporarily retains the entry to detect possible conflicts with operations that have occurred. This retained entry is referred to as a tombstone.)
    
    
| TYPE OF INDEX ENTRY | APPROXIMATE HEAP OVERHEAD |
|--------|--------|
|New index entry     |80 bytes|
|First non-unique index entry|24 bytes|
|Subsequent non-unique index entry|8 bytes to 24 bytes*|

_If there are more than 100 entries for a single index entry, the heap overhead per entry increases from 8 bytes to approximately 24 bytes._

<a id="jvm-settings"></a>
## JVM Settings for SnappyData Smart Connector mode and Local mode 

For SnappyData Smart Connector mode and local mode, we recommend the following JVM settings for optimal performance:

```-XX:-DontCompileHugeMethods -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4k```