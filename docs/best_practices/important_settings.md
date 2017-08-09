# Important Settings <a id="important-settings"></a>

Resource allocation is an important for the execution of any job. If not configured correctly, the job can consume entire cluster resources and cause problems like the execution fails or memory related problems. This section provides guidelines for configuring the following important settings:

* [Buckets] (#buckets)

* [member-timeout](#member-timeout)

* [spark.local.dir](#spark-local-dir)

* [executor-memory](#executor-memory)

* [Operating System Settings](#os_setting)

* [Table Memory Requirements](#table-memory)

* [JVM Settings for SnappyData Smart Connector mode and Local mode](jvm-settings)

<a id="buckets"></a>
## Buckets

A bucket is the unit of partitioning for SnappyData tables. The data is distributed evenly across all the buckets. When a new server joins or an existing server leaves the cluster, the buckets are moved around for rebalancing. 

The number of buckets should be set according to the table size. By default, there are 113 buckets for a table. 
If there are more buckets in a table than required, it means there is less data per bucket. For column tables, this may result in reduced compression that SnappyData achieves with various encodings. 
Similarly, if there are not enough buckets in a table, not enough partitions are created while running a query and hence cluster resources are not used efficiently.
Also, if the cluster is scaled at a later point of time rebalancing may not be optimal.

For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data.  

<a id="member-timeout"></a>
## member-timeout

The default [member-timeout](../configuring_cluster/property_description.md#member-timeout) in SnappyData cluster is 30 seconds. The default `spark.network.timeout` is 120 seconds and `spark.executor.heartbeatInterval` is 10 seconds as noted in the [Spark documents](https://spark.apache.org/docs/latest/configuration.html). </br> 
If applications require node failure detection to be faster, then these properties should be reduced accordingly (`spark.executor.heartbeatInterval` but must always be much lower than `spark.network.timeout` as specified in the Spark Documents). </br>
However, note that this can cause spurious node failures to be reported due to GC pauses. For example, the applications with reduced settings need to be resistant to job failures due to GC settings.

<a id="spark-local-dir"></a>
## spark.local.dir  

SnappyData writes table data on disk.  By default, the disk location that SnappyData uses is the directory specified using `-dir` option, while starting the member. 
SnappyData also uses temporary storage for storing intermediate data. The amount of intermediate data depends on the type of query and can be in the range of the actual data size. </br>
To achieve better performance, it is recommended to store temporary data on a different disk (SSD) than the table data. This can be done by setting the `spark.local.dir` parameter.

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
When there is a lot of disk activity especially during table joins and during an eviction, the process may experience GC pauses. To avoid such situations, it is recommended to reduce the OS cache size by specifying a lower dirty ratio and less expiry time of the dirty pages.</br> 
The following are the typical configuration to be done on the machines that are running SnappyData processes. 

```
sudo sysctl -w vm.dirty_background_ratio=2
sudo sysctl -w vm.dirty_ratio=4
sudo sysctl -w vm.dirty_expire_centisecs=2000
sudo sysctl -w vm.dirty_writeback_centisecs=300
```

**Swap File** </br> 
Since modern operating systems perform lazy allocation, it has been observed that despite setting `-Xmx` and `-Xms` settings, at runtime, the operating system may fail to allocate new pages to the JVM. This can result in the process going down.</br>
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

SnappyData column tables encode data for compression and hence require memory that is less than or equal to the on-disk size of the uncompressed data. If the memory-size is configured (i.e. off-heap is enabled), the entire column table is stored in off-heap memory. 

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

If there are more than 100 entries for a single index entry, the heap overhead per entry increases from 8 bytes to approximately 24 bytes.

<a id="jvm-settings"></a>
## JVM Settings for SnappyData Smart Connector mode and Local mode 

For SnappyData Smart Connector mode and local mode, we recommend the following JVM settings for optimal performance:

```-XX:-DontCompileHugeMethods -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4k```

!!! Note:
	The `executor-memory` controls the amount of memory to use per executor process. The default value set by Spark is is 1g per executor. Set this property in the spark-defaults.conf
