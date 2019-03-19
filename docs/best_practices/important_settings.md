# Important Settings <a id="important-settings"></a>

Resource allocation is important for the execution of any job. If not configured correctly, the job can consume the entire clusters resources and cause execution failure because of memory and other related problems.

This section provides guidelines for configuring the following important settings:

* [Buckets](#buckets)

* [member-timeout](#member-timeout)

* [spark.local.dir](#spark-local-dir)

* [Operating System Settings](#os_setting)

* [SnappyData Smart Connector mode and Local mode Settings](#smartconnector-local-settings)

* [Code Generation and Tokenization](#codegenerationtokenization)

<a id="buckets"></a>
## Buckets

A bucket is the smallest unit of in-memory storage for SnappyData tables. Data in a table is distributed evenly across all the buckets. When a new server joins or an existing server leaves the cluster, buckets are moved around to ensure that the data is balanced across the nodes where the table is defined.

The default number of buckets in the SnappyData cluster mode is 128. In the local mode, it is cores*2, subject to a maximum of 64 buckets and a minimum of 8 buckets.

The number of buckets has an impact on query performance, storage density, and ability to scale the system as data volumes grow.

If there are more buckets in a table than required, it means there is less data per bucket. For column tables, this may result in reduced compression that SnappyData achieves with various encodings. Similarly, if there are not enough buckets in a table, not enough partitions are created while running a query and hence cluster resources are not used efficiently. Also, if the cluster is scaled at a later point of time rebalancing may not be optimal.

For column tables, it is recommended to set a number of buckets such that each bucket has at least 100-150 MB of data. This attribute is set when [creating a table](../reference/sql_reference/create-table.md).

## member-timeout
<a id="member-timeout"></a>

The default [member-timeout](../configuring_cluster/property_description.md#member-timeout) in SnappyData cluster is 30 seconds. The default `spark.network.timeout` is 120 seconds and `spark.executor.heartbeatInterval` is 10 seconds as noted in the [Spark documents](https://spark.apache.org/docs/latest/configuration.html). </br> 
If applications require node failure detection to be faster, then these properties should be reduced accordingly (`spark.executor.heartbeatInterval` but must always be much lower than `spark.network.timeout` as specified in the Spark Documents). </br>
However, note that this can cause spurious node failures to be reported due to GC pauses. For example, the applications with reduced settings need to be resistant to job failures due to GC settings.

This attribute is set in the [configuration files](../configuring_cluster/configuring_cluster.md) **conf/locators**, **conf/servers** and **conf/leads** files. 

<a id="spark-local-dir"></a>
## spark.local.dir  

SnappyData writes table data on disk.  By default, the disk location that SnappyData uses is the directory specified using `-dir` option, while starting the member. 
SnappyData also uses temporary storage for storing intermediate data. The amount of intermediate data depends on the type of query and can be in the range of the actual data size. </br>
To achieve better performance, it is recommended to store temporary data on a different disk (preferably using SSD storage) than the table data. This can be done by setting the `spark.local.dir` property to a location with enough space. For example, ~2X of the data size, in case of single thread execution. In case of concurrent thread execution, the requirement for temp space is approximately data size * number of threads. For example, if the data size in the cluster is 100 GB and three threads are executing concurrent ad hoc analytical queries in the cluster, then the temp space should be ~3X of the data size. This property is set in [**conf/leads**](../configuring_cluster/configuring_cluster.md#lead) as follows:

```
localhost -spark.local.dir=/path/to/local-directory 
```

The path specified is inherited by all servers. The temporary data defaults to **/tmp**. In case different paths are required on each of the servers, then remove the property from **conf/leads** and instead set as system property in each of the **conf/servers** file as follows:

```
localhost ... -J-Dspark.local.dir=/path/to/local-directory1
```

<a id="os_setting"></a>
##  Operating System Settings 

For best performance, the following operating system settings are recommended on the lead and server nodes.

**Ulimit** </br> 
Spark and SnappyData spawn a number of threads and sockets for concurrent/parallel processing so the server and lead node machines may need to be configured for higher limits of open files and threads/processes. </br>
</br>A minimum of 8192 is recommended for open file descriptors limit and nproc limit to be greater than 128K. 
</br>To change the limits of these settings for a user, the **/etc/security/limits.conf** file needs to be updated. A typical **limits.conf** used for SnappyData servers and leads appears as follows: 

```pre
ec2-user          hard    nofile      32768
ec2-user          soft    nofile      32768
ec2-user          hard    nproc       unlimited
ec2-user          soft    nproc       524288
ec2-user          hard    sigpending  unlimited
ec2-user          soft    sigpending  524288
```
* `ec2-user` is the user running SnappyData.


Recent linux distributions using systemd (like RHEL/CentOS 7, Ubuntu 18.04) need the NOFILE limit to be increased in systemd configuration too. Edit **/etc/systemd/system.conf ** as root, search for **#DefaultLimitNOFILE** under the **[Manager] **section. Uncomment and change it to **DefaultLimitNOFILE=32768**. 
Reboot for the above changes to be applied. Confirm that the new limits have been applied in a terminal/ssh window with **"ulimit -a -S"** (soft limits) and **"ulimit -a -H"** (hard limits).

**OS Cache Size**</br> 
When there is a lot of disk activity especially during table joins and during an eviction, the process may experience GC pauses. To avoid such situations, it is recommended to reduce the OS cache size by specifying a lower dirty ratio and less expiry time of the dirty pages.</br> 

Add the following to */etc/sysctl.conf* using the command `sudo vim /etc/sysctl.conf` or `sudo gedit /etc/sysctl.conf` or by using an editor of your choice:</br>

```
vm.dirty_background_ratio=2
vm.dirty_ratio=4
vm.dirty_expire_centisecs=2000
vm.dirty_writeback_centisecs=300
```
Then apply to current session using the command `sudo sysctl -p`

These settings lower the OS cache buffer sizes which reduce the long GC pauses during disk flush but can decrease overall disk write throughput. This is especially true for slower magnetic disks where the bulk insert throughput can see a noticeable drop (such as 20%), while the duration of GC pauses should reduce significantly (such as 50% or more). If long GC pauses, for example in the range of 10s of seconds, during bulk inserts, updates, or deletes is not a problem then these settings can be skipped.

**Swap File** </br> 
Since modern operating systems perform lazy allocation, it has been observed that despite setting `-Xmx` and `-Xms` settings, at runtime, the operating system may fail to allocate new pages to the JVM. This can result in the process going down.</br>
It is recommended to set swap space on your system using the following commands:

```
# sets a swap space of 32 GB

## If fallocate is available, run the following command: 
sudo sh -c "fallocate -l 32G /var/swapfile && chmod 0600 /var/swapfile && mkswap /var/swapfile && swapon /var/swapfile"
## fallocate is recommended since it is much faster, although not supported by some filesystems such as ext3 and zfs.
## In case fallocate is not available, use dd:
sudo dd if=/dev/zero of=/var/swapfile bs=1M count=32768
sudo chmod 600 /var/swapfile
sudo mkswap /var/swapfile
sudo swapon /var/swapfile
```

<a id="smartconnector-local-settings"></a>
## SnappyData Smart Connector Mode and Local Mode Settings

### Managing Executor Memory
For efficient loading of data from a Smart Connector application or a Local Mode application, all the partitions of the input data are processed in parallel by making use of all the available cores. Further, to have better ingestion speed, small internal columnar storage structures are created in the Spark application's cluster itself, which is then directly inserted into the required buckets of the column table in the SnappyData cluster.
These internal structures are in encoded form, and for efficient encoding, some memory space is acquired upfront which is independent of the amount of data to be loaded into the tables. </br>
For example, if there are 32 cores for the Smart Connector application and the number of buckets of the column table is equal or more than that, then, each of the 32 executor threads can take around 32MB of memory. This indicates that 32MB * 32MB (1 GB) of memory is required. Thus, the default of 1GB for executor memory is not sufficient, and therefore a default of at least 2 GB is recommended in this case.

You can modify this setting in the `spark.executor.memory` property. For more information, refer to the [Spark documentation](https://spark.apache.org/docs/latest/configuration.html#available-properties).

### JVM settings for optimal performance
The following JVM settings are set by default on the server nodes of SnappyData cluster. You can use these as guidelines for smart connector and local modes:

*  `-XX:+UseParNewGC`
*  `-XX:+UseConcMarkSweepGC`
*  `-XX:CMSInitiatingOccupancyFraction=50`
*  `-XX:+CMSClassUnloadingEnabled`
*  `-XX:-DontCompileHugeMethods`
*   `-XX:CompileThreshold=2000`
*  ` -XX:+UnlockDiagnosticVMOptions`
*   `-XX:ParGCCardsPerStrideChunk=4k`
*   `-Djdk.nio.maxCachedBufferSize=131072`

**Example**:

```
-XX:-DontCompileHugeMethods -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4k
```
CMS collector with ParNew is used by default as above and recommended. GC settings set above have been seen to work best in representative workloads and can be tuned further as per application. For enterprise users `off-heap` is recommended for best performance.


Set in the **conf/locators**, **conf/leads**, and **conf/servers** file.

<a id=oomerrorhandle> </a>
### Handling Out-of-Memory Error in SnappyData Cluster

When the SnappyData cluster faces an Out-Of-Memory (OOM) situation, it may not function appropriately, and the JVM cannot create a new process to execute the kill command upon OOM. See [JDK-8027434](https://bugs.openjdk.java.net/browse/JDK-8027434).</br> However, JVM uses the **fork()** system call to execute the kill command. This system call can fail for large JVMs due to memory overcommit limits in the operating system. Therefore, to solve such issues in SnappyData, **jvmkill** is used which has much smaller memory requirements.

**jvmkill** is a simple JVMTI agent that forcibly terminates the JVM when it is unable to allocate memory or create a thread. It is also essential for reliability purposes because an OOM error can often leave the JVM in an inconsistent state. Whereas, terminating the JVM allows it to be restarted by an external process manager. </br>A common alternative to this agent is to use the `-XX:OnOutOfMemoryError` JVM argument to execute a `kill -9` command. **jvmkill** is applied by default to all the nodes in a SnappyData cluster, that is the server, lead, and locator nodes. The **jvmkill** agent is useful in a smart connector as well as in a local mode too.

Optionally when using the `-XX:+HeapDumpOnOutOfMemoryError` option, you can specify the timeout period for scenarios when the heap dump takes an unusually long time or hangs up. This option can be specified in the configuration file for leads, locators, or servers respectively. For example:` -snappydata.onCriticalHeapDumpTimeoutSeconds=10`

**jvmkill** agent issues a **SIGTERM** signal initially and waits for a default period of 30 seconds. Thereby allowing for graceful shutdown before issuing a **SIGKILL** if the PID is still running. You can also set the environment variable JVMKILL_SLEEP_SECONDS to set the timeout period. For example: `export JVMKILL_SLEEP_SECONDS=10`

**jvmkill** is verified on centos6 and Mac OSX versions. For running SnappyData on any other versions, you can recompile the **lib** files by running the `snappyHome/aqp/src/main/cpp/io/snappydata/build.sh` script. This script replaces the **lib** file located at the following path:

*	**For Linux **
	*agentPath snappyHome/jars/libgemfirexd.so*

*	**For Mac**
	*agentPath snappyHome/jars/libgemfirexd.dylib*


<a id="codegenerationtokenization"></a>
## Code Generation and Tokenization
SnappyData uses generated code for best performance for most of the queries and internal operations. This is done for both Spark-side whole-stage code generation for queries, for example,[Technical Preview of Apache Spark 2.0 blog]( https://databricks.com/blog/2016/05/11/apache-spark-2-0-technical-preview-easier-faster-and-smarter.html), and internally by SnappyData for many operations. For example, rolling over data from row buffer to column store or merging batches among others. </br>The point key lookup queries on row tables, and JDBC inserts bypass this and perform direct operations. However, for all other operations, the product uses code generation for best performance.

In many cases, the first query execution is slightly slower than subsequent query executions. This is primarily due to the overhead of compilation of generated code for the query plan and optimized machine code generation by JVM's hotspot JIT.
Each distinct piece of generated code is a separate class which is loaded using its own ClassLoader. To reduce these overheads in multiple runs, this class is reused using a cache whose size is controlled by **spark.sql.codegen.cacheSize** property (default is 2000). Thus when the size limit of the cache is breached, the older classes that are used for a while gets removed from the cache.

Further to minimize the generated plans, SnappyData performs tokenization of the values that are most constant in queries by default. Therefore the queries that differ only in constants can still create the same generated code plan.
Thus if an application has a fixed number of query patterns that are used repeatedly, then the effect of the slack during the first execution, due to compilation and JIT, is minimized.

!!!note
	A single query pattern constitutes of queries that differ only in constant values that are embedded in the query string.

For cases where the application has many query patterns, you can increase the value of **spark.sql.codegen.cacheSize** property from the default size of **2000**. 

You can also increase the value for JVM's **ReservedCodeCacheSize** property and add additional RAM capacity accordingly. 

!!!Note
	In the smart connector mode, Apache Spark has the default cache size as 100 which cannot be changed while the same property works if you are using SnappyData's Spark distribution.
