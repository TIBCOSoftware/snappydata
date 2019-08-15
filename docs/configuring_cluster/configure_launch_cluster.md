# Configuring, Launching TIBCO ComputeDB Clusters

Before you configure the TIBCO ComputeDB cluster, check the [system requirements](/install/system_requirements.md). In case you have not yet provisioned TIBCO ComputeDB, you can follow the instructions [here](/install.md).  
TIBCO recommends that you have at least **8 GB** of memory and **4 cores** available even for simple experimentation with TIBCO ComputeDB.


## Launching Single Node Cluster with Default Configuration
If you want to launch the cluster either on Amazon EC2 or on a Kubernetes cluster, you can follow the instructions listed [here (AWS)](/install/setting_up_cluster_on_amazon_web_services.md) and [here (Kubernetes)](/kubernetes.md)

If you are launching on a single node, for example, on your laptop or on a linux server you have access to, you can do so using this simple command:

```
./sbin/snappy-start-all.sh 
```
This launches a single [locator](should point to concepts), [lead](should point to concepts) and a [data server](should point to concepts). You can go to the following URL on your browser to view the cluster dashboard:

**http://(localhost or hostname or machineIP):5050** 

By default, the cluster uses the following ports:

| Cluster Component | Port |Memory Used|
|--------|--------||
| Lead       |**5050** (http port used for dashboard.) </br>**8090** (Port used to submit Spark streaming or batch jobs.) </br>**10000** (Port for hive thrift server.)        |**4 GB**|
|    Locator    |  **1527** (Port used by JDBC clients.)</br> **10334** (Ports used for all cluster members to communicate with each other.)        |**1 GB**|
|Server |**1528** (Port used by ODBC or JDBC clients)  |**4 GB**|

!!!Note 
	By default, the locator uses **1527** port to listen for client connections and the servers that are running on the same machine use subsequent port numbers. Therefore, **1528** port is used by the single server that is launched by the above command. But, if the server was launched on a different machine it would listen on **1527** also.

All the artifacts created such as the server - logs, metrics, and the database files are all stored in a folder called **work** in the product home directory. Click the individual member URLs on the dashboard to view the logs.

**Also see**:

*	[Connecting with JDBC](/howto/connect_using_jdbc_driver.md)

*	[Connecting with ODBC](/howto/connect_using_odbc_driver.md) 

*	[Submitting a Spark Job](/howto/run_spark_job_inside_cluster.md)

<!---(notes: /tmp recommendation, decide on the folder where you want to persist the data ... sizing for this) --->



## Configuring and Launching a Multi-node Cluster

[Provision TIBCO ComputeDB](/install.md) and ensure that all the nodes are setup appropriately. If all the nodes are [SSH enabled](/reference/misc/passwordless_ssh.md) and can share folders using NFS or some shared file system, you can proceed to [Capacity Planning](#initialcapplan). A shared file system is not a requirement but preferred. 

<a id="initialcapplan"></a>
### Step 1: Capacity Planning 
You must consider the capacity required for [storage capacity](#storeplan) (in-memory and disk) as well as the [computational capacity](#computeplan) that is required (memory and CPU).  You can skip this step if the volume of data is low and feel you have abundant resources for the client workload. 

<a id="storeplan"></a>
#### Planning Storage Capacity
TIBCO ComputeDB is optimized for managing all the data in the memory. When data cannot fit in the memory, it automatically overflows the data to disk. To achieve the highest possible performance, we recommend you go through this exercise below. The capacity required is dependent on several variables such as input data format, the data types in use, use of indexes, number of redundant copies in the cluster, cardinality of the individual columns (compression ratio can vary a lot) and so on. 

A general rule of thumb for compressed data (say Parquet) is to configure about **1.5X** the compressed size on disk. Else, you can also use the following steps:

1.	Define your external tables to access the data sources. This only creates catalog entries in TIBCO ComputeDB. </br>For example,  when loading data from a folder with data in CSV format, you can do the following using the [Snappy Shell](/howto/use_snappy_shell.md):


        create external table t1 using csv options(inferSchema 'true', header 'true', path '<some folder or file containing the CSV data>') ;
                    // Spark Scala/Java/Python API example is omitted for brevity.


	More examples for loading data from external sources are available [here](/howto/load_data_from_external_data_stores.md)

2.	Decide if you want to manage this data in a [Columnar or Row format](/programming_guide/tables_in_snappydata.md). 

3.	Load a sample of the data:

		create table t1_inmem using column as (select * from t1 where rand() < 0.1) ;

	This loads about 10% of the data from the external data source. Note that the fraction value (0.1, which is 10%) should be reduced when working with large data sets or increased when the data set is small. You must repeat this process for all the tables that you want to manage in the memory. 

4.	Check the dashboard to view the actual memory consumed. Simply extrapolate to 100% (10 times for the example above). 
The amount of disk space is about two times the memory space requirement. 

<a id="computeplan"></a>
#### Estimating Computation Capacity
TIBCO recommends to configure off-heap memory when using the Enterprise edition that is TIBCO ComputeDB. This option is not available in the community edition of TIBCO ComputeDB where all data must be managed in the JVM heap memory. Even when off-heap is configured, you must also configure enough JVM heap memory for Spark temporary caching, computations, and buffering when the data is loaded. 

!!!Note 
	Only the columnar table data is managed in off-heap memory. Row tables are always in JVM heap.

TIBCO recommends to allocate at least **1 GB **of memory per core to JVM heap for computations. 

For example, when running on **8 core** servers, configure JVM heap to be **8 GB**. 
By default, **50%** of the off-heap memory is available as computational memory. While, you may not need this much computational capacity when large off-heap is configured, it is still recommended for reserving enough capacity if working with large data sets. 


More complex the analytical processing, especially large aggregations, greater the space requirement in off-heap. For example, if your per server off-heap storage need is **100 GB** then, allocate an additional **30 GB** of off-heap for computations. Even if your data set is small, you must still allocated at least a few Gigabytes of off-heap storage for computations.

### Step 2: Configure Core Cluster Component Properties 

Configuration files for locator, lead, and server should be created in the **conf** folder located in the TIBCO ComputeDB home directory with names **locators**, **leads**, and **servers**.

To do so, you can copy the existing template files **servers.template**, **locators.template**, **leads.template**, and rename them to **servers**, **locators**, **leads**.
These files should contain the hostnames of the nodes (one per line) where you intend to start the member. You can modify the properties to configure individual members.

#### Configuring Core Lead properties
The following core properties must be set in the **conf/leads** file:

| Properties | Description |Default Value
|--------|--------|--------|
|  hostname (or IP) |  The hostname on which a TIBCO ComputeDB locator is started   |        | 
|   heap-size     |  Sets the maximum heap size for the Java VM, using TIBCO ComputeDB default resource manager settings. </br>For example, `-heap-size=8g` </br> It is recommended to allocate minimum **6-8 GB** of heap size per lead node. If you use the `-heap-size` option, by default TIBCO ComputeDB sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>TIBCO ComputeDB also sets resource management properties for eviction and garbage collection if the JVM supports them.       |   |
|  dir      |   Working directory of the member that contains the TIBCO ComputeDB Server status file and the default location for the log file, persistent files, data dictionary, and so forth.   | <product_home>/work  |
|   classpath     |  Location of user classes required by the TIBCO ComputeDB Server. This path is appended to the current classpath   | Appended to the current classpath |
|  zeppelin.interpreter.enable=true    |Enables the Zeppelin interpreter. Refer [How to use Apache Zeppelin with TIBCO ComputeDB](/howto/use_apache_zeppelin_with_snappydata.md) |   |
|   spark.executor.cores     | The number of cores to use on each server.    |   |
|    spark.jars    |        |   |


##### Example Configurations
In the following configuration, you set the heap size for the lead and specify the working directory location: 

```
localhost   -dir=/opt/snappydata/data/lead -heap-size=6g
```
You can add a line for each of the Lead members that you want to launch. Typically only one. In production, you may launch two.

In the following configuration, you are specifying the [number of cores to use on each server](/best_practices/setup_cluster.md#computenoscores) as well as enabling the Zeppelin interpreter:

```
localhost -spark.executor.cores=16 -zeppelin.interpreter.enable=true
```

!!!Tip
	It is a common practice to run the Lead and Locator on a single machine. The locator requires very less memory and CPU, the lead memory requirement is directly proportional to the concurrency and the potential for returning large result sets. However, data servers do most of the heavy lifting for query execution and Spark distributed processing.


#### Configuring Core Locator Properties

The following core properties must be set in the **conf/locators** file:

| Properties | Description |Default Value
|--------|--------|--------|
|  client-port |  The port that the network controller listens for client connections in the range of 1 to 65535. |     1527   |
|   dir    |  Working directory of the member that contains the TIBCO ComputeDB Server status file and the default location for the log file, persistent files, data dictionary, and so forth. |Current directory|

### Configuring Core Server Properties

The following core properties must be set in the **conf/servers** file:

| Properties | Description |Default Value
|--------|--------|--------|
| memory-size| Specifies the total memory that can be used by the node for column storage and execution in off-heap. | The default value is either 0 or it gets auto-configured in [specific scenarios](configuring_cluster.md#autoconfigur_offheap)   |
|   heap-size    |  Sets the maximum heap size for the Java VM, using TIBCO ComputeDB default resource manager settings. </br>For example, `-heap-size=8g` </br> It is recommended to allocate minimum 6-8 GB of heap size per lead node. If you use the `-heap-size` option, by default TIBCO ComputeDB sets the critical-heap-percentage to 95% of the heap size, and the `eviction-heap-percentage` to 85.5% of the `critical-heap-percentage`. </br>TIBCO ComputeDB also sets resource management properties for eviction and garbage collection if the JVM supports them.  ||
|   dir    |  Working directory of the member that contains the TIBCO ComputeDB Server status file and the default location for the log file, persistent files, data dictionary, and so forth. |Current directory|

#### Configuration Examples

```
cat <product_home>/conf/servers
node1_hostname -dir=/nfs/opt/snappy-db1/server1 -heap-size=8g -memory-size=42g
node1_hostname -dir=/nfs/opt/snappy-db1/server2 -heap-size=8g -memory-size=42g

// You can launch more than one data server on a host 

node2_hostname -dir=/nfs/opt/snappy-db1/server3 -heap-size=8g -memory-size=42g
node2_hostname -dir=/nfs/opt/snappy-db1/server4 -heap-size=8g -memory-size=42g

```

#### TIBCO ComputeDB Properties

[Reference to all properties and description](property_description.md)

