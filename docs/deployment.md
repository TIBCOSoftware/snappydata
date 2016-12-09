In this section we discuss the various affinity modes which deals with the collocation of related data and computation. 
You can run the SnappyData store in the following modes:

| Modes        |Description            |
| ------------- |:-------------:|
| Local Mode|This is for development where client application, the executors and data store are all running in the same JVM|
| Snappy Smart Connector Mode|Allows you to work with the SnappyData store cluster from any compatabile Spark distrubution|
| Embedded SnappyData Store Mode|The Spark computations and in-memory data store run collocated in the same JVM|

##Local Mode

As the name implies, use this mode to execute everything locally in the application JVM. 

Key Points

* No cluster required
* Launch Single JVM (Single-node Cluster)
* Launches executor threads locally for processing
* Embeds the SnappyData in-memory store in-process
* For development purposes only


<mark>Example: TO BE DONE (Getting Started - Launch Apache Spark shell and provide SnappyDat dependency as a Spark package)
</mark>

If you have downloaded SnappyData, you can use the bundled Spark distribution in which case you do not have to specify any external dependencies to work with the SnappyData store.

### Using the Local mode for developing using an IDE

As the name implies, use this mode to execute everything locally in the application JVM. The local mode is described in the Spark Programming guide.
<mark> TO BE DONE </mark>

![Local Mode](./Images/SnappyLocalMode.png)

### Running a Spark (non-IDE) program and specify the dependencies

<mark> TO BE DONE </mark>

## SnappyData Smart Connector Mode
In certain cases Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

![Smart Connector Mode](./Images/SnappyConnectorMode.png)


Key Points:

* Can work with SnappyData store from any compatabile Spark distribution
* Spark Cluster executes in its own independant JVM processes
* The Spark cluster connects to SnappyData as a Spark Data source
* Supports any of the Spark supported resource managers (example, Spark Standalone Manager, YARN or Mesos)

**Performance**: When Spark partitions store data in **column tables**, the connector automatically attempts to localize the partitions into SnappyData store buckets on the local node. The connector uses the same column store format as well as compression techniques in Spark avoiding all data formatting related in-efficiencies or unnecessary serialization costs. This is the fastest way to ingest data when Spark and the cluster are operating as independent clusters. 

When storing to **Row tables** or when the partitioning in Spark is different than the partitioning configured on the table, data batches could be shuffled across nodes. Whenever Spark applications are writing to Snappy tables, the data is always batched for the highest possible throughput.

When queries are executed, while the entire query planning and execution is coordinated by the Spark engine (Catalyst), the smart connector still carries out a number of optimizations. They are as listed below:

* Route jobs to same machines as SnappyData data nodes if the executor nodes are co-hosted on the same machines as the data nodes. Job for each partition tries to fetch only from same machine data store where possible.
* Column table scan using optimized code generation: Once a column batch is fetched, then it uses the same optimized scan as in the embedded mode
* Collocated joins: If the underlying tables are collocated partition-wise, and executor nodes are co-hosting SnappyData data nodes, then the column batches will be fetched from local machines and the join itself will be partition-wise not requiring any exchange.
* Optimized hash maps for groupBy aggregates without any UnsafeRow conversions like in embedded mode
* Hash join using optimized hash maps avoiding any UnsafeRow conversions like in embedded mode
* GroupBy and hash join on single dictionary column uses an optimized path using dictionary indexes like in embedded mode
* Optimized column batch inserts like in embedded mode with job routing to same machines as data stores if possible (soon to be merged)
* CollectAggregate plan which allows accumulation of aggregate results on the driver itself (rather than a SinglePartition Exchange at the end for non-groupBy aggregates)

**SQL connectivity**: SQL clients (using JDBC or ODBC) can connect and work with the Snappydata store cluster and have no dependency on Spark. So,the Spark application can connect and run native Spark applications using the SnappyData Smart Connector while concurrent SQL clients are executing directly on the Snappydata store cluster.

<mark>Example: TO BE DONE (Run Apache Spark in local/cluster mode connecting to a SnappyData Cluster)

Step 1: Start the SnappyData cluster
You can either start SnappyData members using the `_snappy_start_all_ script` or you can start them individually.

```bash
# start members using the ssh scripts 
$ sbin/snappy-start-all.sh
```

```
# start members individually
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334
```

Step 2: Launch the Apache Spark program in the Local mode

<mark> ADD IMAGE </mark> 

Step 3: Launch the Apache Spark program using external cluster manager

```
./bin/spark-shell  --master local[*] --conf spark.snappydata.store.locators=localhost:10334 --packages "SnappyDataInc:snappydata:0.7-s_2.11"
```

## Embedded SnappyData Store Mode
In this mode the Spark computations and in-memory data store run collocated in the same JVM. This is our out of the box configuration and suitable for most SnappyData real time production environments. You launch SnappyData servers to bootstrap any data from disk, replicas or from external data sources and Spark executors are dynamically launched when the first Spark Job arrives. 
![Embedded Mode](./Images/SnappyEmbeddedMode.png)

You can either start SnappyData members using the `_snappy_start_all_ script` or you can start them individually.

Some of the advantages of this mode are:

* **Highest performance**: All your Spark applications access the table data locally, in-process. The query engine accesses all the data locally by reference and avoids copying (which can be very expensive when working with large volumes).

* **Driver High Availabiltiy**: When Spark jobs are submitted, then can now run in a HA configuration. The submitted job becomes visible to a redundant “lead” node that prevents the executors to go down when the Spark driver fails. Any submitted Spark job continues to run as long as there is at least one “lead” node running.

* **Less complex**: There is only a single cluster to start, monitor, debug and tune.

Example: Submit a Spark Job to the SnappyData Cluster
```
bin/snappy-job.sh submit --app-name JsonApp --class org.apache.spark.examples.snappydata.WorkingWithJson --app-jar examples/jars/quickstart.jar --lead [leadHost:port] --conf json_resource_folder=../../quickstart/src/main/resources
```
