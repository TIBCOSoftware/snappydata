In this section we discuss the various execution modes. You can run the SnappyData store in the following modes:

| Deployment Modes        |Description            |
| ------------- |:-------------:|
| Local Mode|A single JVM that runs the Spark application|
| Snappy Smart Connector Mode|Allows you to work with the SnappyData store cluster from any compatabile Spark distrubution|
| Embedded SnappyData Store Mode|The Spark computations and in-memory data store run collocated in the same JVM|

##Local Mode
<mark> Description TO be done </mark>

Key Points

* No cluster Required
* For development purposes only
* Launch Single JVM (Single-node Cluster)
* Launches executor threads locally for processing
* Embeds the SnappyData in-memory store in-process

<mark>Example: TO BE DONE (Getting Started - Launch Apache Spark shell and provide SnappyDat dependency as a Spark package)
</mark>

If you have downloaded SnappyData,  you can use the bundeled Spark distribution in which case you do not have to specify any external dependencies to work with the Snappy store.

Can I use the Local mode for developing using an IDE?

<mark> TO BE DONE </mark>

Can I run a Spark (non-IDE) program and specify the dependecies?

<mark> TO BE DONE </mark>

## SnappyData Smart Connector Mode
<mark> Description TO be done </mark>

Key Points:

* Can work with SnappyData store from any compatabile Spark distribution
* Spark Cluster executes in its own independant JVM processes
* The Spark cluster connects to SnappyData as a Spark Data source
* Supports any of the Spark supported resource managers (example, Spark Standalone Manager, YARN or Mesos)

**Performance**: When Spark partitions store data in **column tables**, the connector automatically attempts to localize the partitions into SnappyData store buckets on the local node. The connector uses the same column store format as well as compression techniques in Spark avoiding all data formatting related in-efficiencies or unnecessary serialization costs. This is the fastest way to ingest data when Spark and the cluster are operating as independent clusters. 

When storing to **Row tables** or when the partitioning in Spark is different than the partitioning configured on the table, data batches could be shuffled across nodes. Whenever Spark applications are writing to Snappy tables, the data is always batched for the highest possible throughput.

When queries are executed, while the entire query planning and execution is coordinated by the Spark engine (Catalyst), the smart connector still carries out a number of optimizations.

<mark>(get a list from Sumedh to include here).</mark> 

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

<mark> TO BE Done (Code using spark-submit or spark shell - master local[*] Add SnappyData package dependency and configure the locator - Help from Rishi</mark>

## Embedded SnappyData Store Mode
In this mode the Spark computations and in-memory data store run collocated in the same JVM. This is our out of the box configuration and suitable for most SnappyData real time production environments. You launch SnappyData servers to bootstrap any data from disk, replicas or from external data sources and Spark executors are dynamically launched when the first Spark Job arrives. 

You can either start SnappyData members using the `_snappy_start_all_ script` or you can start them individually.

Some of the advantages of this mode are:

* **Highest performance**: All your Spark applications access the table data locally, in-process. The query engine accesses all the data locally by reference and avoids copying (which can be very expensive when working with large volumes).

* **Driver High Availabiltiy**: When Spark jobs are submitted, then can now run in a HA configuration. The submitted job becomes visible to a redundant “lead” node that prevents the executors to go down when the Spark driver fails. Any submitted Spark job continues to run as long as there is at least one “lead” node running.

* **Less complex**: There is only a single cluster to start, monitor, debug and tune.

<mark>Example: Submit a Spark Job to the SnappyData Cluster - Rishi</mark>

<mark> ADD IMAGE </mark>