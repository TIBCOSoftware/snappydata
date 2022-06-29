<a id="connectormode"></a>
# SnappyData Smart Connector Mode

In this mode, the Spark cluster executes in its own independent JVM processes and connects to SnappyData as a Spark data source. Conceptually, this is similar to how Spark applications work with stores like Cassandra, Redis etc. The Smart connector mode also implements several performance optimizations as described in this section.

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers
(either local-only runners, Spark’s own standalone cluster manager, Mesos or YARN), which allocate
resources across applications. Once connected, Spark acquires executors on nodes in the cluster,
which are processes that run computations and store data for your application. Next, it sends your application code
(defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

![Smart Connector Mode](../Images/SnappyConnectorMode.png)

**Key Points:**

* Can work with SnappyData store from a compatible Spark distribution (2.1.1, 2.1.2 or 2.1.3)

* Spark application executes in its own independent JVM processes

* The Spark application connects to SnappyData as a Spark Data source

* Supports any of the Spark supported resource managers (for example, local/local-cluster, Spark Standalone Manager, YARN or Mesos)

**Some of the advantages of this mode are:**

***Performance***

* When Spark partitions store data in **column tables**, the connector automatically attempts to localize
  the partitions into SnappyData store buckets on the local node. The connector uses the same column store
  format as well as compression techniques in Spark avoiding all data formatting related inefficiencies or
  unnecessary serialization costs. This is the fastest way to ingest data when Spark and the SnappyData
  cluster are operating as independent clusters.

* When storing to **Row tables** or when the partitioning in Spark is different from the partitioning
  configured on the table, data batches could be shuffled across nodes. Whenever Spark applications are
  writing to SnappyData tables, the data is always batched for the highest possible throughput.

* When queries are executed, while the entire query planning and execution is coordinated by the Spark engine
  (Catalyst), the smart connector still carries out a number of optimizations, which are listed below:
    * Route jobs to same machines as SnappyData data nodes if the executor nodes are co-hosted on the same machines
      as the data nodes. Job for each partition tries to fetch only from same machine data store where possible.
    * Data for column tables is fetched as it is stored in raw columnar byte data format, and optimized code generated
      plans read the columnar data directly which gives performance equivalent to and exceeding Spark's ColumnVectors
      while avoiding its most expensive step of having to copy external data to ColumnVectors.
    * Colocated joins: If the underlying tables are colocated partition-wise, and executor nodes are co-hosting
      SnappyData data nodes, then the column batches are fetched from local machines and the join itself is
      partition-wise and does not require any exchange.

* Optimized column batch inserts like in the Embedded mode with job routing to same machines as data stores if possible.

<a id="snappysession"></a>
***Features***

Allows creation of `SnappySession` which extends `SparkSession` in various ways:

* Enhanced SQL parser allowing for DML operations `UPDATE`, `DELETE`, `PUT INTO`, and DDL `TRUNCATE` operation
* Enhanced APIs to perform inline row-wise or bulk `UPDATE`, `DELETE` and `PUT INTO` operations
* New `CREATE POLICY` operation (and corresponding `DROP POLICY`) to create a security access policy using SQL or API
* Tokenization and plan caching that allows reuse of plans and generated code for queries that
  differ only in constant values and thus substantially improves overall performance of the queries
* Automatically uses the embedded cluster for the hive meta-data by default, so any tables created will
  show up in the embedded cluster and vice-versa
* Ability to also attach to an external hive meta-store in addition to the embedded one
* Uniform APIs corresponding to DDL operations to `CREATE`, `DROP` tables, views and policies, and `ALTER`, `TRUNCATE` tables

<a id="example"></a>

**Example: Launch a Spark local mode cluster and use Smart Connector to access SnappyData cluster**

**Step 1: Start the SnappyData cluster**:
You can either start SnappyData members using the `snappy_start_all` script or you can start them individually.

**Step 2: Launch the Apache Spark program**

***_In the Local mode_***

```sh

./bin/spark-shell  --master local[*] --conf spark.snappydata.connection=localhost:1527 --packages "io.snappydata:snappydata-spark-connector_2.11:1.3.1-HF-1"
```

!!! Note

    * The `spark.snappydata.connection` property points to the locator of a running SnappyData cluster.
      The value of this property is a combination of locator host and JDBC client port on which the locator
      listens for connections (default is 1527).
    * In the Smart Connector mode, all `snappydata.*` SQL configuration properties should be prefixed with `spark`
      when running with upstream Apache Spark (not required if running bundled SnappyData's spark).
      For example, `spark.snappydata.column.batchSize`.

This opens a Scala Shell.

**Step 3: Import any or all of the following:**

*	**SnappySession**
*	**SparkSession**

``` scala
import org.apache.spark.sql.{SnappySession,SparkSession}
```

 This starts the SnappyData cluster with Smart Connector mode. Create a SnappySession to interact with the SnappyData store.

``` scala
	// Create a SnappySession to work with SnappyData store
	$scala > val snSession = new SnappySession(spark.sparkContext)
```

The code example for writing a Smart Connector application program is located in [SmartConnectorExample](https://github.com/TIBCOSoftware/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SmartConnectorExample.scala)

***_Using External Cluster Manager_***

**Cluster mode**

```sh
./bin/spark-submit --deploy-mode cluster --class somePackage.someClass  --master spark://localhost:7077 --conf spark.snappydata.connection=localhost:1527 --packages "io.snappydata:snappydata-spark-connector_2.11:1.3.1-HF-1"
```

**Client mode**
```sh
./bin/spark-submit --deploy-mode client --class somePackage.someClass  --master spark://localhost:7077 --conf spark.snappydata.connection=localhost:1527 --packages "io.snappydata:snappydata-spark-connector_2.11:1.3.1-HF-1"
```


***_Using YARN as a Cluster Manager_***

**Cluster mode**
```sh
./spark-submit --master yarn --deploy-mode cluster --conf spark.driver.extraClassPath=/home/snappyuser/snappydata-1.3.1-bin/jars/* --conf spark.executor.extraClassPath=/home/snappyuser/snappydata-1.3.1-bin/jars/* --class MainClass SampleProjectYarn.jar
```

**Client mode**
```sh
./spark-submit --master yarn --deploy-mode client --conf spark.driver.extraClassPath=/home/snappyuser/snappydata-1.3.1-bin/jars/* --conf spark.executor.extraClassPath=/home/snappyuser/snappydata-1.3.1-bin/jars/* --class MainClass SampleProjectYarn.jar
```
