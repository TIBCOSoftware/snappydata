This section provides a short overview of the deployment architectures available in SnappyData.


<!---## Unified cluster mode (aka 'Embedded store' mode)
In the default cluster model the Spark computations and in-memory data store run collocated in the same JVM. This is our ootb configuration and suitable for most SnappyData real time production environments. You launch Snappy Data servers to bootstrap any data from disk, replicas or from external data sources and Spark executors are dynamically launched when the first Spark Job arrives. 

You either start SnappyData members using the `_snappy_start_all_ script` or you start them individually. 

```bash
# start members using the ssh scripts 
$ sbin/snappy-start-all.sh

# start members individually
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334
```

Spark applications are coordinated by a SparkContext instance that runs in the Application's main program called the 'Driver'. The driver coordinates the execution by running parallel tasks on executors and is responsible for delivering results to the application when 'Jobs'(i.e. actions like print() ) are executed.

During execution, there can only be a single Spark Context (a single coordinator if you may) for the cluster. To support multiple concurrent Jobs or applications Snappydata manages a singleton SparkContext created and running in the 'Lead' node. i.e. the Spark context is fully managed by Snappydata. Applications simply submit [Jobs](jobs.md) and don't have to be concerned about HA for the context or the driver program. 
The rationale for our design is further explored [here](architecture.md). 
 
### Fully managed Spark driver and context

Programs can connect to the lead node and submit Jobs. The Driver is managed by the Snappy cluster in the lead node and the application doesn’t create or manage the Spark context. Applications implement the _SnappySQLJob_ or the _SnappyStreamingJob_ trait as described in the [Building Snappy applications using Spark API](jobs.md) section.


### Application managed Spark driver and context
While Snappy recommends the use of scala traits mentioned above to implement your application, you could also run your native Spark program on the unified cluster with a slight change to the cluster URL. 

```scala
val conf = new SparkConf().
              // here the locator url is passed as part of the master url
              setMasterURL("snappydata://localhost:10334").
              set("jobserver.enabled", "true")
val sc = new SparkContext(conf) 
```
> ### Note
> We currently don't support external cluster managers like YARN when operating in this mode. While, it is easy to expand and redistribute the data by starting new data servers dynamically we expect such dynamic resource allocations to be a planned and seldom exercised option. Re-distributing large quantities of data can be very expensive and can slow down running applications. 
>For computational intensive workloads or batch processing workloads where extensive data shuffling is involved consider using the Split cluster mode described next. 

<!---## Split cluster mode--->
In certain cases the Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

The driver program managing the SparkContext also participate as a peer member in the SnappyData distributed system and gets access to the store catalog information. To enable this, you must set the _locator_ host/port in the configuration (see example below). When executors running the spark cluster access these tables the catalog metadata is used to locate the store servers managing data partitions and would be accessed in parallel. 
Read the [Spark cluster overview](http://spark.apache.org/docs/latest/cluster-overview.html) for more details on the native Spark architecture. 

```scala
val conf = new SparkConf().
              // Here the spark context connects with Spark's master running on 7077. 
              setMasterURL("spark://localhost:7077").
              set("spark.snappydata.store.locators", "localhost:10334") 
val sc = new SparkContext(conf) 
// use sc to use Spark and Snappy features. 
// The following code connects with the snappy locator to fetch hive metastore. 
val snappyContext = SnappyContext(sc) 

```
The catalog is initialized lazily when SnappyData functionality is accessed. 
The big benefit even while the clusters for compute and data is split is that the catalog is immediately visible to the Spark executors nodes and applications don’t have to explicitly manage connections and schema related information. This design is quite similar to the Spark’s native support for Hive. 

When accessing partitioned data, the partitions are fetched as compressed blobs that is fully compatible with the columnar compression built into Spark. All access is automatically parallelized. 


<!---## Local mode--->
If you want to execute everything locally in the application JVM. The local vs cluster modes are described in the [Spark Programming guide](http://spark.apache.org/docs/latest/programming-guide.html#local-vs-cluster-modes).

```scala
val conf = new SparkConf().
               setMaster("local[*]").
               // Starting jobserver helps when you would want to test your jobs in a local mode. 
               set("jobserver.enabled", "true")
val sc = new SparkContext(conf) 
// use sc to use Spark and Snappy features. 
// JobServer is started too. 
```