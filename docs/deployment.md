# Deployment toplogies (rough)

This section provides a short overview of the different runtime deployment architectures available and recommendations on when to choose one over the other. 
There are three deployment modes available in snappydata. 

|Deployment Mode   | Description  |
|---|---|
|Unified Cluster   | Real time production application. Here the Spark Executor(compute) and Snappy DataStore are collocated   |
|Split Cluster   | Spark executors and SnappyStore form independent clusters. Use for computationally heavy computing and Batch processing  |
|Local | This is for development where client application, the executors and data store are all running in the same JVM |



## Unified cluster mode (aka 'Embedded store' mode)
This is the default cluster model where Spark computations and in-memory data store run collocated in the same JVM. This is our ootb configuration and suitable for most SnappyData real time production environments. You launch Snappy Data servers to bootstrap any data from disk, replicas or from external data sources and Spark executors are dynamically launched when the first Spark Job arrives. 

You either start SnappyData members using the _snappy_start_all_ script or you start them individually. 

```bash
# start members using the ssh scripts 
$ sbin\snappy-start-all.sh

# start members individually
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334
```

Spark applications are coordinated by a SparkContext instance that runs in the Application's main program called the 'Driver'. The driver coordinates the execution by running parallel tasks on executors and is responsible for delivering results to the application when 'Jobs'(i.e. actions like print() ) are executed. 
When executing in this unified cluster mode there can only be a single Spark Context (a single coordinator if you may) for the cluster. To support multiple concurrent Jobs or applications Snappydata manages a singleton SparkContext created and running in the 'Lead' node. i.e. the Spark context is fully managed by Snappydata. Applications simply submit [Jobs](jobs) and don't have to be concerned about HA for the context or the driver program. 
The rationale for our design is further explored [here](architecture). 
 
### Fully managed Spark driver and context

Programs can connect to the lead node and submit Jobs. The Driver is managed by the Snappy cluster in the lead node and the application doesn’t create or managed the Spark context. Applications implement the _SnappySQLJob_ or the _SnappyStreamingJob_ trait as describing in the ['Building Spark Apps'](BuildingSparkApps) section.


### Application managed Spark driver and context
While Snappy recommends the use of these above mentioned scala traits to implement your application, you could also run your native Spark program on the unified cluster with a slight change to the cluster URL. 

```scala
val conf = new SparkConf().
              // here the locator url is passed as part of the master url
              setMasterURL("snappydata://localhost:10334").
              set("jobserver.enabled", "true")
val sc = new SparkContext(conf) 
```

## Split cluster mode
This is the default Spark model where the Spark cluster manager or external cluster manager manages the computational resources. Executors no longer share same process space as data nodes … yada yada …

```scala
val conf = new SparkConf().
              // Here the spark context connects with Spark's master running on 7077. 
              setMasterURL("spark://localhost:7077").
              set("snappydata.store.locators", "localhost:10334") 
val sc = new SparkContext(conf) 
// use sc to use Spark and Snappy features. 
// The following code connects with the snappy locator to fetch hive metastore. 
val snappyContext = SnappyContext(sc) 

```
The catalog is initialized lazily when SnappyData functionality is accessed. 
The big benefit even while the clusters for compute and data is split is that the catalog is immediately visible to the Spark executors nodes and applications don’t have to explicitly manage connections and schema related information. This design is quite similar to the Spark’s native support for Hive. 

When accessing partitioned data, the partitions are fetched as compressed blobs that is fully compatible with the columnar compression built into Spark. 
All access is automatically parallelized. 

e.g. with Spark native URL … etc

(Show picture for each deployment mode)

## Local mode
Say this and that from Spark guides.

```scala
val conf = new SparkConf().
               setMasterURL("snappydata:local[*]"). 
               // Starting jobserver helps when you would want to test your jobs in a local mode. 
               set("jobserver.enabled", "true")
val sc = new SparkContext(conf) 
// use sc to use Spark and Snappy features. 
// JobServer is started too. 
```


