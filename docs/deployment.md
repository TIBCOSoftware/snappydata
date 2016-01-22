# Deployment toplogies (rough)

We offer different options that range from tightly coupled compute with data for real time data intensive applications to loose coupling between a Spark compute layer and the data tier for high throughput batch processing. Or, simply an fully embedded local mode for developers. 

## Unified cluster mode
This is the default cluster model where Spark computations and data store are completely collocated. This is our ootb configuration and suitable for most SnappyData real time production environments. You either start SnappyData members using the script or you start them individually. 

```bash
# start members using the ssh scripts 
$ sbin\snappy-start-all.sh

# start members individually
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334
```
 
### Fully managed Spark driver and context

Programs can connect to the lead node and submit Jobs. The Driver is managed by the Snappy cluster in the lead node and the application doesn’t create or managed the Spark context. 

### Application managed Spark driver and context
… provide some examples … when would I use this vs fully managed?
Not sure if we want to document this. But if yes, here is the code. 

```scala
val conf = new SparkConf().
              // here the locator url is passed as part of master url
              setMasterURL("snappydata://localhost:10334").
              set("snappy.jobserver.enabled", "true")
val sc = new SparkContext(conf) 
```

## Split cluster mode
This is the default Spark model where the Spark cluster manager or external cluster manager manages the computational resources. Executors no longer share same process space as data nodes … yada yada …

```scala
val conf = new SparkConf().
              // Here the spark context connects with Spark's master running on 7077. 
              setMasterURL("spark://localhost:7077").
              set("snappy.locator", "localhost[10334]") 
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
               set("snappy.jobserver.enabled", "true")
val sc = new SparkContext(conf) 
// use sc to use Spark and Snappy features. 
// JobServer is started too. 
```


