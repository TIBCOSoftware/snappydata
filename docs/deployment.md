# Deployment toplogies (rough)

We offer different options that range from tightly coupled compute with data for real time data intensive applications to loose coupling between a Spark compute layer and the data tier for high throughput batch processing. Or, simply an fully embedded local mode for developers. 

## Unified cluster mode
This is the default cluster model where Spark computations and data store are completely collocated. This is our ootb configuration and suitable for most SnappyData real time production environments. 

### Fully managed Spark driver and context

Programs can connect to the lead node and submit Jobs. The Driver is managed by the Snappy cluster in the lead node and the application doesn’t create or managed the Spark context. 
…..yada yada …  This is the preferred option as the driver failures would be fully handled and the catalog …

### Application managed Spark driver and context
… provide some examples … when would I use this vs fully managed?


## Split cluster mode
This is the default Spark model where the Spark cluster manager or external cluster manager manages the computational resources. Executors no longer share same process space as data nodes … yada yada …

The catalog is initialized lazily when SnappyData functionality is accessed. 
The big benefit even while the clusters for compute and data is split is that the catalog is immediately visible to the Spark executors nodes and applications don’t have to explicitly manage connections and schema related information. This design is quite similar to the Spark’s native support for Hive. 

When accessing partitioned data, the partitions are fetched as compressed blobs that is fully compatible with the columnar compression built into Spark. 
All access is automatically parallelized. 

e.g. with Spark native URL … etc

(Show picture for each deployment mode)

## Local mode
Say this and that from Spark guides.


