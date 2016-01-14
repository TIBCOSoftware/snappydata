## Building Snappy applications using Spark API
> ..Explain the SQLJob and programming example
> Submitting jobs
> Running Spark native programs … submitting and running jobs

----
SnappyData bundles Spark and supports all the Spark APIs. You can create Object based RDDs and run transformations or use the rich higher level APIs. Working with the SnappyData Tables themselves builds on top of Spark SQL. So, we recommend getting to know the [concepts in SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) (and hence some core Spark concepts). 

You primarily interact with SnappyData tables using SQL (a richer, more compliant SQL) or the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes). And, you can store and manage arbitrary RDDs (or even Spark DataSets) through implicit or explicit transformation to a DataFrame. 

In Spark SQL, all tables are temporary and cannot be shared across different applications. While you can manage such temporary tables, SnappyData tables are automatically registered to a built-in catalog and persisted using the SnappyStore to disk (i.e. the tables will be there when the cluster recovers). This is similar to how Spark SQL uses the Hive catalog to natively work with Hive clusters. 


### SnappyContext
A SnappyContext is the main entry point for SnappyData extensions to Spark. A SnappyContext extends Spark's [[org.apache.spark.sql.SQLContext]] to work with Row and Column tables. Any DataFrame can be managed as SnappyData tables and any table can be accessed as a DataFrame. This is similar to [[org.apache.spark.sql.hive.HiveContext HiveContext]] - integrates the SQLContext functionality with the Snappy store.

When running in the __embedded__ mode (i.e. Spark executor collocated with Snappy data store), Applications typically submit Jobs to the Snappy-JobServer (provide link) and do not explicitly create a SnappyContext. A single shared context managed by SnappyData makes it possible to re-use Executors across client connections or applications.

> Todo: Provide a simple local[*] example … create sparkContext; snContext = SnappyContext(sc); store into a table
> See Spark SQL guide  … scribe along similar lines ..
> describe few important methods in SnappyContext 


### Deployment topologies with SnappyData … hemant can own this?
- running in local mode … provide an example with SnappyContext and master URL
- running in embedded mode … DB and Spark programs are collocated … who is playing the role of the master, URL? 
This is our primary execution model where Spark is running collocated within the database and the recommended deployment topology for real time data intensive applications to achieve highest possible performance. 

```
snappy locator start -peer-discovery-address=localhost -dir=locator1
snappy server start -locators=localhost[10334] -dir=server1
snappy lead start -locators=localhost[10334] -dir=lead1
```
As soon as the lead node joins the system, it starts a Spark driver and then asks server nodes to start Spark executors within them. 
> show architecture and explain

- running Spark in standalone cluster

> What else?

### Running Spark programs inside the database


To create a job that can be submitted through the job server, the job must implement the _SnappySQLJob or SnappyStreamingJob_ trait. Your job will look like:
```scala
class SnappySampleJob implements SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runJob(sc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def validate(sc: SnappyContext, config: Config): SparkJobValidation
}
```
> The _Job_ traits are simply extensions of the _SparkJob_ implemented by [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver). 

• runJob contains the implementation of the Job. The SparkContext is managed by the SnappyData Leader (which runs an instance of Spark JobServer) and will be provided to the job through this method. This relieves the developer from the boiler-plate configuration management that comes with the creation of a Spark job and allows the Job Server to manage and re-use contexts.
• validate allows for an initial validation of the context and any provided configuration. If the context and configuration are OK to run the job, returning spark.jobserver.SparkJobValid will let the job execute, otherwise returning spark.jobserver.SparkJobInvalid(reason) prevents the job from running and provides means to convey the reason of failure. In this case, the call immediately returns an HTTP/1.1 400 Bad Request status code. validate helps you preventing running jobs that will eventually fail due to missing or wrong configuration and save both time and resources.

See examples(link?) for Spark and spark streaming jobs in the <Product_root>/examples directory.

(Todo: In what way have we extended the SparkJob trait? )
SnappyData manages a SparkContext as a singleton but creates one SQLContext per incoming SQL Connection. 


#### Submitting jobs
Running Spark native programs … submitting and running jobs
```
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
```
This utility submits the job and returns a JSON that has a jobId of this job. 
```
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```
This job ID can be used to query the status of the running job. 
```
$ bin/snappy-job.sh status  \
    --lead hostNameOfLead:8090  \
    --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"

{
  "duration": "17.53 secs",
  "classPath": "io.snappydata.examples.CreateAndLoadAirlineDataJob",
  "startTime": "2016-01-12T16:59:14.746+05:30",
  "context": "snappyContext1452598154529305363",
  "result": "See /home/hemant/snappyhome/work/localhost-lead-1/CreateAndLoadAirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
}
```
Once the tables are created, they can be queried by firing another job. 
```
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.AirlineDataJob \
    --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
```
The status of this job can be queried in the same manner as shown above. The result of the this job will return a file path that has the query results. 


#### Streaming jobs

An implementation of SnappyStreamingJob can be submitted to lead of SnappyData by specifying --stream as a parameter to the snappy-job.sh. 
```
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.TwitterPopularTagsJob \
    --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar \ 
    --stream
```

### Running a standalone Spark compute cluster 

> NOTE: SnappyData, out-of-the-box, collocates Spark executors and the data store for efficient data intensive computations. 
> But, it may desirable to isolate the computational cluster for other reasons - for instance, a  computationally intensive Map-reduce machine learning algorithm that needs to iterate for a  cache data set repeatedly. 
> To support such scenarios it is also possible to run native Spark jobs that accesses a SnappyData cluster as a storage layer in a parallel fashion. 


