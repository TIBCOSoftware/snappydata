## Building Snappy applications using Spark API

SnappyData bundles Spark and supports all the Spark APIs. You can create Object based RDDs and run transformations or use the rich higher level APIs. Working with the SnappyData Tables themselves builds on top of Spark SQL. So, we recommend getting to know the [concepts in SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) (and hence some core Spark concepts). 

You primarily interact with SnappyData tables using SQL (a richer, more compliant SQL) or the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes). And, you can store and manage arbitrary RDDs (or even Spark DataSets) through implicit or explicit transformation to a DataFrame. 

In Spark SQL, all tables are temporary and cannot be shared across different applications. While you can manage such temporary tables, SnappyData tables are automatically registered to a built-in catalog and persisted using the SnappyStore to disk (i.e. the tables will be there when the cluster recovers). This is similar to how Spark SQL uses the Hive catalog to natively work with Hive clusters. 


### SnappyContext
A SnappyContext is the main entry point for SnappyData extensions to Spark. A SnappyContext extends Spark's [SQLContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.SQLContext) to work with Row and Column tables. Any DataFrame can be managed as SnappyData tables and any table can be accessed as a DataFrame. This is similar to [HiveContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.hive.HiveContext) - integrates the SQLContext functionality with the Snappy store.

When running in the __embedded__ mode (i.e. Spark executor collocated with Snappy data store), Applications typically submit Jobs to the [Snappy-JobServer](https://github.com/SnappyDataInc/spark-jobserver) and do not explicitly create a SnappyContext. A single shared context managed by SnappyData makes it possible to re-use Executors across client connections or applications.

##### A simple example that uses SnappyContext to create table and query data 
Create a SnappyContext from SparkContext
```
val conf = new SparkConf().
               setAppName("ExampleTest").
               setMaster("local[*]"). 
               // Starting jobserver helps when you would want to test your jobs in a local mode. 
               set("jobserver.enabled", "true")
val sc = new SparkContext(conf) 
// get the SnappyContext
val snc: SnappyContext = SnappyContext.getOrCreate(sc)
```

Create columnar tables using API

```
  val props1 = Map(
    "BUCKETS" -> "2")
  case class Data(col1: Int, col2: Int, col3: Int)
  val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
  val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

  val dataDF = snc.createDataFrame(rdd)

  // create a column table
  // "column_table" is the name of the table
  // "column" is the table format (that is row or column)
  // dataDF.schema provides the schema for table
  snc.createTable("column_table", "column", dataDF.schema, props1)
  // insert the data in append mode
  dataDF.write.format("column").mode(SaveMode.Append).options(props1).saveAsTable("column_table")

  val results1 = snc.sql("select * from column_table")
  println("contents of column table are:")
  results1.foreach(println)
```

The optional BUCKETS attribute specifies the fixed number of "buckets," the smallest unit of data containment for the table that can be moved around. For more detailes about the properties ('props' map in above example) and createTable API refer to documentation for [row and column tables](https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md)

Create row tables using API

```
  // create a row format table called row_table
  // "row_table" is the name of the table
  // "row" is the table format (that is row or column)
  // dataDF.schema provides the schema for table
  val props2 = Map.empty[String, String]
  snc.createTable("row_table", "row", dataDF.schema, props2)

  // insert the data in append mode
  dataDF.write.format("row").mode(SaveMode.Append).options(props2).saveAsTable("row_table")

  val results2 = snc.sql("select * from row_table")
  println("contents of row table are:")
  results2.foreach(println)
```

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

```scala
class SnappyStreamingSampleJob implements SnappyStreamingJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runJob(sc: SnappyStreamingContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def validate(sc: SnappyContext, config: Config): SparkJobValidation
}
```

> The _Job_ traits are simply extensions of the _SparkJob_ implemented by [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver). 

• ```runJob``` contains the implementation of the Job. The SnappyContext/SnappyStreamingContext is managed by the SnappyData Leader (which runs an instance of Spark JobServer) and will be provided to the job through this method. This relieves the developer from the boiler-plate configuration management that comes with the creation of a Spark job and allows the Job Server to manage and re-use contexts.

• ```validate``` allows for an initial validation of the context and any provided configuration. If the context and configuration are OK to run the job, returning spark.jobserver.SparkJobValid will let the job execute, otherwise returning spark.jobserver.SparkJobInvalid(reason) prevents the job from running and provides means to convey the reason of failure. In this case, the call immediately returns an HTTP/1.1 400 Bad Request status code. validate helps you preventing running jobs that will eventually fail due to missing or wrong configuration and save both time and resources.

See [examples](https://github.com/SnappyDataInc/snappydata/tree/master/snappy-examples/src/main/scala/io/snappydata/examples) for Spark and spark streaming jobs. 

SnappySQLJob trait extends the SparkJobBase trait. It provides users the singleton SnappyContext object that may be reused across jobs. SnappyContext singleton object creates one SQLContext per incomig SQL connection. Similarly SnappyStreamingJob provides users access to SnappyStreamingContext object that can be reused across jobs



#### Submitting jobs
Following command submits [CreateAndLoadAirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/snappy-examples/src/main/scala/io/snappydata/examples/CreateAndLoadAirlineDataJob.scala) from the [snappy-examples](https://github.com/SnappyDataInc/snappydata/tree/master/snappy-examples/src/main/scala/io/snappydata/examples) directory.   This job creates dataframes from parquet files, loads the data from dataframe into column tables and row tables and creates sample table on column table in its runJob method. The program is compiled into a jar file (quickstart-0.1.0-SNAPSHOT.jar) and submitted to jobs server as shown below.

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

An implementation of SnappyStreamingJob can be submitted to lead of SnappyData by specifying --stream as a parameter to the snappy-job.sh.  For example [TwitterPopularTagsJob](https://github.com/SnappyDataInc/snappydata/blob/master/snappy-examples/src/main/scala/io/snappydata/examples/TwitterPopularTagsJob.scala) from the [snappy-examples](https://github.com/SnappyDataInc/snappydata/tree/master/snappy-examples/src/main/scala/io/snappydata/examples) directory an be submitted as follows. This job creates stream tables on tweet streams, registers conctinuous queries and prints results of queries such as top 10 hash tags of last two second, top 10 hash tags until now, top 10 popular tweets.

```
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.TwitterPopularTagsJob \
    --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar \ 
    --stream
```
