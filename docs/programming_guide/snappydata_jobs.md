<a id="TIBCO ComputeDB-jobs"></a>
# Snappy Jobs

To create a job that can be submitted through the job server, the job must implement the **SnappySQLJob** or **SnappyStreamingJob** trait. The structure of a job looks as below:
 
**Scala**

```pre
object SnappySampleJob extends SnappySQLJob {
  /** TIBCO ComputeDB uses this as an entry point to execute Snappy jobs. **/
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
}
  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

}
```

**Java**
```pre
class SnappySampleJob extends JavaSnappySQLJob {
  /** TIBCO ComputeDB uses this as an entry point to execute Snappy jobs. **/
  public Object runSnappyJob(SnappySession snappy, Config jobConfig) {//Implementation}

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(SnappySession snappy, Config config) {//validate}
}

```

**Scala**
```pre
object SnappyStreamingSampleJob extends SnappyStreamingJob {
  /** TIBCO ComputeDB uses this as an entry point to execute Snappy jobs. **/
  override def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any = {
}
  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  override def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
```

**Java**
```pre
class SnappyStreamingSampleJob extends JavaSnappyStreamingJob {
  /** TIBCO ComputeDB uses this as an entry point to execute Snappy jobs. **/
  public Object runSnappyJob(JavaSnappyStreamingContext snsc, Config jobConfig) {//implementation }

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig)
  {//validate}
}
```

!!! Note
	The _Job_ traits are simply extensions of the _SparkJob_ implemented by [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver). 

* `runSnappyJob` contains the implementation of the Job.
The [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession)/[SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) is managed by the TIBCO ComputeDB Leader (which runs an instance of Spark JobServer) and is provided to the job through this method. This relieves the developer from configuration management that comes with the creation of a Spark job and allows the Job Server to manage and reuse contexts.

* `isValidJob` allows for an initial validation of the context and any provided configuration.
    If the context and configuration can run the job, returning `spark.jobserver.SnappyJobValid` allows the job to execute, otherwise returning `spark.jobserver.SnappyJobInvalid<reason>` prevents the job from running and provides means to convey the reason for failure. In this case, the call immediately returns an "HTTP/1.1 400 Bad Request" status code. Validate helps you prevent running jobs that eventually fail due to a  missing or wrong configuration, and saves both time and resources.

See [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) for Spark and Spark streaming jobs. 

SnappySQLJob trait extends the SparkJobBase trait. It provides users the singleton SnappyContext object that may be reused across jobs. SnappyContext singleton object creates one SnappySession per job. Similarly, SnappyStreamingJob provides users access to SnappyStreamingContext object that can be reused across jobs.

## Submitting Jobs

The following command submits [CreateAndLoadAirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/CreateAndLoadAirlineDataJob.scala). This job creates DataFrames from parquet files, loads the data from DataFrame into column tables and row tables, and creates sample table on column table in its `runJob` method.

!!! Note
	When submitting concurrent jobs user must ensure that the `--app-name` parameter is different for each concurrent job. If two applications with the same name are submitted concurrently, the job fails and an error is reported, as the job server maintains a map of the application names and jar files used for that application.

The program must be compiled and bundled as a jar file and submitted to jobs server as shown below:

```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```

The utility `snappy-job.sh` submits the job and returns a JSON that has a Job Id of this job.

- `--lead`: Specifies the host name of the lead node along with the port on which it accepts jobs (8090)

- `--app-name`: Specifies the name given to the submitted application

-  `--class`: Specifies the name of the class that contains implementation of the Spark job to be run

-  `--app-jar`: Specifies the jar file that packages the code for Spark job

-  `--packages`: Specifies the packages names, which must be comma separated. These package names can be used to inform Spark about all the dependencies of a job. For more details, refer to [Deploying Dependency Jars](/connectors/deployment_dependency_jar.md).

The status returned by the utility is displayed below:

```pre
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```
This Job ID can be used to query the status of the running job. 

```pre
$ ./bin/snappy-job.sh status  \
    --lead localhost:8090  \
    --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48

{
  "duration": "17.53 secs",
  "classPath": "io.snappydata.examples.CreateAndLoadAirlineDataJob",
  "startTime": "2016-01-12T16:59:14.746+05:30",
  "context": "snappyContext1452598154529305363",
  "result": "See /home/user1/snappyhome/work/localhost-lead-1/CreateAndLoadAirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
}
```
Once the tables are created, they can be queried by running another job. Please refer to [AirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/AirlineDataJob.scala) for implementing the job. 

```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.AirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```
The status of this job can be queried in the same manner as shown above. The result of the job returns a file path that has the query results.

### Jar Dependencies for Jobs

For writing jobs users need to include [Maven/SBT dependencies for the latest released version of SnappyData](../howto/run_spark_job_inside_cluster.md) to their project dependencies. In case the project already includes dependency on Apache Spark and the user does not want to include snappy-spark dependencies, then, it is possible to explicitly exclude the snappy-spark dependencies.

For example, gradle can be configured as:

```pre
compile('io.snappydata:snappydata-cluster_2.11:1.1.1') {
        exclude(group: 'io.snappydata', module: 'snappy-spark-unsafe_2.11')
        exclude(group: 'io.snappydata', module: 'snappy-spark-core_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-yarn_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-hive-thriftserver_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-streaming-kafka-0.10_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-repl_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-sql_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-mllib_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-streaming_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-catalyst_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-hive_2.11')
        exclude(group: 'io.snappydata',module: 'snappy-spark-graphx_2.11')
    }
```

## Running Python Applications

Python users can submit a Python application using `./bin/spark-submit` in the TIBCO ComputeDB Smart Connector mode. Run the following command to submit a Python application:

```pre
./bin/spark-submit \
    --master local[*]  \
    --conf snappydata.connection=localhost:1527 \
    --conf spark.ui.port=4042 ./quickstart/python/CreateTable.py
```

`snappydata.connection` property is a combination of locator host and JDBC client port on which the locator listens for connections (default 1527). It is used to connect to the SnappyData cluster.

!!! Note
	For running ML/MLlib applications you need to install appropriate python packages(if your application uses any).</br>
	KMeans uses numpy hence you need to install numpy package before using Spark KMeans.</br>
	For example `sudo apt-get install python-numpy`

**Related Topic**:

- [How to use Python to Create Tables and Run Queries](../howto/use_python_to_create_tables_and_run_queries.md)

## Streaming Jobs

An implementation of SnappyStreamingJob can be submitted to the lead node of TIBCO ComputeDB cluster by specifying `--stream` as an option to the submit command. This option creates a new SnappyStreamingContext before the job is submitted. 
Alternatively, you can specify the name of an existing/pre-created streaming context as `--context <context-name>` with the `submit` command.

For example, [TwitterPopularTagsJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/TwitterPopularTagsJob.scala) can be submitted as follows. 
This job creates stream tables on tweet streams, registers continuous queries and prints results of queries such as top 10 hash tags of last two second, top 10 hash tags until now, and top 10 popular tweets.

```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.TwitterPopularTagsJob \
    --conf streaming.batch_interval=5000 \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar \
    --stream

{
  "status": "STARTED",
  "result": {
    "jobId": "982ac142-3550-41e1-aace-6987cb39fec8",
    "context": "snappyStreamingContext1463987084945028747"
  }
}
```
To start another streaming job with a new streaming context, you need to first stop the currently running streaming job, followed by its streaming context.

```pre
$ ./bin/snappy-job.sh stop  \
    --lead localhost:8090  \
    --job-id 982ac142-3550-41e1-aace-6987cb39fec8

$ ./bin/snappy-job.sh listcontexts  \
    --lead localhost:8090
["snappyContext1452598154529305363", "snappyStreamingContext1463987084945028747", "snappyStreamingContext"]

$ ./bin/snappy-job.sh stopcontext snappyStreamingContext1463987084945028747  \
    --lead localhost:8090
```
## Snappy Job Examples

You can import the examples into a separate independent gradle project as is and submit the jobs to the cluster.
Refer to the instructions [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/README.md). The link also contains instructions for importing and running examples from an IDE such as Intellij IDEA.


