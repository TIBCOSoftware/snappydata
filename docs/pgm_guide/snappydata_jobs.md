## SnappyData Jobs
To create a job that can be submitted through the job server, the job must implement the **SnappySQLJob** or **SnappyStreamingJob** trait. Your job is displayed as:
 
#### Scala

```scala
class SnappySampleJob implements SnappySQLJob {
  /** SnappyData uses this as an entry point to execute SnappyData jobs. **/
  def runSnappyJob(snappy: SnappySession, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(snappy: SnappySession, config: Config): SnappyJobValidation
}
```

#### Java
```java
class SnappySampleJob extends SnappySQLJob {
  /** SnappyData uses this as an entry point to execute SnappyData jobs. **/
  public Object runSnappyJob(SnappySession snappy, Config jobConfig) {//Implementation}

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(SnappySession snappy, Config config) {//validate}
}

```

#### Scala
```scala
class SnappyStreamingSampleJob implements SnappyStreamingJob {
  /** SnappyData uses this as an entry point to execute SnappyData jobs. **/
  def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation
}
```

#### Java
```java
class SnappyStreamingSampleJob extends JavaSnappyStreamingJob {
  /** SnappyData uses this as an entry point to execute SnappyData jobs. **/
  public Object runSnappyJob(JavaSnappyStreamingContext snsc, Config jobConfig) {//implementation }

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig)
  {//validate}
}
```

!!! Note
	The _Job_ traits are simply extensions of the _SparkJob_ implemented by [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver). 

* `runSnappyJob` contains the implementation of the Job.
The [SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession)/[SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) is managed by the SnappyData Leader (which runs an instance of Spark JobServer) and is provided to the job through this method. This relieves the developer from the boiler-plate configuration management that comes with the creation of a Spark job and allows the Job Server to manage and re-use contexts.

* `isValidJob` allows for an initial validation of the context and any provided configuration.
    If the context and configuration can run the job, returning `spark.jobserver.SnappyJobValid` allows the job to execute, otherwise returning `spark.jobserver.SnappyJobInvalid<reason>` prevents the job from running and provides means to convey the reason of failure. In this case, the call immediately returns an "HTTP/1.1 400 Bad Request" status code. Validate helps you prevent running jobs that eventually fail due to a  missing or wrong configuration, and saves both time and resources.

See [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) for Spark and Spark streaming jobs. 

SnappySQLJob trait extends the SparkJobBase trait. It provides users the singleton SnappyContext object that may be reused across jobs. SnappyContext singleton object creates one SQLContext per incoming SQL connection. Similarly, SnappyStreamingJob provides users access to SnappyStreamingContext object that can be reused across jobs.

### Submitting Jobs
The following command submits [CreateAndLoadAirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/CreateAndLoadAirlineDataJob.scala). This job creates DataFrames from parquet files, loads the data from DataFrame into column tables and row tables, and creates sample table on column table in its `runJob` method. 
The program is compiled into a jar file (**quickstart.jar**) and submitted to jobs server as shown below.

```bash
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```
The utility `snappy-job.sh` submits the job and returns a JSON that has a Job Id of this job.

- `--lead`: Specifies the host name of the lead node along with the port on which it accepts jobs (8090)

- `--app-name`: Specifies the name given to the submitted application

-  `--class`: Specifies the name of the class that contains implementation of the Spark job to be run

-  `--app-jar`: Specifies the jar file that packages the code for Spark job

The status returned by the utility is displayed below:

```json
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```
This Job ID can be used to query the status of the running job. 
```bash
$ bin/snappy-job.sh status  \
    --lead hostNameOfLead:8090  \
    --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48

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
Once the tables are created, they can be queried by running another job. Please refer to [AirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/AirlineDataJob.scala) for implementing the job. 
```bash
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.AirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```
The status of this job can be queried in the same manner as shown above. The result of the job returns a file path that has the query results.

### Running Python Applications
Python users can submit a Python application using `spark-submit` in the SnappyData Connector mode. For example, run the command given below to submit a Python application:

```bash
$ bin/spark-submit \
  --master local[*]
  --conf snappydata.store.locators=localhost:10334 \
  --conf spark.ui.port=4042
  quickstart/python/AirlineDataPythonApp.py
```
`snappydata.store.locators` property denotes the locator URL of the SnappyData cluster and it is used to connect to the SnappyData cluster.

### Streaming Jobs

An implementation of SnappyStreamingJob can be submitted to the lead node of SnappyData cluster by specifying ```--stream``` as an option to the submit command. This option creates a new SnappyStreamingContext before the job is submitted. 
Alternatively, you can specify the name of an existing/pre-created streaming context as `--context <context-name>` with the `submit` command.

For example, [TwitterPopularTagsJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/TwitterPopularTagsJob.scala) can be submitted as follows. 
This job creates stream tables on tweet streams, registers continuous queries and prints results of queries such as top 10 hash tags of last two second, top 10 hash tags until now, and top 10 popular tweets.

```bash
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.TwitterPopularTagsJob \
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

```bash
$ bin/snappy-job.sh stop  \
    --lead hostNameOfLead:8090  \
    --job-id 982ac142-3550-41e1-aace-6987cb39fec8

$ bin/snappy-job.sh listcontexts  \
    --lead hostNameOfLead:8090
["snappyContext1452598154529305363", "snappyStreamingContext1463987084945028747", "snappyStreamingContext"]

$ bin/snappy-job.sh stopcontext snappyStreamingContext1463987084945028747  \
    --lead hostNameOfLead:8090
```