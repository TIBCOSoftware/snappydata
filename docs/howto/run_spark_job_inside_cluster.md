<a id="howto-job"></a>
# How to Run Spark Job inside the Cluster
Spark program that runs inside a SnappyData cluster is implemented as a SnappyData job.

**Implementing a Job**: 
A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. In the `runSnappyJob` method of the job, you implement the logic for your Spark program using SnappySession object instance passed to it. You can perform all operations such as create a table, load data, execute queries using the SnappySession. <br/>
Any of the Spark APIs can be invoked by a SnappyJob.

```pre
class CreatePartitionedRowTable extends SnappySQLJob {
  /** SnappyData uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappySession, jobConfig: Config): Any

  /**
  SnappyData calls this function to validate the job input and reject invalid job requests.
  You can implement custom validations here, for example, validating the configuration parameters
  **/
  def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
}
```

**Dependencies**:
To compile your job, use the Maven/SBT dependencies for the latest released version of SnappyData.

**Example: Maven dependency**:

```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-cluster_2.11</artifactId>
    <version>1.2.0</version>
</dependency>
```

**Example: SBT dependency**:

```pre
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.2.0"
```

!!! Note
	
    If your project fails while resolving the above dependency (ie. it fails to download javax.ws.rs#javax.ws.rs-api;2.1), it may be due an issue with its pom file. </br> As a workaround, add the below code to the `build.sbt`:

```pre
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).

**Running the Job**: 
Once you create a jar file for SnappyData job, use the `./bin/snappy-job.sh` to submit the job in the SnappyData cluster, and then run the job. This is similar to `spark-submit` for any Spark application. 

For example, to run the job implemented in [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala) you can use the following command. The command submits the job and runs it as:

```pre
 # first change the directory to the SnappyData product directory
 $ cd $SNAPPY_HOME
 $ ./bin/snappy-job.sh submit
    --app-name CreatePartitionedRowTable
    --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
    --app-jar examples/jars/quickstart.jar
    --lead localhost:8090
```
In the above command, **quickstart.jar** contains the program and is bundled in the product distribution and the **--lead** option specifies the host name of the lead node along with the port on which it accepts jobs (default 8090).

**Output**: It returns output similar to:

```pre
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```

**Check Status**: You can check the status of the job using the Job ID listed above:

```pre
./bin/snappy-job.sh status --lead localhost:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48
```

Refer to the [Building SnappyData applications using Spark API](../programming_guide/building_snappydata_applications_using_spark_api.md) section of the documentation for more details.
