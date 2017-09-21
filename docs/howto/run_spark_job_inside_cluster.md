<a id="howto-job"></a>
# How to Run Spark Code inside the Cluster
A Spark program that runs inside a SnappyData cluster is implemented as a SnappyData job.

**Implementing a Job**: 
A SnappyData job is a class or object that implements SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. In the `runSnappyJob` method of the job, you implement the logic for your Spark program using SnappySession object instance passed to it. You can perform all operations such as create a table, load data, execute queries using the SnappySession. <br/>
Any of the Spark APIs can be invoked by a SnappyJob.

```scala
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

```scala
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11 -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-cluster_2.11</artifactId>
    <version>1.0.0-rc1.1</version>
</dependency>
```

**Example: SBT dependency**:

```scala
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.0.0-rc1.1"
```

**Running the Job**: 
Once you create a jar file for SnappyData job, use `bin/snappy-job.sh` to submit the job to SnappyData cluster and run the job. This is similar to Spark-submit for any Spark application. 

For example, to run the job implemented in [CreatePartitionedRowTable.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/CreatePartitionedRowTable.scala) you can use the following command.
Here **quickstart.jar** contains the program and is bundled in the product distribution.<br/>
For example, the command submits the job and runs it as:

```scala
 # first cd to your SnappyData product dir
 $ cd $SNAPPY_HOME
 $ bin/snappy-job.sh submit
    --app-name CreatePartitionedRowTable
    --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
    --app-jar examples/jars/quickstart.jar
    --lead hostNameOfLead:8090
```
In the above comand, **--lead** option specifies the host name of the lead node along with the port on which it accepts jobs (default 8090).

**Output**: It returns output similar to:

```scala
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```

**Check Status**: You can check the status of the job using the Job ID listed above:

```scala
bin/snappy-job.sh status --lead hostNameOfLead:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48
```

Refer to the [Building SnappyData applications using Spark API](../programming_guide/building_snappydata_applications_using_spark_api.md) section of the documentation for more details.
