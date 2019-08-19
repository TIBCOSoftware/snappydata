# Structured Streaming Quick Reference

This quick start guide provides step-by-step instructions to perform structured streaming in TIBCO ComputeDB by using the Spark shell as well as through a Snappy job.

For detailed information, refer to  [Structured Streaming](/howto/use_stream_processing_with_snappydata.md#structuredstreaming).

## Structured Streaming using Spark Shell

Following are the steps to perform structured streaming using Spark shell:

1.	Start TIBCO ComputeDB cluster using the following command.

		./sbin/snappy-start-all

2.	Open a new terminal window and start Netcat connection listening to TCP port 9999:

		nc -lk 9999

3.	Produce some input data in JSON format and keep the terminal running with Netcat connection.

	**Example**:

        {"id":"device1", "signal":10}
        {"id":"device2", "signal":20}
        {"id":"device3", "signal":30}
        
4.	Open a new terminal window, go to TIBCO ComputeDB distribution directory and start Spark shell using the following command     :

		./bin/spark-shell --master local[*] --conf spark.snappydata.connection=localhost:1527

5.	Execute the following code to start a structure streaming query from Spark shell:

        
        import org.apache.spark.sql.SnappySession
        import org.apache.spark.sql.functions.from_json
        import org.apache.spark.sql.streaming.ProcessingTime

        val snappy = new SnappySession(sc)

        // Create target snappy table. Stream data will be ingested in this table
        snappy.sql("create table if not exists devices(id string, signal int) using column")

        val schema = snappy.table("devices").schema

        // Create streaming DataFrame representing the stream of input lines from socket connection
        val df = snappy.
          readStream.
          format("socket").
          option("host", "localhost").
          option("port", 9999).
          load()

        // Start the execution of streaming query
        val streamingQuery = df.
          select(from_json(df.col("value").cast("string"), schema).alias("jsonObject")).
          selectExpr("jsonObject.*").
          writeStream.
          format("snappysink").
          queryName("deviceStream").  // must be unique across the TIBCO ComputeDB cluster
          trigger(ProcessingTime("1 seconds")).
          option("tableName", "devices").
          option("checkpointLocation", "/path/to/checkpointLocation").
          start()
        

6.	Open a new terminal window, navigate to TIBCO ComputeDB distribution directory and start Snappy SQL:

		./bin/snappy-sql

7. 	Connect to running TIBCO ComputeDB cluster using the following command:

		connect client 'localhost:1527';

8.	Check whether the data produced from Netcat connection is getting ingested in the target table:

		select * from devices;

	You can produce some more data on the Netcat connection in JSON format and check whether it is getting ingested in the `devices` table. To stop the streaming query, run the following command in the Spark shell terminal where you started the streaming query.

		streamingQuery.stop

### Structured Streaming  with Kafka Source

Assuming that your Kafka cluster is already setup and running, you can use the following steps to run a structured streaming query using Spark shell:

1.	Start TIBCO ComputeDB cluster using following command.

		./sbin/snappy-start-all

2.	Create a topic named "devices":

		./bin/kafka-topics --create --zookeeper zookeper_server:2181 --partitions 4 --replication-factor 1 --topic devices

3.	Start a console produce in new terminal window and produce some data in JSON format:

		./bin/kafka-console-producer --broker-list kafka_broker:9092 --topic devices

	Example data:

        
        {"id":"device1", "signal":10}
        {"id":"device2", "signal":20}
        {"id":"device3", "signal":30}
        

4.	Open a new terminal window, go to TIBCO ComputeDB distribution directory and start Spark shell using the following command:

		./bin/spark-shell --master local[*] --conf spark.snappydata.connection=localhost:1527

5.	Execute the following code to start a structure streaming query from spark-shell:

        
        import org.apache.spark.sql.SnappySession
        import org.apache.spark.sql.functions.from_json
        import org.apache.spark.sql.streaming.ProcessingTime

        val snappy = new SnappySession(sc)

        // Create target snappy table. Stream data will be ingested in this table.
        snappy.sql("create table if not exists devices(id string, signal int) using column")

        val schema = snappy.table("devices").schema

        // Create DataFrame representing the stream of input lines from Kafka topic 'devices'
        val df = snappy.
          readStream.
          format("kafka").
          option("kafka.bootstrap.servers", "kafka_broker:9092").  
          option("startingOffsets", "earliest").
          option("subscribe", "devices").
          option("maxOffsetsPerTrigger", 100).  // to restrict the batch size
          load()

        // Start the execution of streaming query
        val streamingQuery = df.
          select(from_json(df.col("value").cast("string"), schema).alias("jsonObject")).
          selectExpr("jsonObject.*").
          writeStream.
          format("snappysink").
          queryName("deviceStream").  // must be unique across the TIBCO ComputeDB cluster
          trigger(ProcessingTime("1 seconds")).
          option("tableName", "devices").
          option("checkpointLocation", "/path/to/checkpointLocation").
          start()
6.	Open a new terminal window, navigate to TIBCO ComputeDB distribution directory and start Snappy SQL:

		./bin/snappy-sql

7.	Connect to the running TIBCO ComputeDB cluster using the following command:

		connect client 'localhost:1527';

8.	Check whether the data produced from netcat connection is getting ingested in the target table:
		select * from devices;

	You can produce some more data on the Netcat connection in JSON format and check whether it is getting ingested in the devices` table. To stop the streaming query, run the following command in the Spark shell terminal where you started the streaming query.

		streamingQuery.stop

## Structured Streaming using Snappy Job

Refer to [TIBCO ComputeDB Jobs](../programming_guide/snappydata_jobs.md) for more information about Snappy Jobs.

Following is a Snappy job code that contains similarly structured streaming query:

```
package io.snappydata

import com.typesafe.config.Config
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object Example extends SnappySQLJob {
  override def isValidJob(snappy: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappy: SnappySession, jobConfig: Config): Any = {

    // Create target snappy table. Stream data will be ingested in this table
    snappy.sql("create table if not exists devices(id string, signal int) using column")

    val schema = snappy.table("devices").schema

    // Create DataFrame representing the stream of input lines from socket connection
    val df = snappy.
      readStream.
      format("socket").
      option("host", "localhost").
      option("port", 9999).
      load()

    // start the execution of streaming query
    val streamingQuery = df.
      select(from_json(df.col("value").cast("string"), schema).alias("jsonObject")).
      selectExpr("jsonObject.*").
      writeStream.
      format("snappysink").
      queryName("deviceStream"). // must be unique across the TIBCO ComputeDB cluster
      trigger(ProcessingTime("1 seconds")).
      option("tableName", "devices").
      option("checkpointLocation", "/path/to/checkpointLocation").
      start()

    streamingQuery.awaitTermination()
  }
}
```

Package the above code as part of a jar and submit using the following command:

```
./bin/snappy-job.sh submit  --app-name exampleApp --class io.snappydata.Example --app-jar /path/to/application.jar
```

Output:

```
OKOK{
  "status": "STARTED",
  "result": {
    "jobId": "6cdce50f-e86d-4da0-a34c-3804f7c6155b",
    "context": "snappyContext1561122043543655176"
  }
}
```

Use the following command to stop the running job:

```
./bin/snappy-job.sh stop --job-id 6cdce50f-e86d-4da0-a34c-3804f7c6155b
```

!!!Note
	The job-id used for stopping the job is picked from the job submission response.

## Examples

For more examples, refer [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming) 



