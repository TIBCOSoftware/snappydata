### Getting Started with Spark API 

[SnappyContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext) is the main entry point for SnappyData extensions to Spark. A SnappyContext extends Spark's [SQLContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.SQLContext) to work with Row and Column tables. Any DataFrame can be managed as a SnappyData table and any table can be accessed as a DataFrame. This is similar to [HiveContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.hive.HiveContext) and it integrates the SQLContext functionality with the SnappyData store. Similarly, [SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.streaming.SnappyStreamingContext) is an entry point for SnappyData extensions to Spark Streaming and it extends Spark's [Streaming Context](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.streaming.StreamingContext). 

Applications typically submit Jobs to SnappyData and do not explicitly create a SnappyContext or SnappyStreamingContext. These jobs are the primary mechanism to interact with SnappyData using the Spark API. A job implements either SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. 

```scala
class SnappySampleJob implements SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runJob(snc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def validate(sc: SnappyContext, config: Config): SparkJobValidation
}
```
The implementation of the _runJob_ function from SnappySQLJob uses a SnappyContext to interact with the SnappyData store to process and store tables. The implementation of runJob from SnappyStreamingJob uses a SnappyStreamingContext to create streams and manage the streaming context. The jobs are submitted to the lead node of SnappyData over REST API using a _spark-submit_ like utility. See more details about jobs here: [SnappyData Jobs](./jobs.md)
