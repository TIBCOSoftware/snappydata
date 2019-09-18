# Structured Streaming Considerations

The following best practices for Structured Streaming are explained in this section:

*	[Using Shared File System as Checkpoint Directory Location](#sharefilesys)
*	[Limiting Batch Size](#limitbatchsize)
*	[Limiting Default Incoming Data Frame Size](#limitdefaultincoming)
*	[Running a Structured Streaming Query with Dedicated SnappySession Instance](#dedicatedsnappysession)

<a id= sharefilesys> </a>
## Using Shared File System as Checkpoint Directory Location

The location used to store checkpoint directory content should be on a shared file system like HDFS, which is accessible from all the nodes. This is required because the incremental aggregation state of a streaming query is stored as part of checkpoint directory itself. So if one of the executor nodes goes down, the aggregation state stored by that node needs to be accessible from the other executor nodes for the proper functioning of the streaming query.

<a id= limitbatchsize> </a>
## Limiting Batch Size
A good practice is to limit the batch size of a streaming query such that it remains below **spark.sql.autoBroadcastJoinThreshold** while using **Snappy** **Sink**.

This gives the following advantages:

**Snappy** **Sink** internally caches the incoming dataframe batch. If the batch size is too large, the cached dataframe might not fit in the memory and can start spilling over to the disk. This can lead to performance issues.

By limiting the batch size to **spark.sql.autoBroadcastJoinThreshold**, you can ensure that the **putInto** operation, that is performed as part of **Snappy** **Sink**, uses broadcast join which is significantly faster than sort merge join.

The batch size can be restricted using one of the following options depending upon the source:

### For Apache Kafka Source

**maxOffsetsPerTrigger** - Rate limit on the maximum number of offsets processed for each trigger interval. The specified total number of offsets are proportionally split across topic Partitions of different volume. (default: no max)

Example:

```
val streamingDF = snappySession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",”localhost:9091”)
      .option("maxOffsetsPerTrigger", 100)
      .option("subscribe", “topic1”)
      .load
```

### For File Sources

**maxFilesPerTrigger**- Maximum number of new files to be considered in every trigger (default: no max).

Example:

```
val inputPath = "/path/to/parquet_input"
val schema = snappy.read.parquet(inputPath).schema
val df = snappy.readStream.schema(schema)
  .option("maxFilesPerTrigger", 1)
  .format("parquet")
  .load(inputPath)
```

<a id= limitdefaultincoming> </a>
## Limiting Default Incoming Data Frame Size

Spark relies on the data size statistics provided by the sources to decide join type to be used for the query plan.
Some sources do not provide the correct size statistics and in such a case, Spark falls down to the default value, which is **Long.MaxValue** which is greater than **spark.sql.autoBroadcastJoinThreshold**. As a result of that the **putInto** join query always uses the sort merge join even if the incoming batch size is lesser than **spark.sql.autoBroadcastJoinThreshold**.

!!! Note
	A broadcast join is more performant than a sort merge join.

To overcome this, use the session level property **spark.sql.defaultSizeInBytesyou** and override the default size. The value set for this property should be approximately equal to the maximum batch size that you expect after complying to the suggestion mentioned in [Limiting Batch Size](#limitbatchsize) section.

It can be set using the following SQL command: 

```
set spark.sql.defaultSizeInBytes = <some long value>
```

For example:

```
set spark.sql.defaultSizeInBytes = 10000
```

Using SnappySession instance, you can be run the same as follows: 

```
snappySession.sql(“set spark.sql.defaultSizeInBytes = 10000”)
```

<a id= dedicatedsnappysession> </a>
## Running a Structured Streaming Query with Dedicated SnappySession Instance

A good practice is to run each structured streaming query using it’s own dedicated instance of SnappySession. 
A new instance of SnappySession can be created as follows:

```
val newSession = snappySession.newSession()
```

The newSession instance has a similar session level config as snappySession.

!!!Note
	For embedded snappy jobs, it is recommended to use a new snappy-job for each streaming query.

