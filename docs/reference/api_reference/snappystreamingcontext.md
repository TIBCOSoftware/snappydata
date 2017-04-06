# SNAPPYSTREAMINGCONTEXT

Instance Constructors
-------------

<!--new-->
## SnappyStreamingContext
**Function**:
SnappyStreamingContext(path: String, sparkContext: SparkContext)

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Description**:
Recreate a SnappyStreamingContext from a checkpoint file using an existing SparkContext.

**Notes**:

<!--new-->
## SnappyStreamingContext
**Function**: SnappyStreamingContext
SnappyStreamingContext(path: String)

**Description**:
Recreate a SnappyStreamingContext from a checkpoint file.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!--new-->
## SnappyStreamingContext
**Function**:SnappyStreamingContext
SnappyStreamingContext(path: String, hadoopConf: Configuration)

**Description**:
Recreate a SnappyStreamingContext from a checkpoint file.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:


<!--new-->
## SnappyStreamingContext
**Function**:SnappyStreamingContext
SnappyStreamingContext(conf: SparkConf, batchDuration: Duration)

**Description**:
Create a SnappyStreamingContext by providing the configuration necessary for a new SparkContext.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:


<!--new-->
## SnappyStreamingContext
**Function**:SnappyStreamingContext
SnappyStreamingContext(sparkContext: SparkContext, batchDuration: Duration)

**Description**:
Create a SnappyStreamingContext using an existing SparkContext.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |


**Notes**:

Value Members 
-----------

<!-- def -->
## addStreamingListener

**Function**:addStreamingListener
addStreamingListener(streamingListener: StreamingListener): Unit

**Description**: Add a `org.apache.spark.streaming.scheduler.StreamingListener` object for receiving system events related to streaming.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## awaitTermination
**Function**:awaitTermination
awaitTermination(): Unit
Wait for the execution to stop.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## awaitTerminationOrTimeout

**Function**:awaitTerminationOrTimeout
awaitTerminationOrTimeout(timeout: Long): Boolean
Wait for the execution to stop.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:


<!-- def -->
## binaryRecordsStream
**Function**: binaryRecordsStream
binaryRecordsStream(directory: String, recordLength: Int): DStream[Array[Byte]]

**Description**:
Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them as flat binary files, assuming a fixed length per record, generating one byte array per record.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## checkpoint
**Function**:checkpoint
checkpoint(directory: String): Unit
Set the context to periodically checkpoint the DStream operations for driver fault-tolerance.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## createSchemaDStream
**Function**:createSchemaDStream
createSchemaDStream(rowStream: DStream[Row], schema: StructType): SchemaDStream

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## createSchemaDStream
**Function**:
createSchemaDStream[A <: Product](stream: DStream[A])(implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[A]): SchemaDStream
Creates a SchemaDStream from an DStream of Product (e.g.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## fileStream
**Function**:
fileStream[K, V, F <: InputFormat[K, V]](directory: String, filter: (Path) ⇒ Boolean, newFilesOnly: Boolean, conf: Configuration)(implicit arg0: ClassTag[K], arg1: ClassTag[V], arg2: ClassTag[F]): InputDStream[(K, V)]

**Description**:
Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them using the given key-value types and input format.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:


<!-- def -->
## fileStream

**Function**:
fileStream[K, V, F <: InputFormat[K, V]](directory: String, filter: (Path) ⇒ Boolean, newFilesOnly: Boolean)(implicit arg0: ClassTag[K], arg1: ClassTag[V], arg2: ClassTag[F]): InputDStream[(K, V)]

**Description**:
Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them using the given key-value types and input format.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## fileStream
**Function**:
fileStream[K, V, F <: InputFormat[K, V]](directory: String)(implicit arg0: ClassTag[K], arg1: ClassTag[V], arg2: ClassTag[F]): InputDStream[(K, V)]

**Description**:
Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them using the given key-value types and input format.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## getSchemaDStream
**Function**:
getSchemaDStream(tableName: String): SchemaDStream

**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## getState
**Function**:
getState(): StreamingContextState
:: DeveloperApi ::

**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## queueStream
**Function**:
queueStream[T](queue: Queue[RDD[T]], oneAtATime: Boolean, <!-- def -->aultRDD: RDD[T])(implicit arg0: ClassTag[T]): InputDStream[T]

**Description**:
Create an input stream from a queue of RDDs.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## queueStream
**Function**:
queueStream[T](queue: Queue[RDD[T]], oneAtATime: Boolean = true)(implicit arg0: ClassTag[T]): InputDStream[T]

**Description**:
Create an input stream from a queue of RDDs.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## rawSocketStream
**Function**:
rawSocketStream[T](hostname: String, port: Int, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)(implicit arg0: ClassTag[T]): ReceiverInputDStream[T]

**Description**:
Create an input stream from network source hostname:port, where data is received as serialized blocks (serialized using the Spark's serializer) that can be directly pushed into the block manager without deserializing them.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## receiverStream
**Function**:
receiverStream[T](receiver: Receiver[T])(implicit arg0: ClassTag[T]): ReceiverInputDStream[T]

**Description**:
Create an input stream with any arbitrary user implemented receiver.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## registerCQ
**Function**:
registerCQ(queryStr: String): SchemaDStream

**Description**:
Registers and executes given SQL query and returns SchemaDStream to consume the results

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## remember
**Function**:
remember(duration: Duration): Unit
Set each DStream in this context to remember RDDs it generated in the last given duration.

**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- val-->
## snappyContext
**Function**:
snappyContext: SnappyContext

**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- val -->
## snappySession
**Function**:
snappySession: SnappySession

**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## socketStream
**Function**:
socketStream[T](hostname: String, port: Int, converter: (InputStream) ⇒ Iterator[T], storageLevel: StorageLevel)(implicit arg0: ClassTag[T]): ReceiverInputDStream[T]

**Description**:
Creates an input stream from TCP source hostname:port.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## socketTextStream
**Function**:
socketTextStream(hostname: String, port: Int, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2): ReceiverInputDStream[String]

**Description**:
## 
Creates an input stream from TCP source hostname:port.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## sparkContext
**Function**:
sparkContext: SparkContext

**Description**:
Return the associated Spark context

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## sql
**Function**:
sql(sqlText: String): DataFrame


**Description**:

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## start
**Function**:
start(): Unit
Start the execution of the streams.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Description**:

**Notes**:

<!-- def -->
## stop
**Function**:
stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit

**Description**:
Stop the execution of the streams, with option of ensuring all received data has been processed.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## stop

**Function**:
stop(stopSparkContext: Boolean = ...): Unit

**Description**:
Stop the execution of the streams immediately (does not wait for all received data to be processed).

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## textFileStream

**Function**:
textFileStream(directory: String): DStream[String]

**Description**:
Create an input stream that monitors a Hadoop-compatible filesystem for new files and reads them as text files (using key as LongWritable, value as Text and input format as TextInputFormat).

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## transform

**Function**:
transform[T](dstreams: Seq[DStream[_]], transformFunc: (Seq[RDD[_]], Time) ⇒ RDD[T])(implicit arg0: ClassTag[T]): DStream[T]

**Description**:
Create a new DStream in which each RDD is generated by applying a function on RDDs of the DStreams.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:

<!-- def -->
## union

**Function**:
union[T](streams: Seq[DStream[T]])(implicit arg0: ClassTag[T]): DStream[T]
 Permalink

**Description**:
Create a unified DStream from multiple DStreams of the same type and same slide duration.

**Parameters**:

| Parameter | Description |
|--------|--------|
| |  |
| |  |
| |  |

**Notes**:
