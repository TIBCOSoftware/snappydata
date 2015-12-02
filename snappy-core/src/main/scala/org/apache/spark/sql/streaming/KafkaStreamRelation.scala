package org.apache.spark.sql.streaming


import kafka.serializer.StringDecoder
import org.apache.spark.Logging
import org.apache.spark.sql.{SnappyContext, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by ymahajan on 25/09/15.
 */
case class KafkaStreamRelation(@transient val sqlContext: SQLContext,
                               options: Map[String, String],
                               override val schema: StructType)
  extends StreamBaseRelation with DeletableRelation
  with DestroyRelation with Logging with StreamPlan with Serializable {

  val ZK_QUORUM = "zkquorum"
  //Zookeeper quorum (hostname:port,hostname:port,..)
  val GROUP_ID = "groupid"
  //The group id for this consumer
  val TOPICS = "topics" //Map of (topic_name -> numPartitions) to consume

  val KAFKA_PARAMS = "kafkaparams"
  //Kafka configuration parameters ("metadata.broker.list" or "bootstrap.servers")
  val FROM_OFFSETS = "fromoffsets"
  //Per-topic/partition Kafka offsets defining the (inclusive) starting point of the stream
  val MESSAGE_HINDLER = "messagehandler" //Function for translating each message and metadata into the desired type

  @transient val context = StreamingCtxtHolder.streamingContext

  val storageLevel = options.get("storageLevel")
    .map(StorageLevel.fromString)
    .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)


  /*import scala.reflect.runtime.{universe => ru}
  @transient val formatter = StreamUtils.loadClass(options("formatter")).newInstance() match {
  case f: StreamFormatter[_] => f.asInstanceOf[StreamFormatter[Any]]
  case f => throw new AnalysisException(s"Incorrect StreamFormatter $f")
  }*/

  private val streamToRow = {
    try {
      val clz = StreamUtils.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[MessageToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }
  
  @transient private val kafkaStream = if (options.exists(_._1 == ZK_QUORUM)) {
    val zkQuorum: String = options(ZK_QUORUM)
    val groupId: String = options(GROUP_ID)

    val topics: Map[String, Int] = options(TOPICS).split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    KafkaUtils.createStream(context, zkQuorum, groupId, topics, storageLevel)
  } else {
    //Direct Kafka
    val topicsSet = options(TOPICS).split(",").toSet
    val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
      t.split(", ").map { s =>
        val a = s.split("->")
        (a(0), a(1))
      }.toMap
    }.getOrElse(Map())

    // Currently works with Strings only
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
     context, kafkaParams, topicsSet)
    //TODO Yogesh, need to provide typed decoders to createDirectStream
  }

  @transient val stream: DStream[InternalRow] = kafkaStream.map(_._2).map(streamToRow.toRow)

  override def destroy(ifExists: Boolean): Unit = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  override def delete(filterExpr: String): Int = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  def truncate(): Unit = {
    throw new IllegalAccessException("Stream tables cannot be truncated")
  }
}
