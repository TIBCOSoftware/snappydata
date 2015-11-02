package org.apache.spark.sql.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by ymahajan on 25/09/15.
 */
class KafkaStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String], schema: StructType) = {

    val ZK_QUORUM = "zkquorum" //Zookeeper quorum (hostname:port,hostname:port,..)
    val GROUP_ID = "groupid" //The group id for this consumer
    val TOPICS = "topics" //Map of (topic_name -> numPartitions) to consume

    val KAFKA_PARAMS = "kafkaparams" //Kafka configuration parameters ("metadata.broker.list" or "bootstrap.servers")
    val FROM_OFFSETS = "fromoffsets" //Per-topic/partition Kafka offsets defining the (inclusive) starting point of the stream
    val MESSAGE_HINDLER = "messagehandler" //Function for translating each message and metadata into the desired type

    val context = StreamingCtxtHolder.streamingContext
    //TODO: Yogesh, remove this dependency on checkpoint
    context.checkpoint("tmp")

    val storageLevel = options.get("storageLevel")
      .map(StorageLevel.fromString)
      .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

    import scala.reflect.runtime.{universe => ru}

    val formatter = StreamUtils.loadClass(options("formatter")).newInstance() match {
      case f: StreamFormatter[_] => f.asInstanceOf[StreamFormatter[Any]]
      case f => throw new AnalysisException(s"Incorrect StreamFormatter $f")
    }

    if (options.exists(_._1 == ZK_QUORUM)) {
      val zkQuorum: String = options(ZK_QUORUM)
      val groupId: String = options(GROUP_ID)

      val topics: Map[String, Int] = options(TOPICS).split(",").map { s =>
        val a = s.split(":")
        (a(0), a(1).toInt)
      }.toMap

      val dStream = KafkaUtils.createStream(context, zkQuorum, groupId, topics, storageLevel)
      KafkaStreamRelation(dStream.asInstanceOf[DStream[Any]], options, formatter.format,
        schema, sqlContext)
    } else {
      //Direct Kafka
      val topicsSet = options(TOPICS).split(",").toSet
      val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
        t.split(",").map { s =>
          val a = s.split("->")
          (a(0), a(1))
        }.toMap
      }.getOrElse(Map())

      val dStream = KafkaUtils.createDirectStream(
        context, kafkaParams, topicsSet)

      KafkaStreamRelation(dStream.asInstanceOf[DStream[Any]], options, formatter.format,
        schema, sqlContext)
    }
  }
}
