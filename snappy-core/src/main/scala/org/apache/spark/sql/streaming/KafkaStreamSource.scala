package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.util.Utils

/**
 * Created by ymahajan on 25/09/15.
 */
class KafkaStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String],
                              schema: StructType): BaseRelation = {
    new KafkaStreamRelation(sqlContext, options, schema)
  }
}

case class KafkaStreamRelation(@transient val sqlContext: SQLContext,
                               options: Map[String, String],
                               override val schema: StructType)
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  val ZK_QUORUM = "zkquorum"
  //Zookeeper quorum (hostname:port,hostname:port,..)
  val GROUP_ID = "groupid"
  //The group id for this consumer
  val TOPICS = "topics" //Map of (topic_name -> numPartitions) to consume

  val zkQuorum: String = options(ZK_QUORUM)
  val groupId: String = options(GROUP_ID)

  val topics: Map[String, Int] = options(TOPICS).split(",").map { s =>
    val a = s.split(":")
    (a(0), a(1).toInt)
  }.toMap


  val storageLevel = options.get("storageLevel")
    .map(StorageLevel.fromString)
    .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  private val streamToRow = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("streamToRow"))
      clz.newInstance().asInstanceOf[StreamToRowConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  @transient val context = StreamingCtxtHolder.streamingContext

  @transient private val kafkaStream = KafkaUtils.
    createStream(context, zkQuorum, groupId, topics, storageLevel)

  @transient val stream: DStream[InternalRow] =
    kafkaStream.map(_._2).map(streamToRow.toRow)
}
