package org.apache.spark.sql.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.KafkaUtils

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
    extends StreamBaseRelation(options) {

  // Zookeeper quorum (hostname:port,hostname:port,..)
  val ZK_QUORUM = "zkquorum"

  // The group id for this consumer
  val GROUP_ID = "groupid"

  // Map of (topic_name -> numPartitions) to consume
  val TOPICS = "topics"

  val zkQuorum: String = options(ZK_QUORUM)
  val groupId: String = options(GROUP_ID)

  val topics: Map[String, Int] = options(TOPICS).split(",").map { s =>
    val a = s.split(":")
    (a(0), a(1).toInt)
  }.toMap


  @transient private val kafkaStream = KafkaUtils.
      createStream(context, zkQuorum, groupId, topics, storageLevel)

  stream = kafkaStream.map(_._2).flatMap(rowConverter.toRows)
}
