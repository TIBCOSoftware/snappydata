package org.apache.spark.sql.streaming

import kafka.serializer.StringDecoder

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by ymahajan on 25/09/15.
  */
class DirectKafkaStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new DirectKafkaStreamRelation(sqlContext, options, schema)
  }
}

case class DirectKafkaStreamRelation(@transient val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(options) with Logging with StreamPlan with Serializable {

  val topicsSet = options("topics").split(",").toSet
  val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
    t.split(", ").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap
  }.getOrElse(Map())

  // Currently works with Strings only
  @transient private val kafkaStream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
    context, kafkaParams, topicsSet)
  // TODO Yogesh, need to provide typed decoders to createDirectStream

  stream = kafkaStream.map(_._2).flatMap(rowConverter.toRows)
}
