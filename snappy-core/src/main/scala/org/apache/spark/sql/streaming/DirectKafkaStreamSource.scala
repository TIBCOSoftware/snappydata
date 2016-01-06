package org.apache.spark.sql.streaming

import kafka.serializer.StringDecoder

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
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

  if (DirectKafkaStreamRelation.getRowStream() == null) {
    rowStream = {
      KafkaUtils
          .createDirectStream[String, String, StringDecoder, StringDecoder](
        context, kafkaParams, topicsSet).map(_._2).flatMap(rowConverter.toRows)
    }
    DirectKafkaStreamRelation.setRowStream(rowStream)
    // TODO Yogesh, this is required from snappy-shell, need to get rid of this
    rowStream.foreachRDD { rdd => rdd }
  } else {
    rowStream = DirectKafkaStreamRelation.getRowStream()
  }
}

object DirectKafkaStreamRelation extends Logging {
  private var rowStream: DStream[InternalRow] = null

  private val LOCK = new Object()

  private def setRowStream(stream: DStream[InternalRow]): Unit = {
    LOCK.synchronized {
      rowStream = stream
    }
  }

  private def getRowStream(): DStream[InternalRow] = {
    LOCK.synchronized {
      rowStream
    }
  }
}