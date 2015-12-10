package org.apache.spark.sql.streaming

import kafka.serializer.StringDecoder
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
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  @transient val context = StreamingCtxtHolder.streamingContext

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

  //Direct Kafka
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
  //TODO Yogesh, need to provide typed decoders to createDirectStream


  @transient val stream: DStream[InternalRow] =
    kafkaStream.map(_._2).map(streamToRow.toRow)

}
