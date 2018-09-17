/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.streaming

import scala.reflect.ClassTag

import kafka.serializer.Decoder

import org.apache.kafka.common.TopicPartition
import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.util.Utils
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.Random
import scala.util.control.NonFatal

class DirectKafkaStreamSource extends StreamPlanProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.KAFKA_STREAM_SOURCE

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new DirectKafkaStreamRelation(sqlContext, options, schema)
  }
}

final class DirectKafkaStreamRelation(
    @transient override val sqlContext: SQLContext,
    opts: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(opts)
    with Logging with Serializable {

  val topics = options("subscribe").split(",").toList
  val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
    t.split(";").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap
  }.getOrElse(Map())

  val preferredHosts = LocationStrategies.PreferConsistent
  val startingOffsets = JsonUtils.partitionOffsets(options("startingOffsets"))

//  val K = options.getOrElse("K", "java.lang.String")
//  val V = options.getOrElse("V", "java.lang.String")
//  val KD = options.getOrElse("KD", "kafka.serializer.StringDecoder")
//  val VD = options.getOrElse("VD", "kafka.serializer.StringDecoder")

  override protected def createRowStream(): DStream[InternalRow] = {
//    val ck: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(K))
//    val cv: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(V))
//    val ckd: ClassTag[Decoder[Any]] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(KD))
//    val cvd: ClassTag[Decoder[Any]] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(VD))
//    KafkaUtils.createDirectStream[Any, Any, Decoder[Any], Decoder[Any]](context,
//      kafkaParams, topicsSet)(ck, cv, ckd, cvd).mapPartitions { iter =>
//      val encoder = RowEncoder(schema)
//      // need to call copy() below since there are builders at higher layers
//      // (e.g. normal Seq.map) that store the rows and encoder reuses buffer
//      iter.flatMap(p => rowConverter.toRows(p._2).iterator.map(
//        encoder.toRow(_).copy()))
//    }
val consumerStrategies = ConsumerStrategies
  .Subscribe[String, String](topics, kafkaParams, startingOffsets)

    val stream = KafkaUtils.createDirectStream[String, String](context,
      preferredHosts, consumerStrategies)
      .mapPartitions { iter =>
        val encoder = RowEncoder(schema)
        iter.flatMap(p => {
          rowConverter.toRows(p.value()).iterator.map(
            encoder.toRow(_).copy())
        })
      }
    stream
  }
}

/**
  * Copied from org.apache.spark.sql.kafka010.JsonUtils
  */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read TopicPartitions from json string
    */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap { case (topic, parts) =>
        parts.map { part =>
          new TopicPartition(topic, part)
        }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
    * Write TopicPartitions as json string
    */
  def partitions(partitions: Iterable[TopicPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition :: parts)
    }
    Serialization.write(result)
  }

  /**
    * Read per-TopicPartition offsets from json string
    */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          new TopicPartition(topic, part) -> offset
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}