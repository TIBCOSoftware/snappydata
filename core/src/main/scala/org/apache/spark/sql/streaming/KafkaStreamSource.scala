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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.util.Utils

class KafkaStreamSource extends StreamPlanProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.KAFKA_STREAM_SOURCE

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new KafkaStreamRelation(sqlContext, options, schema)
  }
}

final class KafkaStreamRelation(
    @transient override val sqlContext: SQLContext,
    opts: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(opts) {

  val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
    t.split(";").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap
  }.getOrElse(Map())

  val topics: Map[String, Int] = options("topics").split(",").map { s =>
    val a = s.split(":")
    (a(0), a(1).toInt)
  }.toMap

  val K = options.getOrElse("K", "java.lang.String")
  val V = options.getOrElse("V", "java.lang.String")
  val KD = options.getOrElse("KD", "kafka.serializer.StringDecoder")
  val VD = options.getOrElse("VD", "kafka.serializer.StringDecoder")

  override protected def createRowStream(): DStream[InternalRow] = {
    val ck: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(K))
    val cv: ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(V))
    val ckd: ClassTag[Decoder[Any]] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(KD))
    val cvd: ClassTag[Decoder[Any]] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(VD))
    KafkaUtils.createStream[Any, Any, Decoder[Any], Decoder[Any]](context,
      kafkaParams, topics, storageLevel)(ck, cv, ckd, cvd).mapPartitions { iter =>
      val encoder = RowEncoder(schema)
      // need to call copy() below since there are builders at higher layers
      // (e.g. normal Seq.map) that store the rows and encoder reuses buffer
      iter.flatMap(p => rowConverter.toRows(p._2).iterator.map(
        encoder.toRow(_).copy()))
    }
  }
}
