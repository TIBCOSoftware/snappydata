/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.streaming.DirectKafkaStreamRelation.{STARTING_OFFSETS_PROP, partitionOffsetMethod}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.util.Utils

class DirectKafkaStreamSource extends StreamPlanProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.KAFKA_STREAM_SOURCE

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new DirectKafkaStreamRelation(sqlContext, options, schema)
  }
}

object DirectKafkaStreamRelation{
  private val partitionOffsetMethod = {
    val clazz = Utils.classForName("org.apache.spark.sql.kafka010.JsonUtils")
    clazz.getMethod("partitionOffsets", classOf[String])
  }

  private val STARTING_OFFSETS_PROP = "startingOffsets"
}

final class DirectKafkaStreamRelation(
    @transient override val sqlContext: SQLContext,
    opts: Map[String, String],
    override val schema: StructType)
  extends StreamBaseRelation(opts)
    with Logging with Serializable {

  private val topics = options("subscribe").split(",").toSet
  private val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
    t.split(";").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap
  }.getOrElse(Map())

  private val preferredHosts = LocationStrategies.PreferConsistent
  private val startingOffsets = getStartingOffsets

  private def getStartingOffsets = options.get(STARTING_OFFSETS_PROP) match {
    case Some(offsets) => partitionOffsetMethod.invoke(null, offsets)
        .asInstanceOf[Map[TopicPartition, Long]]
    case None => Map.empty[TopicPartition, Long]
  }

  override protected def createRowStream(): DStream[InternalRow] = {
    val consumerStrategies = ConsumerStrategies
        .Subscribe[Any, Any](topics, kafkaParams, startingOffsets)

    val stream = KafkaUtils.createDirectStream[Any, Any](context,
      preferredHosts, consumerStrategies).mapPartitions{ iter =>
        val encoder = RowEncoder(schema)
        iter.flatMap(p => {
          rowConverter.toRows(p.value()).iterator.map(
            encoder.toRow(_).copy())
        })
      }
    stream
  }
}
