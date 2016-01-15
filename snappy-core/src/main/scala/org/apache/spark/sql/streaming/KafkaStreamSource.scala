/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.KafkaUtils

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

  val tableName = options("tableName")

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

  StreamBaseRelation.LOCK.synchronized {
    if (StreamBaseRelation.getRowStream(tableName) == null) {
      rowStream = {
        KafkaUtils.createStream(context, zkQuorum, groupId, topics, storageLevel)
            .map(_._2).flatMap(rowConverter.toRows)
      }
      StreamBaseRelation.setRowStream(tableName, rowStream)
    } else {
      rowStream = StreamBaseRelation.getRowStream(tableName)
    }
  }
}