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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

final class RabbitMQStreamSource extends StreamPlanProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.RABBITMQ_STREAM_SOURCE

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new RabbitMQStreamRelation(sqlContext, options, schema)
  }
}

final class RabbitMQStreamRelation(
    @transient override val sqlContext: SQLContext,
    opts: Map[String, String],
    override val schema: StructType)
    extends StreamBaseRelation(opts) {

  val T = options.getOrElse("T", "java.lang.String")
  val D = options.getOrElse("D", "org.apache.spark.sql.streaming.RabbitMQStringDecoder")

  val t : ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(T))
  val d : ClassTag[Any] = ClassTag(Utils.getContextOrSparkClassLoader.loadClass(D))

  override protected def createRowStream(): DStream[InternalRow] = {
    RabbitMQUtils.createStream[Any, Any](context, options)(t, d)
        .mapPartitions { iter =>
      val encoder = RowEncoder(schema)
      // need to call copy() below since there are builders at higher layers
      // (e.g. normal Seq.map) that store the rows and encoder reuses buffer
      iter.flatMap(rowConverter.toRows(_).iterator.map(
        encoder.toRow(_).copy()))
    }
  }
}
