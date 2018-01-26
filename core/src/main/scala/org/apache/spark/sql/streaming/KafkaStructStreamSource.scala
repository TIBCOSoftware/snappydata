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

import java.util.UUID
import java.{util => ju}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.kafka010.{EarliestOffsetRangeLimit, KafkaOffsetReader, LatestOffsetRangeLimit, _}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

trait StructStreamPlanProvider extends SchemaRelationProvider


class KafkaStructStreamSource extends StructStreamPlanProvider {

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String],
                              schema: StructType): BaseRelation = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${UUID.randomUUID}"
    val caseInsensitiveParams = options.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      options
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> options(k) }
        .toMap

    val startingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY,
      EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, KafkaSourceProvider.ENDING_OFFSETS_OPTION_KEY,
      LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy(caseInsensitiveParams),
      kafkaParamsForDriver(specifiedKafkaParams),
      options,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    new KafkaStructStreamRelation(
      sqlContext,
      schema,
      kafkaOffsetReader,
      ju.Collections.emptyMap(),
      options,
      false,
      startingRelationOffsets,
      endingRelationOffsets)
  }
  def kafkaParamsForDriver(specifiedKafkaParams: Map[String, String]) =
    ConfigUpdater("source", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSourceProvider.deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceProvider.deserClassName)

      // Set to "earliest" to avoid exceptions. However, KafkaSource will fetch the initial
      // offsets by itself instead of counting on KafkaConsumer.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      // So that consumers in the driver does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // So that the driver does not pull too much data
      .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, new java.lang.Integer(1))

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  def kafkaParamsForExecutors(
                               specifiedKafkaParams: Map[String, String], uniqueGroupId: String) =
    ConfigUpdater("executor", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSourceProvider.deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceProvider.deserClassName)

      // Make sure executors do only what the driver tells them.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

      // So that consumers in executors do not mess with any existing group id
      .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-executor")

      // So that consumers in executors does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()
  private def strategy(caseInsensitiveParams: Map[String, String]) =
    caseInsensitiveParams.find(x =>
      KafkaSourceProvider.STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        AssignStrategy(JsonUtils.partitions(value))
      case ("subscribe", value) =>
        SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty))
      case ("subscribepattern", value) =>
        SubscribePatternStrategy(value.trim())
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }
  import scala.collection.JavaConverters._

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
      }
      this
    }

    def build(): ju.Map[String, Object] = map
  }
}


final class KafkaStructStreamRelation(
                                       @transient override val sqlContext: SQLContext,
                                       schema: StructType,
                                       kafkaReader: KafkaOffsetReader,
                                       executorKafkaParams: ju.Map[String, Object],
                                       sourceOptions: Map[String, String],
                                       failOnDataLoss: Boolean,
                                       startingOffsets: KafkaOffsetRangeLimit,
                                       endingOffsets: KafkaOffsetRangeLimit)
  extends KafkaRelation(sqlContext, kafkaReader, executorKafkaParams,
    sourceOptions,
    failOnDataLoss,
    startingOffsets,
    endingOffsets) with TableScan with Serializable {
  override def buildScan(): RDD[Row] = {
    throw new UnsupportedOperationException()
    val dataSource =
      DataSource(
        sqlContext.sparkSession,
        userSpecifiedSchema = None,
        className = "kafka",
        options = sourceOptions)
    Dataset.ofRows(sqlContext.sparkSession, StreamingRelation(dataSource)).rdd
  }
}