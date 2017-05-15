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
package org.apache.spark.sql.streaming.jdbc

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SQLContext, SnappyContext, SnappySession}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

trait SnappyStreamSink {
  def process(snappySession: SnappySession, sinkProps: Properties, df: Dataset[Row]): Unit
}

class SnappyStoreSinkProvider extends StreamSinkProvider with DataSourceRegister {
  @Override
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val props = new Properties()
    parameters.foreach { case (k, v) => props.setProperty(k, v) }
    val cc = Utils.classForName(parameters("sink")).newInstance()
    SnappyStoreSink(sqlContext, props,
      cc.asInstanceOf[SnappyStreamSink])
  }

  @Override
  def shortName(): String = "snappystore"
}

case class SnappyStoreSink(sqlContext: SQLContext,
    properties: Properties, sink: SnappyStreamSink) extends Sink {

  @Override
  def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    sink.process(sqlContext.asInstanceOf[SnappyContext].snappySession, properties, data)
  }
}
