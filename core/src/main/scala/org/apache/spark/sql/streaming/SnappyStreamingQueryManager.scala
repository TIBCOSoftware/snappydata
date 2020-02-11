/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import io.snappydata.Property.HashAggregateSize
import org.apache.spark.sql.execution.streaming.{Sink, StreamingQueryWrapper}
import org.apache.spark.sql.{AnalysisException, DataFrame, SnappySession}
import org.apache.spark.util.{Clock, SystemClock}

class SnappyStreamingQueryManager(snappySession: SnappySession)
    extends StreamingQueryManager(snappySession) {
  private val snappySinkQueriesLock = new Object()

  override private[sql] def startQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      sink: Sink,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean = false,
      recoverFromCheckpointLocation: Boolean = true,
      trigger: Trigger = ProcessingTime(0),
      triggerClock: Clock = new SystemClock()): StreamingQuery = {

    def isQueryWithSnappySinkRunning = {
      super.active.map(_.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink)
          .exists(_.isInstanceOf[SnappyStoreSink])
    }

    def startQuery = {
      // Disabling `SnappyAggregateStrategy` for streaming queries as it clashes with
      // `StatefulAggregationStrategy` which is applied by spark for streaming queries. This
      // implies that Snappydata aggregation optimisation will be turned off for any usage of
      // this session including non-streaming queries.

      HashAggregateSize.set(snappySession.sessionState.conf, "-1")
      super.startQuery(userSpecifiedName, userSpecifiedCheckpointLocation, df, sink, outputMode,
        useTempCheckpointLocation, recoverFromCheckpointLocation, trigger, triggerClock)
    }

    if (sink.isInstanceOf[SnappyStoreSink]) {
      snappySinkQueriesLock.synchronized {
        if (isQueryWithSnappySinkRunning) {
          val message = "A streaming query with snappy sink is already running with current" +
              " session. Please start query with new SnappySession."
          throw new AnalysisException(message)
        } else {
          startQuery
        }
      }
    } else {
      startQuery
    }
  }
}
