/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.streaming

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone, UUID}

import scala.collection.mutable.HashMap

import org.apache.commons.collections.buffer.CircularFifoBuffer

import org.apache.spark.sql.streaming.{SinkProgress, SourceProgress, StreamingQueryListener}
import org.apache.spark.ui.UIUtils

// scalastyle:off

class StreamingRepository {

  val allQueries = HashMap.empty[UUID, StreamingQueryStatistics]
  val activeQueries = HashMap.empty[UUID, String]
  val inactiveQueries = HashMap.empty[UUID, String]

  // def getAllQueries: mutable.HashMap[UUID, StreamingQueryStatistics] = allQueries
  // def getActiveQueries: mutable.HashMap[UUID, String] = activeQueries
  // def getInactiveQueries: mutable.HashMap[UUID, String] = inactiveQueries

}

object StreamingRepository {

  val MAX_SAMPLE_SIZE = 180

  private val _instance: StreamingRepository = new StreamingRepository

  def getInstance: StreamingRepository = _instance
}



class StreamingQueryStatistics (qId: UUID, qName: String, runId: UUID, startTime: Long) {

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  val queryUUID: UUID = qId
  val queryName: String = qName
  val queryStartTime: Long = startTime
  var queryUptime: Long = 0L
  var queryUptimeText: String = ""

  var runUUID: UUID = runId

  var isActive: Boolean = true

  var currentBatchId: Long = 0L

  var sources = Array.empty[SourceProgress]
  var sink: SinkProgress = null

  var trendInterval: Int = 0

  val timeLine = new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)
  val numInputRowsTrend = new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)
  val inputRowsPerSecondTrend = new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)
  val processedRowsPerSecondTrend =
    new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)
  val processingTimeTrend = new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)
  val batchIds = new CircularFifoBuffer(StreamingRepository.MAX_SAMPLE_SIZE)

  def updateQueryStatistics(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress

    val currDateTime: Date = timestampFormat.parse(progress.timestamp)
    this.queryUptime = currDateTime.getTime - this.queryStartTime
    this.queryUptimeText = UIUtils.formatDurationVerbose(this.queryUptime)

    this.timeLine.add(currDateTime.getTime)
    this.numInputRowsTrend.add(
      if (progress.numInputRows.isNaN) 0 else progress.numInputRows)
    this.inputRowsPerSecondTrend.add(
      if (progress.inputRowsPerSecond.isNaN) 0.0 else progress.inputRowsPerSecond)
    this.processedRowsPerSecondTrend.add(
      if (progress.processedRowsPerSecond.isNaN) 0.0 else progress.processedRowsPerSecond)
    this.processingTimeTrend.add(progress.durationMs.get("triggerExecution"))
    this.batchIds.add(progress.batchId)
    this.currentBatchId = progress.batchId

    this.sources = progress.sources
    this.sink = progress.sink

  }

  def setStatus (status: Boolean) = {
    this.isActive = status
  }

}

class StreamingSourceStatistics (queriUUID: UUID, description: String) {
  var numInputRows: Int = 0
  var processedRowsPerSecond: Int = 0
}

class StreamingSinkStatistics (queriUUID: UUID, description: String)