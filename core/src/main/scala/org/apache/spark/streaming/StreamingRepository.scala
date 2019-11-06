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

  private val _instance: StreamingRepository = new StreamingRepository

  def getInstance: StreamingRepository = _instance
}



class StreamingQueryStatistics (qId: UUID, qName: String, runId: UUID, startTime: Long) {

  private val MAX_SAMPLE_SIZE = 100

  private val simpleDateFormat = new SimpleDateFormat("dd-MMM-YYYY hh:mm:ss")
  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  val queryUUID: UUID = qId
  val queryName: String = qName
  val queryStartTime: Long = startTime
  val queryStartTimeText: String = simpleDateFormat.format(startTime)
  var queryUptime: Long = 0L
  var queryUptimeText: String = ""

  var runUUID: UUID = runId

  var isActive: Boolean = true

  var currentBatchId: Long = -1L

  var sources = Array.empty[SourceProgress]
  var sink: SinkProgress = null

  var trendInterval: Int = 0

  var totalInputRows: Long = 0L
  var avgInputRowsPerSec: Double = 0.0
  var avgProcessedRowsPerSec: Double = 0.0
  var totalProcessingTime: Long = 0L
  var avgProcessingTime: Double = 0.0
  var numBatchesProcessed: Long = 0L

  val timeLine = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val numInputRowsTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val inputRowsPerSecondTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val processedRowsPerSecondTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val processingTimeTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val batchIds = new CircularFifoBuffer(MAX_SAMPLE_SIZE)

  var currStateOpNumRowsTotal = 0L
  var currStateOpNumRowsUpdated = 0L
  val stateOpNumRowsTotalTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)
  val stateOpNumRowsUpdatedTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE)

  def updateQueryStatistics(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress

    if(this.currentBatchId < progress.batchId){
      this.numBatchesProcessed = this.numBatchesProcessed + 1
    }

    this.currentBatchId = progress.batchId
    this.batchIds.add(progress.batchId)

    val currDateTime: Date = timestampFormat.parse(progress.timestamp)
    this.queryUptime = currDateTime.getTime - this.queryStartTime
    this.queryUptimeText = UIUtils.formatDurationVerbose(this.queryUptime)

    this.timeLine.add(currDateTime.getTime)

    val tmpNumInpRows = { if (progress.numInputRows.isNaN) 0 else progress.numInputRows }
    this.numInputRowsTrend.add(tmpNumInpRows)
    this.totalInputRows = this.totalInputRows + tmpNumInpRows

    val tmpInputRowsPerSec = { if (progress.inputRowsPerSecond.isNaN) 0.0 else progress.inputRowsPerSecond }
    this.inputRowsPerSecondTrend.add(tmpInputRowsPerSec)
    this.avgInputRowsPerSec = calcAvgOfGivenTrend(this.inputRowsPerSecondTrend)

    val tmpProcessedRowsPerSec = { if (progress.processedRowsPerSecond.isNaN) 0.0 else progress.processedRowsPerSecond }
    this.processedRowsPerSecondTrend.add(tmpProcessedRowsPerSec)
    this.avgProcessedRowsPerSec = calcAvgOfGivenTrend(this.processedRowsPerSecondTrend)

    val tmpProcessingTime = progress.durationMs.get("triggerExecution")
    this.processingTimeTrend.add(tmpProcessingTime)
    this.totalProcessingTime = this.totalProcessingTime + tmpProcessingTime
    this.avgProcessingTime = this.totalProcessingTime / this.numBatchesProcessed

    this.sources = progress.sources
    this.sink = progress.sink

    val stateOperators = progress.stateOperators
    if (stateOperators.size > 0) {

      var sumAllSTNumRowsTotal = 0L
      var sumAllSTNumRowsUpdated = 0L

      stateOperators.foreach(so => {
        sumAllSTNumRowsTotal = sumAllSTNumRowsTotal + so.numRowsTotal
        sumAllSTNumRowsUpdated = sumAllSTNumRowsUpdated + so.numRowsUpdated
      })

      if (currStateOpNumRowsTotal < sumAllSTNumRowsTotal) {
        this.currStateOpNumRowsTotal = sumAllSTNumRowsTotal
      }
      this.stateOpNumRowsTotalTrend.add(sumAllSTNumRowsTotal)

      if (currStateOpNumRowsUpdated < sumAllSTNumRowsUpdated) {
        this.currStateOpNumRowsUpdated = sumAllSTNumRowsUpdated
      }
      this.stateOpNumRowsUpdatedTrend.add(sumAllSTNumRowsUpdated)

    }
  }

  def calcAvgOfGivenTrend (trend: CircularFifoBuffer) : Double = {
    val arrValues = trend.toArray()
    var sumOfElements = 0.0

    arrValues.foreach(value => {
      sumOfElements = sumOfElements + value.asInstanceOf[Double]
    })

    val avgValue = sumOfElements / arrValues.size

    avgValue
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