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

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

class SnappyStreamingQueryListener(sparkContext: SparkContext) extends StreamingQueryListener {
  // scalastyle:off

  val streamingRepo = StreamingRepository.getInstance

  val activeQueries = HashMap.empty[UUID, String]
  val allQueriesBasicDetails = HashMap.empty[UUID, HashMap[String, Any]]
  val activeQueryProgress = HashMap.empty[UUID, StreamingQueryProgress]
  val stoppedQueries = HashMap.empty[UUID, String]

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println("====== ====== ====== Query started: event.id :: " + event.id + " | event.name :: " + event.name)

    val queryName = {
      if (event.name == null || event.name.isEmpty)
        event.id.toString
      else
        event.name
    }

    activeQueries.put(event.id, event.name)
    val qMap = mutable.HashMap[String, Any]( "name"->queryName, "startTime" -> System.currentTimeMillis(),
      "isActive"-> true, "uptime" -> 0, "isRestarted" -> false,
      "attemptCount" -> 0)
    allQueriesBasicDetails.put(event.id, qMap)

    streamingRepo.activeQueries.put(event.id, queryName)
    streamingRepo.allQueries.put(event.id,
      new StreamingQueryStatistics(event.id, queryName, event.runId, System.currentTimeMillis()))

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val pr = event.progress
    activeQueryProgress.put(pr.id, pr)
    println("====== ====== ====== Query made progress: event.progress :: " + pr.id + " | event.name :: " + pr.name)
    println(" Query id:: " + pr.id + "\n Time: "+ pr.timestamp + " \n Run id :: " + pr.runId + "\n Batch Id: " + pr.batchId)

    if (streamingRepo.allQueries.contains(pr.id)) {
      val sqs = streamingRepo.allQueries.get(pr.id).get
      sqs.updateQueryStatistics(event)
    } else {
      val queryName = {
        if (pr.name == null || pr.name.isEmpty)
          pr.id.toString
        else
          pr.name
      }
      val sqs = new StreamingQueryStatistics(pr.id, queryName, pr.runId, System.currentTimeMillis())
      sqs.updateQueryStatistics(event)
      streamingRepo.allQueries.put(pr.id, sqs)
    }

  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println("====== ====== ====== Query terminated: event.id :: " + event.id)
    activeQueries.remove(event.id)

    val q = streamingRepo.activeQueries.remove(event.id).get
    streamingRepo.inactiveQueries.put(event.id, q)

    streamingRepo.allQueries.get(event.id).get.setStatus(false)
  }

  // scalastyle:on
}
