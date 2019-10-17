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

  val numRecordsToHold = 100

  val activeQueries = new HashMap[UUID, String]
  val allQueriesBasicDetails = new HashMap[UUID, HashMap[String, Any]]
  val activeQueryProgress = new HashMap[UUID, StreamingQueryProgress]
  val stoppedQueries = new HashMap[UUID, String]

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println("====== ====== ====== Query started: event.id :: " + event.id + " | event.name :: " + event.name)
    activeQueries.put(event.id, event.name)
    val qMap = mutable.HashMap[String, Any]( "name"->event.name, "startTime" -> System.currentTimeMillis(),
      "isActive"-> true, "uptime" -> 0, "isRestarted" -> false,
      "attemptCount" -> 0)
    allQueriesBasicDetails.put(event.id, qMap)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val pr = event.progress
    activeQueryProgress.put(pr.id, pr)
    println("====== ====== ====== Query made progress: event.progress :: " + pr.id + " | event.name :: " + pr.name)
    println(" Query id:: " + pr.id + " \n Run id :: " + pr.runId + "\n Batch Id: " + pr.batchId)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println("====== ====== ====== Query terminated: event.id :: " + event.id)
    activeQueries.remove(event.id)
  }

  // scalastyle:on
}
