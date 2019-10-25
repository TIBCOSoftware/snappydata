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
package org.apache.spark.status.api.v1

import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.mutable.ListBuffer

import org.apache.spark.streaming.StreamingRepository

// scalastyle:off

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamsInfoResource {
  @GET
  def streamInfo(): Seq[StreamsSummary] = {
    val streamingRepo = StreamingRepository.getInstance

    val streamsBuff: ListBuffer[StreamsSummary] = ListBuffer.empty[StreamsSummary]
    streamsBuff += new StreamsSummary (streamingRepo.activeQueries,
      streamingRepo.inactiveQueries, streamingRepo.allQueries.values.toList)

    streamsBuff.toList
  }
}
