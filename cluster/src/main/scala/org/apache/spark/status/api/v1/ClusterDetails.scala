/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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


import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClusterDetails {
  def getClusterDetailsInfo: Seq[ClusterSummary] = {
    val clusterBuff: ListBuffer[ClusterSummary] = ListBuffer.empty[ClusterSummary]
    // todo : build cluster object here

    val arrCpu = new mutable.Queue[Double]
    val arrHeap = new mutable.Queue[Double]
    val arrOffHeap = new mutable.Queue[Double]

    for(i <- 0 to 10){
      arrCpu.enqueue(Math.random()*100)
      arrHeap.enqueue(Math.random()*100)
      arrOffHeap.enqueue(Math.random()*100)
    }

    // clusterBuff += new ClusterSummary(Array.empty[Long], Array.empty[Long], Array.empty[Long])
    clusterBuff += new ClusterSummary(arrCpu.toList, arrHeap.toList, arrOffHeap.toList)
    clusterBuff.toList
  }
}
