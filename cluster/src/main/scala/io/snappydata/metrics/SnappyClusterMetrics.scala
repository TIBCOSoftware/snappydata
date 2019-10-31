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
package io.snappydata.metrics

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.metrics.SnappyMetricsClass.{createGauge, createHistogram}
import org.apache.spark.groupon.metrics.{SparkHistogram, UserMetricsSystem}

import scala.collection.mutable

object SnappyClusterMetrics {

  def convertStatsToMetrics(key: String, clusterStats: Any) {
    
    val namespace = s"ClusterMetrics.$key"
    var clusterInfo = mutable.HashMap.empty[String, Any]

    clusterStats match {
      case i1: mutable.HashMap[String, Any] =>
        clusterInfo = i1.asInstanceOf[mutable.HashMap[String, Any]]
        clusterInfo.foreach({
          case (k, v) => createGauge(s"$namespace.$k", v.asInstanceOf[AnyVal])
        })
      case i2: Array[AnyRef] =>
        var clusterInfoArray = i2.asInstanceOf[Array[AnyRef]]
        lazy val tempHistogram: SparkHistogram = UserMetricsSystem.histogram(s"$namespace")
        for (i <- clusterInfoArray) {
          tempHistogram.update(i.asInstanceOf[Number].longValue())
        }
    }
  }

}