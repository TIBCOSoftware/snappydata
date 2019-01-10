/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.ui

import org.apache.spark.Logging

/** Web UI showing storage status of all Snappy Tables */
private[ui] class SnappyStatsTab(sparkUI: SparkUI)
    extends SparkUITab(sparkUI, "Snappy Store") with Logging {
  val parent = sparkUI
  attachPage(new SnappyStatsPage(this))
  parent.attachTab(this)
}
