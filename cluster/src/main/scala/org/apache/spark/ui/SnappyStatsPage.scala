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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/** Page showing list of tables currently stored in the cluster */
private[ui] class SnappyStatsPage(parent: SnappyStatsTab)
    extends WebUIPage("") with Logging {
  val numFormatter = java.text.NumberFormat.getIntegerInstance

  def render(request: HttpServletRequest): Seq[Node] = {
    val uiDisplayInfo = SnappyTableStatsProviderService.getService
        .getAggregatedStatsOnDemand

    val uiTableInfo = uiDisplayInfo._1
    val nodes = if (uiTableInfo.nonEmpty) {
      <span>
        <h4>Snappy Tables</h4>{UIUtils.listingTable(header, rowTable, uiTableInfo.values)}
      </span>
    } else Nil

    UIUtils.headerSparkPage("Snappy Store", nodes, parent, Some(500))

  }

  private def header = Seq("Table Name", "Table Type", "Memory Used", "Total Rows")


  private def rowTable(stats: SnappyRegionStats) = {
    val columnTable = if (stats.isColumnTable) " COLUMN " else " ROW "
    <tr>
      <td sorttable_customkey={stats.getTableName}>
        {stats.getTableName}
      </td>
      <td sorttable_customkey={columnTable}>
        {columnTable}
      </td>
      <td sorttable_customkey={stats.getSizeInMemory.toString}>
        {Utils.bytesToString(stats.getSizeInMemory)}
      </td>
      <td sorttable_customkey={stats.getRowCount.toString}>
        {numFormatter.format(stats.getRowCount)}
      </td>
    </tr>
  }
}
