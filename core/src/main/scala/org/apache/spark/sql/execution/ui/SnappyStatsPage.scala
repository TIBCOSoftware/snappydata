/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import io.snappydata.{StoreTableSizeProvider, UIAnalytics}

import org.apache.spark.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/** Page showing list of tables currently stored in the cluster */
private[ui] class SnappyStatsPage(parent: SnappyStatsTab)
    extends WebUIPage("") with Logging {
  val numFormatter = java.text.NumberFormat.getIntegerInstance
  def render(request: HttpServletRequest): Seq[Node] = {
    val uiDetails = StoreTableSizeProvider.getTableSizes
    val snappyRowTable = UIUtils.listingTable(
      rowHeader, rowTable, uiDetails.filter(row => !row.isColumnTable))
    val snappyColumnTable = UIUtils.listingTable(
      columnHeader, columnTable, uiDetails.filter(row => row.isColumnTable))

    val content =
      <span>
        <h4>Snappy Row Tables</h4>{snappyRowTable}<h4>Snappy Column Tables</h4>{snappyColumnTable}
      </span>

    UIUtils.headerSparkPage("Snappy Store", content, parent, Some(500))
  }

  private def rowHeader = Seq("TableName", "TotalSize" , "TotalRows")

  private def columnHeader = Seq("TableName", "Row Buffer Size", "Row Buffer Rows",
    "Column Store Size", "Column Store Rows", "TotalSize")

  private def rowTable(stats: UIAnalytics) = {
    <tr>
      <td>
        {stats.tableName}
      </td>
      <td sorttable_customkey={stats.rowBufferSize.toString}>
        {Utils.bytesToString(stats.rowBufferSize)}
      </td>
      <td sorttable_customkey={stats.rowBufferCount.toString}>
        {numFormatter.format(stats.rowBufferCount)}
      </td>
    </tr>
  }

  private def columnTable(stats: UIAnalytics) = {
    val totalSize = stats.rowBufferSize + stats.columnBufferSize
    <tr>
      <td>
        {stats.tableName}
      </td>
      <td sorttable_customkey={stats.rowBufferSize.toString}>
        {Utils.bytesToString(stats.rowBufferSize)}
      </td>
      <td sorttable_customkey={stats.rowBufferCount.toString}>
        {numFormatter.format(stats.rowBufferCount)}
      </td>
      <td sorttable_customkey={stats.columnBufferSize.toString}>
        {Utils.bytesToString(stats.columnBufferSize)}
      </td>
      <td sorttable_customkey={stats.columnBufferCount.toString}>
        {numFormatter.format(stats.columnBufferCount)}
      </td>
      <td sorttable_customkey={totalSize.toString}>
        {Utils.bytesToString(totalSize)}
      </td>
    </tr>
  }
}