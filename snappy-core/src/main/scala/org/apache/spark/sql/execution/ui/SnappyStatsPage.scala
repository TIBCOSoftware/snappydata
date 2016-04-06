/*
 * Changes for SnappyData additions and modifications.
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

import org.apache.spark.Logging
import org.apache.spark.sql.sources.SnappyAnalyticsService
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/** Page showing list of tables currently stored in the cluster */
private[ui] class SnappyStatsPage(parent: SnappyStatsTab)
		extends WebUIPage("") with Logging {
	def render(request: HttpServletRequest): Seq[Node] = {
		val snappyStatsTable = UIUtils.listingTable(
			tableHeader, tableRow, SnappyAnalyticsService.getTableStats.toSeq, fixedWidth = true)
		val content =
			<span>
				<h4>Runtime Information</h4>{snappyStatsTable}
			</span>

		UIUtils.headerSparkPage("Snappy", content, parent, Some(500))
	}

	private def tableHeader = Seq("TableName", "TotalSize")

	private def tableRow(kv: (String, Long)) = {
		<tr>
			<td>
				{kv._1}
			</td>
			<td sorttable_customkey={kv._2.toString}>
				{Utils.bytesToString(kv._2)}
			</td>
		</tr>
	}
}
