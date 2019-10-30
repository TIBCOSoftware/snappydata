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

import org.apache.spark.internal.Logging
import java.text.SimpleDateFormat
// scalastyle:off

private[ui] class SnappyStructuredStreamingPage(parent: SnappyStreamingTab)
    extends WebUIPage("") with Logging {

  val activeQueries = parent.listener.activeQueries

  def commonHeaderNodesSnappy: Seq[Node] = {
    <link rel="stylesheet" type="text/css"
          href={UIUtils.prependBaseUri("/static/snappydata/snappy-streaming.css")}/>
    <script src={UIUtils.prependBaseUri("/static/snappydata/d3.js")}></script>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script src={UIUtils.prependBaseUri("/static/snappydata/jquery.sparkline.min.js")}></script>
    <script src={UIUtils.prependBaseUri("/static/snappydata/snappy-commons.js")}></script>
    <script src={UIUtils.prependBaseUri("/static/snappydata/snappy-streaming.js")}></script>
  }

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText: String = SnappyStructuredStreamingPage.pageHeaderText

    var errorMessage = <div></div>

    if (activeQueries.isEmpty) {

      errorMessage = <div>No active streaming queries present..</div>
      UIUtils.headerSparkPage(pageHeaderText, errorMessage, parent, Some(500),
        useDataTables = true)

    } else {

      val mainContent = {
        createMainContent
      }

      val pageContent = commonHeaderNodesSnappy ++ mainContent

      UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500),
        useDataTables = true)
    }

  }

  private def createMainContent: Seq[Node] = {
    val navPanel = createNavigationPanel
    val detailsPanel = createQueryDetailsPanel

    <div class="main-container">
      {navPanel ++ detailsPanel}
    </div>
  }

  private def createNavigationPanel: Seq[Node] = {
    <div class="left-navigation-panel">
      <div style="width:100%;">
        <table id="streamingQueriesGrid" class="table table-bordered table-condensed"
               style="background-color: #bdbdbd; margin: 0px !important;">
          <thead>
            <tr>
              <th class="table-th-col-heading" style="font-size: medium;">
                <span data-toggle="tooltip" title=""
                      data-original-title="Streaming Queries">
                  { SnappyStructuredStreamingPage.leftNavPanelTitle }
                </span>
              </th>
            </tr>
          </thead>
        </table>
      </div>
    </div>
  }

  private def createQueryDetailsPanel: Seq[Node] = {
    <div class="right-details-panel">
      {createQueryDetailsEntry}
    </div>
  }

  private def createQueryDetailsEntry (): Seq[Node] = {

    <div id="querydetails">
      <div class="container-fluid details-section">
        <div id="selectedQueryName">
        </div>
      </div>
      <div class="container-fluid details-section">
        <div class="basic-details">
          <div>
            <div class="basic-details-title">
              { SnappyStructuredStreamingPage.streamingStats("startDateTime") }
            </div>
            <div id="startDateTime" class="basic-details-value">&nbsp;</div>
          </div>
          <div>
            <div class="basic-details-title">
              { SnappyStructuredStreamingPage.streamingStats("uptime") }
            </div>
            <div id="uptime" class="basic-details-value">&nbsp;</div>
          </div>
          <div>
            <div class="basic-details-title" style="width: 50%;">
              { SnappyStructuredStreamingPage.streamingStats("batchesProcessed") }
            </div>
            <div id="numBatchesProcessed" class="basic-details-value">&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="container-fluid details-section">
        <div class="stats-block" style="width: 14%;">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("status") }
            </div>
            <div id="status" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("totalInputRows") }
            </div>
            <div id="totalInputRows" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("totalInputRowsPerSec") }
            </div>
            <div id="totalInputRowsPerSec" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("totalProcessedRowsPerSec") }
            </div>
            <div id="totalProcessedRowsPerSec" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("totalProcessingTime") }
            </div>
            <div id="totalProcessingTime" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title">
              { SnappyStructuredStreamingPage.streamingStats("avgProcessingTime") }
            </div>
            <div id="avgProcessingTime" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="container-fluid" style="text-align: center;">
        <div id="googleChartsErrorMsg"
             style="text-align: center; color: #ff0f3f; display:none;">
          { SnappyStructuredStreamingPage.googleChartsErrorMsg }
        </div>
      </div>
      <div class="container-fluid details-section">
        <div id="inputTrendsContainer" class="graph-container">
        </div>
        <div id="processingTrendContainer" class="graph-container">
        </div>
        <div id="processingTimeContainer" class="graph-container">
        </div>
        <!-- <div id="stateOparatorContainer" class="graph-container">
        </div> -->
        <div id="delayTrendContainer" class="graph-container">
        </div>
      </div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;">
          { SnappyStructuredStreamingPage.sourcesTitle }
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div id="sourcesDetailsContainer" class="container-fluid details-section"
           style="height: 100px; border: 1px solid grey; padding: 10px; margin: 10px;">&nbsp;</div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;">
          { SnappyStructuredStreamingPage.sinkTitle }
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div id="sinkDetailsContainer" class="container-fluid details-section"
           style="height: 100px; border: 1px solid grey; padding: 10px; margin: 10px;">&nbsp;</div>
    </div>
  }
}

object SnappyStructuredStreamingPage {
  val pageHeaderText = "Structured Streaming Queries"

  val streamingStats = scala.collection.mutable.HashMap.empty[String, Any]
  streamingStats += ("startDateTime" -> "Start Date & Time")
  streamingStats += ("uptime" -> "Uptime")
  streamingStats += ("status" -> "Status")
  streamingStats += ("batchesProcessed" -> "Batches Processed")
  streamingStats += ("totalInputRows" -> "Total Input Records")
  streamingStats += ("totalInputRowsPerSec" -> "Input Records / Sec")
  streamingStats += ("totalProcessedRowsPerSec" -> "Processed Records / Sec")
  streamingStats += ("totalProcessingTime" -> "Total Processing Time")
  streamingStats += ("avgProcessingTime" -> "Avg. Batch Processing Time")

  val googleChartsErrorMsg = "Error while loading charts. Please check your internet connection."

  val leftNavPanelTitle = "Query Names"
  val sourcesTitle = "Sources"
  val sinkTitle = "Sink"

}
