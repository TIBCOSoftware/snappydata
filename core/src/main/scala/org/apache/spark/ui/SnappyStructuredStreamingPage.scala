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

    val mainContent = {
      val connErrorMsgNode = {
        <div id="AutoUpdateErrorMsgContainer">
          <div id="AutoUpdateErrorMsg">
          </div>
        </div>
      }
      connErrorMsgNode ++ createMainContent
    }

    val pageContent = commonHeaderNodesSnappy ++ mainContent

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500), useDataTables = true)

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
                      data-original-title={SnappyStructuredStreamingPage.tooltips("leftNavPanelTitle")}>
                  { SnappyStructuredStreamingPage.leftNavPanelTitle }
                </span>
              </th>
            </tr>
          </thead>
        </table>
      </div>
    </div>
  }

  private def createSourcesTable: Seq[Node] = {

    <div style="width:100%;">
      <table id="querySourcesGrid" class="table table-bordered table-condensed"
             style="background-color: #DDD; /*margin: 0px !important;*/">
        <thead>
          <tr>
            <th class="table-th-col-heading" style="font-size: medium;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyStructuredStreamingPage.tooltips("srcDescription")}>
                { SnappyStructuredStreamingPage.streamingStats("srcDescription") }
              </span>
            </th>
            <th class="table-th-col-heading" style="font-size: medium;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyStructuredStreamingPage.tooltips("srcInputRecords")}>
                { SnappyStructuredStreamingPage.streamingStats("srcInputRecords") }
              </span>
            </th>
            <th class="table-th-col-heading" style="font-size: medium;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyStructuredStreamingPage.tooltips("srcInputRate")}>
                { SnappyStructuredStreamingPage.streamingStats("srcInputRate") }
              </span>
            </th>
            <th class="table-th-col-heading" style="font-size: medium;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyStructuredStreamingPage.tooltips("srcProcessingRate")}>
                { SnappyStructuredStreamingPage.streamingStats("srcProcessingRate") }
              </span>
            </th>
          </tr>
        </thead>
      </table>
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
        <div id="selectedQueryTitle" data-toggle="tooltip" title=""
             data-original-title={SnappyStructuredStreamingPage.tooltips("queryName")}>
          { SnappyStructuredStreamingPage.streamingStats("queryName") }:
        </div>
        <div id="selectedQueryName"></div>
      </div>
      <div class="container-fluid details-section">
        <div class="basic-details">
          <div>
            <div class="basic-details-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("startDateTime")}>
              { SnappyStructuredStreamingPage.streamingStats("startDateTime") }
            </div>
            <div id="startDateTime" class="basic-details-value">&nbsp;</div>
          </div>
          <div>
            <div class="basic-details-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("uptime")}>
              { SnappyStructuredStreamingPage.streamingStats("uptime") }
            </div>
            <div id="uptime" class="basic-details-value">&nbsp;</div>
          </div>
          <div>
            <div class="basic-details-title" style="width: 50%;" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("batchesProcessed")}>
              { SnappyStructuredStreamingPage.streamingStats("batchesProcessed") }
            </div>
            <div id="numBatchesProcessed" class="basic-details-value">&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="container-fluid details-section">
        <div class="stats-block" style="width: 14%;">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("status")}>
              { SnappyStructuredStreamingPage.streamingStats("status") }
            </div>
            <div id="status" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("totalInputRows")}>
              { SnappyStructuredStreamingPage.streamingStats("totalInputRows") }
            </div>
            <div id="totalInputRows" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("currInputRowsPerSec")}>
              { SnappyStructuredStreamingPage.streamingStats("currInputRowsPerSec") }
            </div>
            <div id="currInputRowsPerSec" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("currProcessedRowsPerSec")}>
              { SnappyStructuredStreamingPage.streamingStats("currProcessedRowsPerSec") }
            </div>
            <div id="currProcessedRowsPerSec" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("totalProcessingTime")}>
              { SnappyStructuredStreamingPage.streamingStats("totalProcessingTime") }
            </div>
            <div id="totalProcessingTime" class="stats-block-value">&nbsp;</div>
          </div>
        </div>
        <div class="stats-block">
          <div>
            <div class="stats-block-title" data-toggle="tooltip" title=""
                 data-original-title={SnappyStructuredStreamingPage.tooltips("avgProcessingTime")}>
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
        <div id="stateOperatorContainer" class="graph-container">
        </div>
        <!-- <div id="delayTrendContainer" class="graph-container">
        </div> -->
      </div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;" data-toggle="tooltip" title=""
             data-original-title={SnappyStructuredStreamingPage.tooltips("sources")}>
          { SnappyStructuredStreamingPage.sourcesTitle }
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div id="sourcesDetailsContainer" class="container-fluid details-section"
           style="margin: 10px;">
        { createSourcesTable }
      </div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;" data-toggle="tooltip" title=""
             data-original-title={SnappyStructuredStreamingPage.tooltips("sink")}>
          { SnappyStructuredStreamingPage.sinkTitle }
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div id="sinkDetailsContainer" class="container-fluid details-section"
           style="/*height: 100px;*/ border: 1px solid grey; padding: 10px; margin: 10px;">&nbsp;</div>
    </div>
  }
}

object SnappyStructuredStreamingPage {
  val pageHeaderText = "Structured Streaming Queries"

  val streamingStats = scala.collection.mutable.HashMap.empty[String, Any]
  streamingStats += ("queryName" -> "Query Name")
  streamingStats += ("startDateTime" -> "Start Date & Time")
  streamingStats += ("uptime" -> "Uptime")
  streamingStats += ("status" -> "Status")
  streamingStats += ("batchesProcessed" -> "Batches Processed")
  streamingStats += ("totalInputRows" -> "Total Input Records")
  streamingStats += ("currInputRowsPerSec" -> "Current Input Rate")
  streamingStats += ("currProcessedRowsPerSec" -> "Current Processing Rate")
  streamingStats += ("totalProcessingTime" -> "Total Batch Processing Time")
  streamingStats += ("avgProcessingTime" -> "Avg. Batch Processing Time")
  streamingStats += ("srcDescription" -> "Description")
  streamingStats += ("srcInputRecords" -> "Input Records")
  streamingStats += ("srcInputRate" -> "Input Rate")
  streamingStats += ("srcProcessingRate" -> "Processing Rate")

  val tooltips = scala.collection.mutable.HashMap.empty[String, String]
  tooltips += ("leftNavPanelTitle" -> "Streaming Query Names")
  tooltips += ("queryName" -> "Streaming Query Name")
  tooltips += ("startDateTime" -> "Date & time when streaming query started its execution")
  tooltips += ("uptime" -> "Total time since streaming query started its execution")
  tooltips += ("batchesProcessed" -> "Number of batches processed since execution its started")
  tooltips += ("status" -> "Streaming query status (Active / Inactive)")
  tooltips += ("totalInputRows" -> "Total number of input records received since execution started")
  tooltips += ("currInputRowsPerSec" -> "Records / second received in current trigger interval")
  tooltips += ("currProcessedRowsPerSec" -> "Records processed / second in current trigger interval")
  tooltips += ("totalProcessingTime" -> "Total processing time of all batches received since execution is started")
  tooltips += ("avgProcessingTime" -> "Average processing time per batch")
  tooltips += ("sources" -> "Streaming queries sources")
  tooltips += ("srcDescription" -> "Description of streaming query source")
  tooltips += ("srcInputRecords" -> "Number of records received from source in current interval")
  tooltips += ("srcInputRate" -> "Number of records / second received from source in current interval")
  tooltips += ("srcProcessingRate" -> "Number of records processed / second in current interval")
  tooltips += ("sink" -> "Streaming queries sink")
  tooltips += ("snkDescription" -> "Description of streaming query sink")

  val googleChartsErrorMsg = "Error while loading charts. Please check your internet connection."

  val leftNavPanelTitle = "Query Names"
  val sourcesTitle = "Sources"
  val sinkTitle = "Sink"

}
