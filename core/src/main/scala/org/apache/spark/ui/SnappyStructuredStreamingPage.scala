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
import org.apache.spark.ui.UIUtils.prependBaseUri
import java.text.SimpleDateFormat
// scalastyle:off

private[ui] class SnappyStructuredStreamingPage(parent: SnappyStreamingTab)
    extends WebUIPage("") with Logging {

  val simpleDateFormat = new SimpleDateFormat("dd-MMM-YYYY hh:mm:ss")

  val activeQueries = parent.listener.activeQueries
  val activeQueryProgress = parent.listener.activeQueryProgress
  val allQueriesBasicDetails = parent.listener.allQueriesBasicDetails
  var selectedQueryId: String = ""

  override def render(request: HttpServletRequest): Seq[Node] = {

    selectedQueryId = {
      if (request.getParameter("query") != null
          && request.getParameter("query").nonEmpty)
        {
          request.getParameter("query")
        } else {
        activeQueries.values.head
      }
    }

    val pageHeaderText: String = SnappyStructuredStreamingPage.pageHeaderText
    val cssStylesheets = <link rel="stylesheet" type="text/css"
                               href={prependBaseUri("/static/snappydata/snappy-streaming.css")}/>
    val jsScripts = <script src={
                            UIUtils.prependBaseUri("/static/snappydata/snappy-streaming.js")
                            }></script>

    val queryCountsSummaryNode = {

      val queryCountsMap = new collection.immutable.HashMap[String, Int]
      queryCountsMap -> ("totalQueries", activeQueries.size)
      queryCountsMap -> ("totalActiveQueries", 0)
      queryCountsMap -> ("totalStoppedQueries", 0)

      createQueryCountsSummaryNode(queryCountsMap)
    }

    val mainContent = {
      createMainContent
    }

    val pageContent = cssStylesheets ++ jsScripts ++
                      queryCountsSummaryNode ++ mainContent

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500),
      useDataTables = true)

  }

  private def createQueryCountsSummaryNode(queryCountsMap: Map[String, Int]): Seq[Node] = {
    <div class="row-fluid">
      <div class="span12">
        <div style="position: absolute; width: 410px; right: 25px;">
          <div style="width: 100%; max-height: 60px;
           background-color: #A0DFFF; border: 2px solid #9EBFE4; border-radius: 5px;
           position: relative; margin: -5px auto; overflow: auto;">
            <div style="width: 100%; float: right; font-weight: bold; text-align: center;
                        line-height: 30px;">
              <span style="padding-left: 5px;">Total Queries:</span>
              <span id="totalCores">
                {queryCountsMap.getOrElse("totalQueries", 0)}
              </span>
              <span style="padding-left: 5px;">Active Queries:</span>
              <span id="totalCores">
                {queryCountsMap.getOrElse("totalActiveQueries", 0)}
              </span>
              <span style="padding-left: 5px;">Inactive Queries:</span>
              <span id="totalCores">
                {queryCountsMap.getOrElse("totalStoppedQueries", 0)}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
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
      <div class="vertical-menu-heading">
        <div>
          Streaming Queries
        </div>
      </div>
      <div class="vertical-menu">
        {
          activeQueries.map(ql => {
            val qid = "" + ql._2
            val url = "/structurestreaming/?query=" + qid;
            <a id={qid} href={url}
               class={if(qid.equalsIgnoreCase(selectedQueryId)) "active" else ""}>
              {ql._2}
            </a>
          })
        }
      </div>
    </div>
  }

  private def createQueryDetailsPanel: Seq[Node] = {
    <div class="right-details-panel">
      {createQueryDetailsEntry}
    </div>
  }

  private def createQueryDetailsEntry (): Seq[Node] = {

    val sqpEntry = activeQueryProgress.find(_._2.name.equalsIgnoreCase(selectedQueryId)).getOrElse(null)
    if(sqpEntry == null) {
      <div id="querydetails"> Query details not found </div>
    }
    val sqpEntryValue = sqpEntry._2
    val aqpEntry = allQueriesBasicDetails.get(sqpEntry._1).get

    // println("aqpEntry: ")
    // println(aqpEntry)

    // println("aqpEntry aqpEntry.get(\"startTime\").get: ")
    // println(aqpEntry.get("startTime").get)

    // println("aqpEntry.get(\"startTime\")")
    // println(aqpEntry.get("startTime"))

    // println("--------------------------------------------------------------------------")
    // println(sqpEntryValue)
    // println("--------------------------------------------------------------------------")

    <div id="querydetails">
      <div class="container-fluid details-section">
        <div class="basic-details">
          <div style="margin: 10px;width: auto;height: auto;">
            <div style="text-align: left;width: 100%;">
              <div style="float: left; padding: 10px; width: 30%; font-size: medium;
                          font-weight: bold;">
                Start Date &amp; Time :
              </div>
              <div style="/*! float: left; */padding: 10px;width: 70%;">
                {simpleDateFormat.format(aqpEntry("startTime"))}
              </div>
            </div>
            <div style="text-align: left;width: 100%;">
              <div style="float: left; padding: 10px; width: 30%; font-size: medium;
                          font-weight: bold;">
                Duration :
              </div>
              <div style="/*! float: left; */padding: 10px;width: 70%;">
                {
                  val st = aqpEntry("startTime").toString.toLong
                  val ct = System.currentTimeMillis()
                  UIUtils.formatDurationVerbose(ct -st)
                }
              </div>
            </div>
          </div>
        </div>
        <div class="basic-details">
          <div style="margin: 10px;width: auto;height: auto;">
            <div style="text-align: left;width: 100%;">
              <div style="float: left; padding: 10px; width: 20%; font-size: medium;
                          font-weight: bold;">
                Restarted
              </div>
              <div style="float: left;padding: 10px;width: 25%;">
                {
                  if (aqpEntry("isRestarted").toString.toBoolean) "YES" else "NO"
                }
              </div>
              <div style="float: left; padding: 10px; width: 20%; font-size: medium;
                          font-weight: bold;">
                Attempt #
              </div>
              <div style="float: left;padding: 10px;width: 20%;">
                {aqpEntry("attemptCount")}
              </div>
            </div>
            <div style="text-align: left;width: 100%;">
              <div style="float: left; padding: 10px; width: 20%; font-size: medium;
                          font-weight: bold;">
                Batch Interval
              </div>
              <div style="float: left;padding: 10px;width: 25%;">
                5 Secs
              </div>
              <div style="float: left; padding: 10px; width: 20%; font-size: medium;
                          font-weight: bold;">
                Batches Proceesed
              </div>
              <div style="float: left;padding: 10px;width: 20%;">
                TBD
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="container-fluid details-section">
        <div class="stats-block" style="width: 14%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">STATUS</div>
            <div style="margin-top: 20px;font-size: 20px;">
              {sqpEntryValue.numInputRows}
            </div>
          </div>
        </div>
        <div class="stats-block" style="width: 15%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">TOTAL INPUT ROWS</div>
            <div style="margin-top: 20px;font-size: 20px;">
              {sqpEntryValue.numInputRows}
            </div>
          </div>
        </div>
        <div class="stats-block" style="width: 15%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">INPUT ROWS / SEC</div>
            <div style="margin-top: 20px;font-size: 20px;">
              {sqpEntryValue.inputRowsPerSecond}
            </div>
          </div>
        </div>
        <div class="stats-block" style="width: 15%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">Heading</div>
            <div style="margin-top: 20px;font-size: 20px;">TBD</div>
          </div>
        </div>
        <div class="stats-block" style="width: 15%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">Heading</div>
            <div style="margin-top: 20px;font-size: 20px;">TBD</div>
          </div>
        </div>
        <div class="stats-block" style="width: 15%;">
          <div style="margin: 10px; width: auto; height: 80%;">
            <div style="font-weight: bold;font-size: large;">Heading</div>
            <div style="margin-top: 20px;font-size: 20px;">TBD</div>
          </div>
        </div>
      </div>
      <div class="container-fluid details-section">
        <div id="cpuUsageContainer" class="graph-container">TBD Chart
        </div>
        <div id="heapUsageContainer" class="graph-container">TBD Chart
        </div>
        <div id="offheapUsageContainer" class="graph-container">TBD Chart
        </div>
        <div id="diskSpaceUsageContainer" class="graph-container">TBD Chart
        </div>
      </div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;">
          Sources
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div class="container-fluid details-section"
           style="height: 100px; border: 1px solid grey; padding: 10px; margin: 10px;">
        {sqpEntryValue.sources} <span>{sqpEntryValue.name}</span>
      </div>
      <div class="container-fluid details-section">
        <div style="width: 5%;display: inline-block;border: 1px #8e8e8e solid;"></div>
        <div style="width: 10%;display: inline-block;font-size: 20px;font-weight: bold;">
          Sink
        </div>
        <div style="width: 84%;display: inline-block;border: 1px #8e8e8e solid;"></div>
      </div>
      <div class="container-fluid details-section"
           style="height: 100px; border: 1px solid grey; padding: 10px; margin: 10px;">
        {sqpEntryValue.sink}
      </div>
    </div>
  }
}

object SnappyStructuredStreamingPage {
  val pageHeaderText = "Structured Streaming Queries"

}
