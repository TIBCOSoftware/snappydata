/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.xml.{Node, Unparsed}

import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberLogsMessage
import com.pivotal.gemfirexd.internal.engine.ui.MemberStatistics
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


private[ui] class SnappyMemberDetailsPage(parent: SnappyDashboardTab)
    extends WebUIPage("memberDetails") with Logging {

  private var workDir: File = null
  private var logFileName: String = null
  private val defaultBytes: Long = 1024 * 100

  private def createPageTitleNode(title: String): Seq[Node] = {
    <div id="AutoUpdateErrorMsgContainer">
      <div id="AutoUpdateErrorMsg">
      </div>
    </div>
    <div id="autorefreshswitch-container">
      <div id="autorefreshswitch-holder">
        <div class="onoffswitch">
          <input type="checkbox" name="onoffswitch" class="onoffswitch-checkbox"
                 id="myonoffswitch" checked="checked" />
          <label class="onoffswitch-label" for="myonoffswitch" data-toggle="tooltip" title=""
                 data-original-title="ON/OFF Switch for Auto Update of Statistics">
            <span class="onoffswitch-inner"></span>
            <span class="onoffswitch-switch"></span>
          </label>
        </div>
        <div id="autorefreshswitch-label">Auto Refresh:</div>
      </div>
    </div>
    <div class="row-fluid">
      <div class="span12">
        <h3 class="page-title-node-h3">
          {title}
        </h3>
      </div>
    </div>
  }

  private def getMemberStats(memberDetails: MemberStatistics): Seq[Node] = {

    val status = memberDetails.getStatus

    val statusImgUri = if (status.equalsIgnoreCase("running")) {
      "/static/snappydata/running-status-icon-70x68.png"
    } else {
      "/static/snappydata/warning-status-icon-70x68.png"
    }

    val memberType = {
      if (memberDetails.isLead) {
        if (memberDetails.isLeadActive) {
          "LEAD (Active)"
        } else {
          "LEAD"
        }
      } else if (memberDetails.isLocator) {
        "LOCATOR"
      } else if (memberDetails.isDataServer) {
        "DATA SERVER"
      } else {
        "CONNECTOR"
      }
    }

    val cpuUsage = memberDetails.getCpuActive.toDouble;

    val diskStoreDiskSpace = memberDetails.getDiskStoreDiskSpace

    val heapStoragePoolUsed = memberDetails.getHeapStoragePoolUsed
    val heapStoragePoolSize = memberDetails.getHeapStoragePoolSize
    val heapExecutionPoolUsed = memberDetails.getHeapExecutionPoolUsed
    val heapExecutionPoolSize = memberDetails.getHeapExecutionPoolSize

    val offHeapStoragePoolUsed = memberDetails.getOffHeapStoragePoolUsed
    val offHeapStoragePoolSize = memberDetails.getOffHeapStoragePoolSize
    val offHeapExecutionPoolUsed = memberDetails.getOffHeapExecutionPoolUsed
    val offHeapExecutionPoolSize = memberDetails.getOffHeapExecutionPoolSize

    val heapMemorySize = memberDetails.getHeapMemorySize
    val heapMemoryUsed = memberDetails.getHeapMemoryUsed
    val offHeapMemorySize = memberDetails.getOffHeapMemorySize
    val offHeapMemoryUsed = memberDetails.getOffHeapMemoryUsed
    val jvmHeapSize = memberDetails.getJvmTotalMemory
    val jvmHeapUsed = memberDetails.getJvmUsedMemory

    var memoryUsage: Long = 0
    if ((heapMemorySize + offHeapMemorySize) > 0) {
      memoryUsage = (heapMemoryUsed + offHeapMemoryUsed) * 100 /
          (heapMemorySize + offHeapMemorySize)
    }
    var jvmHeapUsage: Long = 0
    if (jvmHeapSize > 0) {
      jvmHeapUsage = jvmHeapUsed * 100 / jvmHeapSize
    }

    val memberBasicDetailsContent = {
      <div class="basic-stats">
        <span>Member :</span><br/>
        <span class="basic-stats-value"> {memberDetails.getId} </span>
      </div>
      <div class="basic-stats">
        <span>Type :</span><br/>
        <span class="basic-stats-value"> {memberType} </span>
      </div>
      <div class="basic-stats">
        <span>Process ID :</span><br/>
        <span class="basic-stats-value"> {memberDetails.getProcessId} </span>
      </div>
      <div class="basic-stats">
        <span>Status :</span><br/>
        <span class="basic-stats-value"> {status} </span>
      </div>
    }

    val heapHtmlContent = if (memberType.toString.equalsIgnoreCase("LOCATOR")) {
      <div class="basic-stats">
        <span>Storage Heap:</span><br/>
        <span class="basic-stats-value"> {SnappyMemberDetailsPage.ValueNotApplicable} </span>
      </div>
      <div class="basic-stats">
        <span>Execution Heap:</span><br/>
        <span class="basic-stats-value"> {SnappyMemberDetailsPage.ValueNotApplicable} </span>
      </div>
      <div class="basic-stats">
        <span>Total Heap:</span><br/>
        <span class="basic-stats-value"> {SnappyMemberDetailsPage.ValueNotApplicable} </span>
      </div>
    } else {
      <div class="basic-stats">
        <span>Storage Heap:</span><br/>
        <span id="currHeapStoragePool" class="basic-stats-value">
          {Utils.bytesToString(heapStoragePoolUsed).toString + " / " +
            Utils.bytesToString(heapStoragePoolSize).toString}
        </span>
      </div>
      <div class="basic-stats">
        <span>Execution Heap:</span><br/>
        <span id="currHeapExecutionPool" class="basic-stats-value">
          {Utils.bytesToString(heapExecutionPoolUsed).toString + " / " +
            Utils.bytesToString(heapExecutionPoolSize).toString}
        </span>
      </div>
      <div class="basic-stats">
        <span>Total Heap:</span><br/>
        <span id="currHeapMemory" class="basic-stats-value">
          {Utils.bytesToString(heapMemoryUsed).toString + " / " +
            Utils.bytesToString(heapMemorySize).toString}
        </span>
      </div>
    }

    val offHeapHtmlContent = if (memberType.toString.equalsIgnoreCase("LOCATOR")) {
      <div class="basic-stats">
        <span>Storage Off-Heap:</span><br/>
        <span class="basic-stats-value">
          {SnappyMemberDetailsPage.ValueNotApplicable}
        </span>
      </div>
      <div class="basic-stats">
        <span>Execution Off-Heap:</span><br/>
        <span class="basic-stats-value">
          {SnappyMemberDetailsPage.ValueNotApplicable}
        </span>
      </div>
      <div class="basic-stats">
        <span>Total Off-Heap:</span><br/>
        <span class="basic-stats-value">
          {SnappyMemberDetailsPage.ValueNotApplicable}
        </span>
      </div>
    } else {
      <div class="basic-stats">
          <span>Storage Off-Heap:</span><br/>
          <span id="currOffHeapStoragePool" class="basic-stats-value">
            {Utils.bytesToString(offHeapStoragePoolUsed).toString + " / " +
              Utils.bytesToString(offHeapStoragePoolSize).toString}
          </span>
        </div>
        <div class="basic-stats">
          <span>Execution Off-Heap:</span><br/>
          <span id="currOffHeapExecutionPool" class="basic-stats-value">
            {Utils.bytesToString(offHeapExecutionPoolUsed).toString + " / " +
              Utils.bytesToString(offHeapExecutionPoolSize).toString}
          </span>
        </div>
        <div class="basic-stats">
          <span>Total Off-Heap:</span><br/>
          <span id="currOffHeapMemory" class="basic-stats-value">
            {Utils.bytesToString(offHeapMemoryUsed).toString + " / " +
              Utils.bytesToString(offHeapMemorySize).toString}
          </span>
        </div>
    }

    val diskSpaceHtmlContent = {
      <div class="basic-stats">
        <span>Disk Space:</span> <br/>
        <span id="currDiskSpace" class="basic-stats-value">
          {Utils.bytesToString(diskStoreDiskSpace).toString}
        </span>
      </div>
    }

    <div class="container-fluid" style="text-align: center;">
      <div class="basic-stats-container">
        {memberBasicDetailsContent}
        <div class="basic-stats-separator"></div>
        {heapHtmlContent}
        <div class="basic-stats-separator"></div>
        {offHeapHtmlContent}
        <div class="basic-stats-separator"></div>
        {diskSpaceHtmlContent}
      </div>
    </div>
    <div class="container-fluid" style="text-align: center;">
      <div id="cpuUsageContainer" class="graph-container">
      </div>
      <div id="heapUsageContainer" class="graph-container">
      </div>
      <div id="offheapUsageContainer" class="graph-container">
      </div>
      <div id="diskSpaceUsageContainer" class="graph-container">
      </div>
    </div>
  }

  override def render(request: HttpServletRequest): Seq[Node] = {

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength =
      Option(request.getParameter("byteLength")).map(_.toLong).getOrElse(defaultBytes)

    val memberId = Option(request.getParameter("memId")).map { memberId =>
      UIUtils.decodeURLParameter(memberId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val allMembers = SnappyTableStatsProviderService.getService.getMembersStatsFromService
    val memberDetails: MemberStatistics = {
      var mem: MemberStatistics = null
      breakable {
        allMembers.foreach(m => {
          if (m._2.getId().equalsIgnoreCase(memberId)) {
            mem = m._2
            break
          }
        })
      }
      mem
    }

    if (memberDetails == null) {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val memberStats = getMemberStats(memberDetails)

    // set members workDir and LogFileName
    workDir = new File(memberDetails.getUserDir)
    logFileName = memberDetails.getLogFile

    // Get Log Details
    val collector = new GfxdListResultCollector(null, true)
    val msg = new MemberLogsMessage(collector)
    msg.setMemberId(memberId)
    msg.setByteLength(byteLength)
    msg.setLogDirectory(workDir);
    msg.setLogFileName(logFileName);

    if (offset == None) {
      // set offset null
      msg.setOffset(null)
    } else {
      msg.setOffset(offset.get)
    }

    msg.executeFunction()

    val memStats = collector.getResult
    val itr = memStats.iterator()
    var logData: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any];

    while (itr.hasNext) {
      val o = itr.next().asInstanceOf[ListResultCollectorValue]
      val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]
      logData = memMap.get("logData").asInstanceOf[java.util.HashMap[String, Any]]
    }

    val logText = logData.get("logText")
    val startByte = logData.get("startIndex").asInstanceOf[Long]
    val endByte = logData.get("endIndex").asInstanceOf[Long]
    val logLength = logData.get("totalLength").asInstanceOf[Long]

    val curLogLength = endByte - startByte

    val range =
      <span id="log-data" style="font-weight:bold;">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMore()"} class="log-more-btn btn btn-default">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNew()"} class="log-new-btn btn btn-default">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "/?memId=%s".format(memberId)

    val jsOnload = "window.onload = " +
        s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <div style="margin-top:5px; margin-left:15px;">
        {range}
        <div class="log-content"
             style="height:60vh; overflow:auto; margin-top:5px; border: 1px solid #E2E2E2;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    val pageHeaderText: String = SnappyMemberDetailsPage.pageHeaderText

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    var PageContent: Seq[Node] = mutable.Seq.empty

    val memberLogTitle =
      <div class="row-fluid">
        <div class="span12">
          <h4 class="title-node-h4"
              data-toggle="tooltip" data-placement="top" title=""
              data-original-title="Member Logs">
            Member Logs
          </h4>
          <div style="margin-left:15px;">
            <span style="font-weight: bolder;">Location :</span>
            {memberDetails.getUserDir}/{memberDetails.getLogFile}
          </div>
        </div>
      </div>

    val jsScripts = <script src={
                              UIUtils.prependBaseUri("/static/snappydata/snappy-memberdetails.js")
                            }></script> ++
                    <script>setMemberId('{Unparsed("%s".format(memberId))}');</script>

    PageContent = jsScripts ++ pageTitleNode ++ memberStats ++ memberLogTitle ++ content

    UIUtils.headerSparkPage(pageHeaderText, PageContent, parent, Some(500),
      useDataTables = true, isSnappyPage = true)
  }

  def renderLog(request: HttpServletRequest): String = {

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength =
      Option(request.getParameter("byteLength")).map(_.toLong).getOrElse(defaultBytes)

    val memberId = Option(request.getParameter("memId")).map { memberId =>
      UIUtils.decodeURLParameter(memberId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    // Get Log Details
    val collector = new GfxdListResultCollector(null, true)
    val msg = new MemberLogsMessage(collector)
    msg.setMemberId(memberId)
    msg.setByteLength(byteLength)
    msg.setLogDirectory(workDir)
    msg.setLogFileName(logFileName)

    if (offset == None) {
      // set offset null
      msg.setOffset(null)
    } else {
      msg.setOffset(offset.get)
    }

    msg.executeFunction()

    val memStats = collector.getResult
    val itr = memStats.iterator()
    var logData: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any];

    while (itr.hasNext) {
      val o = itr.next().asInstanceOf[ListResultCollectorValue]
      val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]
      logData = memMap.get("logData").asInstanceOf[java.util.HashMap[String, Any]]
    }
    val logText = logData.get("logText")
    val startByte = logData.get("startIndex").asInstanceOf[Long]
    val endByte = logData.get("endIndex").asInstanceOf[Long]
    val logLength = logData.get("totalLength").asInstanceOf[Long]

    val pre =
      s"==== Bytes $startByte-$endByte of $logLength of ${workDir.getPath}/$logFileName ====\n"

    pre + logText

  }

}

object SnappyMemberDetailsPage {
  val pageHeaderText = "Member Details"

  object Status {
    val stopped = "Stopped"
    val running = "Running"
  }

  val ValueNotApplicable = "N/A"

  val memberStats = scala.collection.mutable.HashMap.empty[String, String]
  memberStats += ("status" -> "Status")
  memberStats += ("statusTooltip" -> "Members Status")
  memberStats += ("cpuUsage" -> "CPU Usage")
  memberStats += ("cpuUsageTooltip" -> "CPU used by Member Host")
  memberStats += ("memoryUsage" -> "Memory Usage")
  memberStats += ("memoryUsageTooltip" -> "Memory(Heap + Off-Heap) used by Member")
  memberStats += ("jvmHeapUsage" -> "JVM Heap Usage")
  memberStats += ("jvmHeapUsageTooltip" -> "Clusters Total JVM Heap Usage")
}