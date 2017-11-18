/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import scala.xml.{Unparsed, Node}

import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.sql.execute.{MemberLogsMessage}
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


private[ui] class SnappyMemberDetailsPage (parent: SnappyDashboardTab)
    extends WebUIPage("memberDetails") with Logging {

  private var workDir: File = null
  private var logFileName: String = null
  private val defaultBytes:Long = 1024 * 100

  private def createPageTitleNode(title: String): Seq[Node] = {

    val sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss")
    val lastUpdatedOn = sdf.format(new Date())

    <div class="row-fluid">
      <div class="span12">
        <h3 style="vertical-align: bottom; display: inline-block;">
          {title}
        </h3>
        <span style="float:right; font-size: 12px;" data-toggle="tooltip" title=""
              data-original-title="Reload page to refresh Dashboard." >Last updated on {
          lastUpdatedOn
          }</span>
      </div>
    </div>
  }

  private def getMemberStats(memberDetails: mutable.Map[String, Any]): Seq[Node] = {

    val status = memberDetails.getOrElse("status", "")

    val statusImgUri = if(status.toString.equalsIgnoreCase("running")) {
      "/static/snappydata/running-status-icon-70x68.png"
    } else {
      "/static/snappydata/warning-status-icon-70x68.png"
    }

    val memberType = {
      if(memberDetails.getOrElse("lead", false).toString.toBoolean){
        if(memberDetails.getOrElse("activeLead", false).toString.toBoolean)
          "LEAD (Active)"
        else
          "LEAD"
      } else if(memberDetails.getOrElse("locator",false).toString.toBoolean){
        "LOCATOR"
      } else if(memberDetails.getOrElse("dataServer",false).toString.toBoolean){
        "DATA SERVER"
      } else {
        "CONNECTOR"
      }
    }

    val cpuUsage = memberDetails.getOrElse("cpuActive",0).asInstanceOf[Integer].toDouble;

    val heapStoragePoolUsed = memberDetails.getOrElse("heapStoragePoolUsed", 0).asInstanceOf[Long]
    val heapStoragePoolSize = memberDetails.getOrElse("heapStoragePoolSize", 0).asInstanceOf[Long]
    val heapExecutionPoolUsed = memberDetails.getOrElse("heapExecutionPoolUsed", 0).asInstanceOf[Long]
    val heapExecutionPoolSize = memberDetails.getOrElse("heapExecutionPoolSize", 0).asInstanceOf[Long]

    val offHeapStoragePoolUsed = memberDetails.getOrElse("offHeapStoragePoolUsed", 0).asInstanceOf[Long]
    val offHeapStoragePoolSize = memberDetails.getOrElse("offHeapStoragePoolSize", 0).asInstanceOf[Long]
    val offHeapExecutionPoolUsed = memberDetails.getOrElse("offHeapExecutionPoolUsed", 0).asInstanceOf[Long]
    val offHeapExecutionPoolSize = memberDetails.getOrElse("offHeapExecutionPoolSize", 0).asInstanceOf[Long]

    val heapMemorySize = memberDetails.getOrElse("heapMemorySize", 0).asInstanceOf[Long]
    val heapMemoryUsed = memberDetails.getOrElse("heapMemoryUsed", 0).asInstanceOf[Long]
    val offHeapMemorySize = memberDetails.getOrElse("offHeapMemorySize", 0).asInstanceOf[Long]
    val offHeapMemoryUsed = memberDetails.getOrElse("offHeapMemoryUsed", 0).asInstanceOf[Long]
    val jvmHeapSize = memberDetails.getOrElse("totalMemory", 0).asInstanceOf[Long]
    val jvmHeapUsed = memberDetails.getOrElse("usedMemory",0).asInstanceOf[Long]

    var memoryUsage:Long = 0
    if((heapMemorySize + offHeapMemorySize) > 0) {
      memoryUsage = (heapMemoryUsed + offHeapMemoryUsed) * 100 /
          (heapMemorySize + offHeapMemorySize)
    }
    var jvmHeapUsage:Long = 0
    if(jvmHeapSize > 0) {
      jvmHeapUsage = jvmHeapUsed * 100 / jvmHeapSize
    }

    val heapHtmlContent = if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
      <div class="keyStatsValue" style="border: 1px solid #e2e2e2; border-radius: 10px;">
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Storage :</span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Execution : </span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Total :</span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
      </div>
    } else {
      <div class="keyStatsValue" style="border: 1px solid #e2e2e2; border-radius: 10px;">
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Storage :</span>
          <span>{ Utils.bytesToString(heapStoragePoolUsed).toString + " / " +
              Utils.bytesToString(heapStoragePoolSize).toString }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Execution : </span>
          <span>{ Utils.bytesToString(heapExecutionPoolUsed).toString + " / " +
              Utils.bytesToString(heapExecutionPoolSize).toString }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Total :</span>
          <span>{ Utils.bytesToString(heapMemoryUsed).toString + " / " +
              Utils.bytesToString(heapMemorySize).toString }</span>
        </div>
      </div>
    }

    val offHeapHtmlContent = if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
      <div class="keyStatsValue" style="border: 1px solid #e2e2e2; border-radius: 10px;">
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Storage :</span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Execution : </span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Total :</span>
          <span>{ SnappyMemberDetailsPage.ValueNotApplicable }</span>
        </div>
      </div>
    } else {
      <div class="keyStatsValue" style="border: 1px solid #e2e2e2; border-radius: 10px;">
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Storage :</span>
          <span>{ Utils.bytesToString(offHeapStoragePoolUsed).toString + " / " +
              Utils.bytesToString(offHeapStoragePoolSize).toString }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Execution : </span>
          <span>{ Utils.bytesToString(offHeapExecutionPoolUsed).toString + " / " +
              Utils.bytesToString(offHeapExecutionPoolSize).toString }</span>
        </div>
        <div style="text-align: left; padding: 5px;">
          <span style="font-weight:bold;">Total :</span>
          <span>{ Utils.bytesToString(offHeapMemoryUsed).toString + " / " +
              Utils.bytesToString(offHeapMemorySize).toString }</span>
        </div>
      </div>
    }

    <div class="row-fluid">
      <div class="keyStates" style="width: 300px; margin: 0px 10px;">
        <div class="keyStatesText" style="text-align: left;">
          Member : <span>{memberDetails.getOrElse("id","NA")}</span>
        </div>
        <div class="keyStatesText" style="text-align: left;">
          Type : <span>{memberType}</span>
        </div>
        <div class="keyStatesText" style="text-align: left;">
          Process ID : <span>{memberDetails.getOrElse("processId","").toString}</span>
        </div>
      </div>
      <div class="keyStates" style="width: 250px; margin: 0px 10px;">
        {heapHtmlContent}
        <div class="keyStatesText">Heap Memory</div>
      </div>
      <div class="keyStates" style="width: 250px; margin: 0px 10px;">
        {offHeapHtmlContent}
        <div class="keyStatesText">Off-Heap Memory</div>
      </div>
      <div class="keyStates" style="margin: 0px 10px;">
        <div class="keyStatsValue"
             style="width:50%; margin: auto;" data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("status").toString + ": " + status.toString
             } >
          <img style="padding-top: 15px;" src={statusImgUri} />
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("status")}</div>
      </div>
      <div class="keyStates" style="margin: 0px 10px;">
        <div class="keyStatsValue" id="cpuUsage" data-value={cpuUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("cpuUsageTooltip").toString
             }>
          <svg id="cpuUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("cpuUsage")}</div>
      </div>
      <div class="keyStates" style="margin: 0px 10px;">
        <div class="keyStatsValue" id="memoryUsage" data-value={memoryUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("memoryUsageTooltip").toString
             }>
          <svg id="memoryUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("memoryUsage")}</div>
      </div>
      <div class="keyStates" style="margin: 0px 10px;">
        <div class="keyStatsValue" id="jvmHeapUsage" data-value={jvmHeapUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("jvmHeapUsageTooltip").toString
             }>
          <svg id="jvmHeapUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("jvmHeapUsage")}</div>
      </div>
    </div>
  }

  override def render(request: HttpServletRequest): Seq[Node] = {

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toLong).getOrElse(defaultBytes)

    val memberId = Option(request.getParameter("memId")).map { memberId =>
      UIUtils.decodeURLParameter(memberId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val allMembers = SnappyTableStatsProviderService.getService.getMembersStatsOnDemand
    val memberDetails: scala.collection.mutable.Map[String, Any] = {
      var mem = scala.collection.mutable.Map.empty[String, Any]
      breakable {
        allMembers.foreach(m => {
          if (m._2("id").toString.equalsIgnoreCase(memberId)) {
            mem = m._2
            break
          }
        })
      }
      mem
    }

    if(memberDetails.isEmpty){
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val memberStats = getMemberStats(memberDetails)

    // set members workDir and LogFileName
    workDir = new File(memberDetails.getOrElse("userDir", "").toString)
    logFileName = memberDetails.getOrElse("logFile", "").toString

    // Get Log Details
    val collector = new GfxdListResultCollector(null, true)
    val msg = new MemberLogsMessage(collector)
    msg.setMemberId(memberId)
    msg.setByteLength(byteLength)
    msg.setLogDirectory(workDir);
    msg.setLogFileName(logFileName);

    if(offset == None){
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

    val pageHeaderText : String  = SnappyMemberDetailsPage.pageHeaderText

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    var PageContent: Seq[Node] = mutable.Seq.empty

    val memberLogTitle =
      <div class="row-fluid">
        <div class="span12">
          <h4 style="vertical-align: bottom; display: inline-block;"
              data-toggle="tooltip" data-placement="top" title=""
              data-original-title="Member Logs">
            Member Logs
          </h4>
          <div style="margin-left:15px;">
            <span style="font-weight: bolder;">Location :</span>
            {memberDetails.getOrElse("userDir", "")}/{memberDetails.getOrElse("logFile", "")}
          </div>
        </div>
      </div>

    PageContent = pageTitleNode ++ memberStats ++ memberLogTitle ++ content

    UIUtils.simpleSparkPageWithTabs(pageHeaderText, PageContent, parent, Some(500))
  }

  def renderLog(request: HttpServletRequest): String = {

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toLong).getOrElse(defaultBytes)

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

    if(offset == None){
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

    val pre = s"==== Bytes $startByte-$endByte of $logLength of ${workDir.getPath}/$logFileName ====\n"

    pre + logText

  }

}

object SnappyMemberDetailsPage{
  val pageHeaderText = "SnappyData Member Details"

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