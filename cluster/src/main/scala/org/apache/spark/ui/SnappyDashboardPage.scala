/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.Node

import com.pivotal.gemfirexd.internal.engine.ui.{SnappyIndexStats, SnappyRegionStats}
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[ui] class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText : String  = SnappyDashboardPage.pageHeaderText

    var clusterStatsMap = scala.collection.mutable.HashMap.empty[String, Any]
    clusterStatsMap += ("status" -> SnappyDashboardPage.Status.normal)
    clusterStatsMap += ("numMembers" -> 0)
    clusterStatsMap += ("numLeads" -> 0)
    clusterStatsMap += ("numLocators" -> 0)
    clusterStatsMap += ("numTables" -> 0)
    clusterStatsMap += ("numDataServers" -> 0)
    clusterStatsMap += ("numClients" -> 0)
    clusterStatsMap += ("memoryUsage" -> 0)
    clusterStatsMap += ("numColumnTables" -> 0)
    clusterStatsMap += ("numRowTables" -> 0)

    val allMembers = SnappyTableStatsProviderService.getService.getMembersStatsOnDemand

    var clusterMembers = scala.collection.mutable.HashMap.empty[String, mutable.Map[String, Any]]
    var sparkConnectors = scala.collection.mutable.HashMap.empty[String, mutable.Map[String, Any]]

    allMembers.foreach(m => {
      if (!m._2("dataServer").toString.toBoolean
          && !m._2("activeLead").toString.toBoolean
          && !m._2("lead").toString.toBoolean
          && !m._2("locator").toString.toBoolean) {

        if (!m._2("status").toString.equalsIgnoreCase("stopped"))
          sparkConnectors += m

      } else {
        clusterMembers += m
      }
    })

    val (tableBuff, indexBuff) = SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand

    updateClusterStats(clusterStatsMap, clusterMembers, tableBuff)

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle,
                                SnappyDashboardPage.clusterStatsTitleTooltip)

    val clusterDetails = clusterStats(clusterStatsMap)

    val keyStatsSection = clustersStatsTitle ++ clusterDetails

    val membersStatsDetails = {
      val countsList:Array[mutable.Map[String, Any]] = new Array(3)
      countsList(0) = mutable.Map(
                           "value" -> clusterStatsMap.getOrElse("numLocators",0).toString.toInt,
                           "displayText" -> "Locators")
      countsList(1) = mutable.Map(
                           "value" -> clusterStatsMap.getOrElse("numLeads",0).toString.toInt,
                           "displayText" -> "Leads")
      countsList(2) = mutable.Map(
                           "value" -> clusterStatsMap.getOrElse("numDataServers",0).toString.toInt,
                           "displayText" -> "Data Servers")

      val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle,
                                SnappyDashboardPage.membersStatsTitleTooltip,
                                countsList)
      val membersStatsTable = memberStats(clusterMembers)

      membersStatsTitle ++ membersStatsTable
    }

    val sparkConnectorsStatsDetails = {
      val sparkConnectorsStatsTitle = createTitleNode(SnappyDashboardPage.sparkConnectorsStatsTitle,
                                SnappyDashboardPage.sparkConnectorsStatsTitleTooltip,
                                sparkConnectors.size)
      val sparkConnectorsStatsTable = connectorStats(sparkConnectors)

      if(sparkConnectors.size > 0)
        sparkConnectorsStatsTitle ++ sparkConnectorsStatsTable
      else
        mutable.Seq.empty[Node]
    }

    val tablesStatsDetails = {
      val countsList:Array[mutable.Map[String, Any]] = new Array(2)
      countsList(0) = mutable.Map(
                           "value" -> clusterStatsMap.getOrElse("numColumnTables",0).toString.toInt,
                           "displayText" -> "Column Tables")
      countsList(1) = mutable.Map(
                           "value" -> clusterStatsMap.getOrElse("numRowTables",0).toString.toInt,
                           "displayText" -> "Row Tables")

      val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle,
                                SnappyDashboardPage.tablesStatsTitleTooltip,
                                countsList)
      val tablesStatsTable = tableStats(tableBuff)

      tablesStatsTitle ++ tablesStatsTable
    }

    val indexStatsDetails = {
      val indexStatsTitle = createTitleNode(SnappyDashboardPage.indexStatsTitle,
                               SnappyDashboardPage.indexStatsTitleTooltip,
                               indexBuff.size)
      val indexStatsTable = indexStats(indexBuff)

      if(indexBuff.size > 0)
        indexStatsTitle ++ indexStatsTable
      else
        mutable.Seq.empty[Node]
    }

    val pageContent = pageTitleNode ++ keyStatsSection ++ membersStatsDetails ++
                      sparkConnectorsStatsDetails ++ tablesStatsDetails ++ indexStatsDetails

    UIUtils.simpleSparkPageWithTabs(pageHeaderText, pageContent, parent, Some(500))

  }

  private def updateClusterStats(
      clusterStatsMap: mutable.HashMap[String, Any],
      membersBuf: mutable.Map[String, mutable.Map[String, Any]],
      tablesBuf: Map[String, SnappyRegionStats] ) : Unit = {

    val numMembers = membersBuf.size
    var isClusterStateNormal = true
    var numLead = 0
    var numLocator = 0
    var numDataServers = 0
    var numClients = 0
    var numClientsToLocator = 0
    var numClientsToDataServers = 0
    var numColumnTables = 0
    var numRowTables = 0
    var cpuUsage:Double = 0
    var memoryUsage:Double = 0;
    var heapUsage:Double = 0;
    var offHeapUsage:Double = 0;
    var jvmHeapUsage:Double = 0;

    var totalCPUSize:Int = 0;
    var totalCPUUsage:Int = 0;
    var totalMemorySize:Long = 0;
    var totalMemoryUsage:Long = 0;
    var totalHeapSize:Long = 0;
    var totalHeapUsage:Long = 0;
    var totalOffHeapSize:Long = 0;
    var totalOffHeapUsage:Long = 0;
    var totalJvmHeapSize:Long = 0;
    var totalJvmHeapUsage:Long = 0;

    val hostsList:mutable.Set[String] = mutable.Set.empty[String]

    membersBuf.foreach(mb => {
      val m = mb._2

      if(!m("status").toString.equalsIgnoreCase("running")){
        isClusterStateNormal = false
      }

      if(m("lead").toString.toBoolean || m("activeLead").toString.toBoolean){
        numLead += 1
      }
      if(m("locator").toString.toBoolean){
        numLocator += 1
        numClientsToLocator = m("clients").toString.toInt
      }
      if(m("dataServer").toString.toBoolean
          && !m("activeLead").toString.toBoolean
          && !m("lead").toString.toBoolean
          && !m("locator").toString.toBoolean){
        numDataServers += 1
      }

      numClients += m("clients").toString.toInt

      totalHeapSize = totalHeapSize + m("heapMemorySize").asInstanceOf[Long]
      totalHeapUsage = totalHeapUsage + m("heapMemoryUsed").asInstanceOf[Long]

      totalOffHeapSize = totalOffHeapSize + m("offHeapMemorySize").asInstanceOf[Long]
      totalOffHeapUsage = totalOffHeapUsage + m("offHeapMemoryUsed").asInstanceOf[Long]

      totalJvmHeapSize = totalJvmHeapSize + m("totalMemory").asInstanceOf[Long]
      totalJvmHeapUsage = totalJvmHeapUsage + m("usedMemory").asInstanceOf[Long]

      // CPU
      if(!hostsList.contains(m("host").toString)
          && !m("locator").toString.toBoolean ){
        hostsList.add(m("host").toString)
        totalCPUUsage = totalCPUUsage + m("cpuActive").asInstanceOf[Int]
        totalCPUSize += 1
      }

    })

    if(membersBuf.size > 0){
      totalMemorySize = totalHeapSize + totalOffHeapSize
      totalMemoryUsage = totalHeapUsage + totalOffHeapUsage

      if(totalMemorySize > 0)
        memoryUsage = totalMemoryUsage * 100 / totalMemorySize
      if(totalHeapSize > 0)
        heapUsage = totalHeapUsage * 100 / totalHeapSize
      if(totalOffHeapSize > 0)
        offHeapUsage = totalOffHeapUsage * 100 / totalOffHeapSize
      if(totalJvmHeapSize > 0)
        jvmHeapUsage = totalJvmHeapUsage * 100 / totalJvmHeapSize

      cpuUsage = totalCPUUsage / totalCPUSize
    }

    numClientsToDataServers = numClients - numClientsToLocator

    tablesBuf.foreach(tb => {
      val tbl = tb._2

      if(tbl.isColumnTable)
        numColumnTables += 1
      else
        numRowTables += 1
    })

    clusterStatsMap += ("status" -> {if(isClusterStateNormal) "Normal" else "Warning"})
    clusterStatsMap += ("numMembers" -> numMembers)
    clusterStatsMap += ("numTables" -> tablesBuf.size)
    clusterStatsMap += ("numLeads" -> numLead)
    clusterStatsMap += ("numLocators" -> numLocator)
    clusterStatsMap += ("numDataServers" -> numDataServers)
    clusterStatsMap += ("numClients" -> numClients)
    clusterStatsMap += ("numClientsToLocator" -> numClientsToLocator)
    clusterStatsMap += ("numClientsToDataServers" -> numClientsToDataServers)
    clusterStatsMap += ("numColumnTables" -> numColumnTables)
    clusterStatsMap += ("numRowTables" -> numRowTables)
    clusterStatsMap += ("cpuUsage" -> cpuUsage)
    clusterStatsMap += ("memoryUsage" -> memoryUsage)
    clusterStatsMap += ("totalMemoryUsage" -> totalMemoryUsage)
    clusterStatsMap += ("heapUsage" -> heapUsage)
    clusterStatsMap += ("totalHeapUsage" -> totalHeapUsage)
    clusterStatsMap += ("offHeapUsage" -> offHeapUsage)
    clusterStatsMap += ("totalOffHeapUsage" -> totalOffHeapUsage)
    clusterStatsMap += ("jvmHeapUsage" -> jvmHeapUsage)
    clusterStatsMap += ("totalJvmHeapUsage" -> totalJvmHeapUsage)

  }

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

  private def createTitleNode(title: String, tooltip: String): Seq[Node] = {
    <div class="row-fluid">
      <div class="span12">
        <h4 style="vertical-align: bottom; display: inline-block;"
            data-toggle="tooltip" data-placement="top" title={tooltip}>
          {title}
        </h4>
      </div>
    </div>
  }

  private def createTitleNode(title:String, tooltip:String,
      count:Integer): Seq[Node] = {
    <div class="row-fluid">
      <div class="span12">
        <h4 style="vertical-align: bottom; display: inline-block;"
            data-toggle="tooltip" data-placement="top" title={tooltip}>
          {title}
        </h4>
        <div class="titleNodeCount" >( {count} )</div>
      </div>
    </div>
  }

  private def createTitleNode(title:String, tooltip:String,
      countList:Array[mutable.Map[String, Any]]): Seq[Node] = {
    var total = 0;
    var tooltipDetails = "";
    for(i <- 0 until countList.length){
      val ele = countList(i)
      total = total + ele.getOrElse("value", 0).toString.toInt
      if(tooltipDetails.isEmpty)
        tooltipDetails += ele.getOrElse("displayText", 0).toString + ": " +
            ele.getOrElse("value", 0).toString.toInt
      else
        tooltipDetails += " | " + ele.getOrElse("displayText", 0).toString + ": " +
            ele.getOrElse("value", 0).toString.toInt
    }

    <div class="row-fluid">
      <div class="span12">
        <h4 style="vertical-align: bottom; display: inline-block;"
            data-toggle="tooltip" data-placement="top" title={tooltip}>
          {title}
        </h4>
        <div class="titleNodeCount2">({
          <a data-toggle="tooltip" data-placement="top" title={tooltipDetails}>{total}</a>
          })</div>
      </div>
    </div>
  }

  private def clusterStats(clusterDetails: mutable.Map[String, Any]): Seq[Node] = {

    val status = clusterDetails.getOrElse("status", "")

    val statusImgUri = if(status.toString.equalsIgnoreCase("normal")) {
      "/static/snappydata/running-status-icon-70x68.png"
    } else {
      "/static/snappydata/warning-status-icon-70x68.png"
    }

    val cpuUsage = clusterDetails.getOrElse("cpuUsage", 0.0).asInstanceOf[Double];
    val memoryUsage = clusterDetails.getOrElse("memoryUsage", 0.0).asInstanceOf[Double];
    // val heapUsage = clusterDetails.getOrElse("heapUsage", 0.0).asInstanceOf[Double];
    // val offHeapUsage = clusterDetails.getOrElse("offHeapUsage", 0.0).asInstanceOf[Double];
    val jvmHeapUsage = clusterDetails.getOrElse("jvmHeapUsage", 0.0).asInstanceOf[Double];

    <div class="row-fluid">
      <div class="keyStates">
        <div class="keyStatsValue"
             style="width:50%; margin: auto;" data-toggle="tooltip" title=""
             data-original-title={
               SnappyDashboardPage.clusterStats("status").toString + ": " + status.toString
             } >
          <img style="padding-top: 15px;" src={statusImgUri} />
        </div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("status")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="cpuUsage" data-value={cpuUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
               SnappyDashboardPage.clusterStats("cpuUsageTooltip").toString
             }>
          <svg id="cpuUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("cpuUsage")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="memoryUsage" data-value={memoryUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyDashboardPage.clusterStats("memoryUsageTooltip").toString
             }>
          <svg id="memoryUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("memoryUsage")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="jvmHeapUsage" data-value={jvmHeapUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
               SnappyDashboardPage.clusterStats("jvmHeapUsageTooltip").toString
             }>
          <svg id="jvmHeapUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("jvmHeapUsage")}</div>
      </div>
    </div>
  }

  private def memberStats(membersBuf: mutable.Map[String, mutable.Map[String, Any]]): Seq[Node] = {

    val locators = mutable.Map.empty[String, mutable.Map[String, Any]]
    val leads = mutable.Map.empty[String, mutable.Map[String, Any]]
    val dataServers = mutable.Map.empty[String, mutable.Map[String, Any]]

    membersBuf.foreach(mb => {
      val m = mb._2

      if(m("lead").toString.toBoolean || m("activeLead").toString.toBoolean){
        leads.put(mb._1, mb._2)
      }
      if(m("locator").toString.toBoolean){
        locators.put(mb._1, mb._2)
      }
      if(m("dataServer").toString.toBoolean
          && !m("activeLead").toString.toBoolean
          && !m("lead").toString.toBoolean
          && !m("locator").toString.toBoolean){
        dataServers.put(mb._1, mb._2)
      }

    })

    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; width: 60px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("statusTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("status")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("descriptionTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("description")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 100px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("memberTypeTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("memberType")}
              </span>
            </th>
            <th style="text-align:center; width: 200px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("cpuUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("cpuUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 200px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("memoryUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("memoryUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 200px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("heapMemoryTooltip")
                    }
                    style="font-size: 17px;">
                Heap Memory<br/>(Used / Total)
              </span>
            </th>
            <th style="text-align:center; width: 200px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("offHeapMemoryTooltip")
                    }
                    style="font-size: 17px;">
                Off-Heap Memory<br/>(Used / Total)
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {dataServers.map(mb => memberRow(mb._2))}
          {leads.map(mb => memberRow(mb._2))}
          {locators.map(mb => memberRow(mb._2))}
        </tbody>
      </table>
    </div>
  }

  private def connectorStats(sparkConnectors: mutable.Map[String,
      mutable.Map[String, Any]]): Seq[Node] = {
    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.sparkConnectorsStatsColumn("nameOrIdTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("nameOrId")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.sparkConnectorsStatsColumn("cpuUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("cpuUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.sparkConnectorsStatsColumn("memoryUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("memoryUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 150px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.sparkConnectorsStatsColumn("usedMemoryTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("usedMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 150px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.sparkConnectorsStatsColumn("totalMemoryTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("totalMemory")}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {sparkConnectors.map(mb => connectorRow(mb._2))}
        </tbody>
      </table>
    </div>
  }

  private def tableStats(tablesBuf: Map[String, SnappyRegionStats]): Seq[Node] = {

    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("nameTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("name")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("storageModelTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("storageModel")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("distributionTypeTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("distributionType")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("rowCountTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("rowCount")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("sizeInMemoryTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("sizeInMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("totalSizeTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("totalSize")}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {tablesBuf.map(t => tableRow(t._2)).toArray}
        </tbody>
      </table>
    </div>
  }

  private def indexStats(indexBuf: Map[String, SnappyIndexStats]): Seq[Node] = {
    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyDashboardPage.indexStatsColumn("nameTooltip")}
                    style="font-size: 17px;">
                {SnappyDashboardPage.indexStatsColumn("name")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyDashboardPage.indexStatsColumn("rowCountTooltip")}
                    style="font-size: 17px;">
                {SnappyDashboardPage.indexStatsColumn("rowCount")}
              </span>
            </th>
            <th style="text-align:center; width: 250px; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={SnappyDashboardPage.indexStatsColumn("totalSizeTooltip")}
                    style="font-size: 17px;">
                {SnappyDashboardPage.indexStatsColumn("totalSize")}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {indexBuf.map(t => indexRow(t._2)).toArray}
        </tbody>
      </table>
    </div>
  }


  private def memberRow(memberDetails: mutable.Map[String, Any]): Seq[Node] = {

    val status = memberDetails.getOrElse("status","")
    val statusImgUri = if(status.toString.toLowerCase.equals("running")) {
      "/static/snappydata/running-status-icon-20x19.png"
    } else {
      "/static/snappydata/stopped-status-icon-20x19.png"
    }

    val nameOrId = {
      if(memberDetails.getOrElse("name","NA").equals("NA")
          || memberDetails.getOrElse("name","NA").equals("")){
        memberDetails.getOrElse("id","NA")
      }else{
        memberDetails.getOrElse("name","NA")
      }
    }

    val host = memberDetails.getOrElse("host", "").toString
    val fullDirName = memberDetails.getOrElse("userDir", "").toString
    val shortDirName = fullDirName.substring(
                         fullDirName.lastIndexOf(System.getProperty("file.separator")) + 1)
    val processId = memberDetails.getOrElse("processId","").toString

    val memberDescription = {
      host +
          " | " + shortDirName +
          " | " + processId
    }
    val memberDescriptionDetails = {
      <span><strong>Host:</strong> {host}
        <br/><strong>Directory:</strong> {fullDirName}
        <br/><strong>Process ID:</strong> {processId}
      </span>
    }

    val memberDescDetailsBtn = shortDirName + "-btn";
    val memberDescDetailsHandler = "toggleCellDetails('" + shortDirName + "');";

    val memberType = {
      if(memberDetails.getOrElse("lead", false).toString.toBoolean){
        if(memberDetails.getOrElse("activeLead", false).toString.toBoolean)
           <strong data-toggle="tooltip" title="" data-original-title="Active Lead">LEAD</strong>
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

    val heapDetailsId = shortDirName + "-heap"
    val heapDetailsBtn = heapDetailsId + "-btn"
    val heapDetailsHandler = "toggleCellDetails('" + heapDetailsId + "');";
    val heapUsageDetails = {
      if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
        <span><strong>JVM Heap:</strong>
          <br/> { Utils.bytesToString(jvmHeapUsed).toString + " / " +
            Utils.bytesToString(jvmHeapSize).toString }
          <br/><strong>Storage Memory:</strong>
          <br/> { SnappyDashboardPage.ValueNotApplicable }
          <br/><strong>Execution Memory:</strong>
          <br/> { SnappyDashboardPage.ValueNotApplicable }
        </span>
      } else {
        <span><strong>JVM Heap:</strong>
          <br/> { Utils.bytesToString(jvmHeapUsed).toString + " / " +
            Utils.bytesToString(jvmHeapSize).toString }
          <br/><strong>Storage Memory:</strong>
          <br/> { Utils.bytesToString(heapStoragePoolUsed).toString + " / " +
            Utils.bytesToString(heapStoragePoolSize).toString }
          <br/><strong>Execution Memory:</strong>
          <br/> { Utils.bytesToString(heapExecutionPoolUsed).toString + " / " +
            Utils.bytesToString(heapExecutionPoolSize).toString }
        </span>
      }
    }

    val offHeapDetailsId = shortDirName + "-offheap"
    val offHeapDetailsBtn = offHeapDetailsId + "-btn"
    val offHeapDetailsHandler = "toggleCellDetails('" + offHeapDetailsId + "');";
    val offHeapUsageDetails = {
      if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
        <span><strong>Storage Memory:</strong>
          <br/> { SnappyDashboardPage.ValueNotApplicable }
          <br/><strong>Execution Memory:</strong>
          <br/> { SnappyDashboardPage.ValueNotApplicable }
        </span>
      } else {
        <span><strong>Storage Memory:</strong>
          <br/> { Utils.bytesToString(offHeapStoragePoolUsed).toString + " / " +
            Utils.bytesToString(offHeapStoragePoolSize).toString }
          <br/><strong>Execution Memory:</strong>
          <br/> { Utils.bytesToString(offHeapExecutionPoolUsed).toString + " / " +
            Utils.bytesToString(offHeapExecutionPoolSize).toString }
        </span>
      }
    }

    <tr>
      <td>
        <div style="float: left; height: 24px; padding: 0 20px;" >
          <img src={statusImgUri} data-toggle="tooltip" title=""
               data-original-title={status.toString} />
        </div>
      </td>
      <td>
        <div style="width: 80%; float: left; padding-left: 10px; font-weight: bold;">
          <a href={
             parent.appUIBaseAddress +
                 "/" + parent.prefix +
                 "/memberDetails/?memId=" +
                 memberDetails.getOrElse("id","NA")
             }>{memberDescription}</a>
        </div>
        <div style="width: 10px; float: right; padding-right: 10px; cursor: pointer;"
             onclick={memberDescDetailsHandler}>
          <span class="caret-downward" id={memberDescDetailsBtn}></span>
        </div>
        <div class="cellDetailsBox" id={shortDirName}>
          {memberDescriptionDetails}
        </div>
      </td>
      <td>
        <div style="text-align:center;">{memberType}</div>
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("cpuActive",0).asInstanceOf[Integer].toDouble)}
      </td>
      <td>
        {makeProgressBar(memoryUsage)}
      </td>
      <td>
        <div style="width: 80%; float: left; padding-right:10px; text-align:right;">{
            if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
              SnappyDashboardPage.ValueNotApplicable
            } else {
              Utils.bytesToString(heapMemoryUsed).toString + " / " +
                  Utils.bytesToString(heapMemorySize).toString
            }
          }</div>
        <div style="width: 5px; float: right; padding-right: 10px; cursor: pointer;"
             onclick={heapDetailsHandler}>
          <span class="caret-downward" id={heapDetailsBtn}></span>
        </div>
        <div class="cellDetailsBox" id={heapDetailsId}
             style="width: 90%;">
          {heapUsageDetails}
        </div>
      </td>
      <td>
        <div style="width: 80%; float: left; padding-right:10px; text-align:right;">{
            if(memberType.toString.equalsIgnoreCase("LOCATOR")) {
              SnappyDashboardPage.ValueNotApplicable
            } else {
              Utils.bytesToString(offHeapMemoryUsed).toString + " / " +
                  Utils.bytesToString(offHeapMemorySize).toString
            }
          }</div>
        <div style="width: 5px; float: right; padding-right: 10px; cursor: pointer;"
             onclick={offHeapDetailsHandler}>
          <span class="caret-downward" id={offHeapDetailsBtn}></span>
        </div>
        <div class="cellDetailsBox" id={offHeapDetailsId}
             style="width: 90%;">
          {offHeapUsageDetails}
        </div>
      </td>
    </tr>
  }

  private def connectorRow(memberDetails: mutable.Map[String, Any]): Seq[Node] = {

    val nameOrId = {
      if(memberDetails.getOrElse("name","NA").equals("NA")
          || memberDetails.getOrElse("name","NA").equals("")){
        memberDetails.getOrElse("id","NA")
      }else{
        memberDetails.getOrElse("name","NA")
      }
    }

    val totalMemory = memberDetails.getOrElse("totalMemory", 0).asInstanceOf[Long]
    val usedMemory = memberDetails.getOrElse("usedMemory",0).asInstanceOf[Long]
    val memoryUsage: Double = (usedMemory * 100) / totalMemory

    <tr>
      <td>
        <div style="width:100%; padding-left:10px;">{nameOrId}</div>
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("cpuActive",0).asInstanceOf[Integer].toDouble)}
      </td>
      <td>
        {makeProgressBar(memoryUsage)}
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{
            Utils.bytesToString(usedMemory)
          }</div>
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{
            Utils.bytesToString(totalMemory).toString
          }</div>
      </td>
    </tr>
  }

  private def tableRow(tableDetails: SnappyRegionStats): Seq[Node] = {

    val numFormatter = java.text.NumberFormat.getIntegerInstance
    val storageModel = if (tableDetails.isColumnTable) " COLUMN " else " ROW "
    val distributionType = if (tableDetails.isReplicatedTable) " REPLICATED " else " PARTITIONED "

    <tr>
      <td>
        <div style="width:100%; padding-left:10px;">
          {tableDetails.getRegionName}
        </div>
      </td>
      <td>
        <div style="width:100%; text-align:center;">
          {storageModel}
        </div>
      </td>
      <td>
        <div style="width:100%; text-align:center;">
          {distributionType}
        </div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">
          {numFormatter.format(tableDetails.getRowCount)}
        </div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">
          {Utils.bytesToString(tableDetails.getSizeInMemory)}
        </div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">
          {Utils.bytesToString(tableDetails.getTotalSize)}
        </div>
      </td>
    </tr>

  }

  private def indexRow(indexDetails: SnappyIndexStats): Seq[Node] = {

    val numFormatter = java.text.NumberFormat.getIntegerInstance
    <tr>
      <td>
        <div style="width:100%; padding-left:10px;">
          {indexDetails.getIndexName}
        </div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">
          {numFormatter.format(indexDetails.getRowCount)}
        </div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">
          {Utils.bytesToString(indexDetails.getSizeInMemory)}
        </div>
      </td>
    </tr>
  }


  def makeProgressBar(completed: Double): Seq[Node] = {
    val completeWidth = "width: %s%%".format(completed)

    <div style="width:100%;">
      <div style="float: left; width: 75%;">
        <div class="progressBar">
          <div class="completedProgress" style={completeWidth}>&nbsp;</div>
        </div>
      </div>
      <div class="progressValue">{completed}%</div>
    </div>
  }
}

object SnappyDashboardPage{
  val pageHeaderText = "SnappyData Dashboard"

  object Status {
    val normal = "Normal"
    val warning = "Warning"
    val error = "Error"
    val severe = "Severe"
  }

  val ValueNotApplicable = "N/A"

  val clusterStatsTitle = "Cluster"
  val clusterStatsTitleTooltip = "SnappyData Clusters Summary"
  val clusterStats = scala.collection.mutable.HashMap.empty[String, Any]
  clusterStats += ("status" -> "Cluster Status")
  clusterStats += ("members" -> "Members")
  clusterStats += ("servers" -> "Data Servers")
  clusterStats += ("leads" -> "Leads")
  clusterStats += ("locators" -> "Locators")
  clusterStats += ("clients" -> "Connections")
  clusterStats += ("tables" -> "Tables")
  clusterStats += ("cpuUsage" -> "CPU Usage")
  clusterStats += ("cpuUsageTooltip" -> "Clusters CPU Usage")
  clusterStats += ("memoryUsage" -> "Memory Usage")
  clusterStats += ("memoryUsageTooltip" -> "Clusters Total Memory Usage")
  clusterStats += ("heapUsage" -> "Heap Usage")
  clusterStats += ("heapUsageTooltip" -> "Clusters Total Heap Usage")
  clusterStats += ("offHeapUsage" -> "Off-Heap Usage")
  clusterStats += ("offHeapUsageTooltip" -> "Clusters Total Off-Heap Usage")
  clusterStats += ("jvmHeapUsage" -> "JVM Heap Usage")
  clusterStats += ("jvmHeapUsageTooltip" -> "Clusters Total JVM Heap Usage")

  val membersStatsTitle = "Members"
  val membersStatsTitleTooltip = "SnappyData Members Summary"
  val memberStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  memberStatsColumn += ("status" -> "Status")
  memberStatsColumn += ("statusTooltip" -> "Members Status")
  memberStatsColumn += ("id" -> "Id")
  memberStatsColumn += ("idTooltip" -> "Members unique Identifier")
  memberStatsColumn += ("name" -> "Name")
  memberStatsColumn += ("nameTooltip" -> "Members Name")
  memberStatsColumn += ("nameOrId" -> "Member")
  memberStatsColumn += ("nameOrIdTooltip" -> "Members Name/Id")
  memberStatsColumn += ("description" -> "Member")
  memberStatsColumn += ("descriptionTooltip" -> "Members Description")
  memberStatsColumn += ("host" -> "Host")
  memberStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  memberStatsColumn += ("cpuUsage" -> "CPU Usage")
  memberStatsColumn += ("cpuUsageTooltip" -> "CPU used by Member Host")
  memberStatsColumn += ("memoryUsage" -> "Memory Usage")
  memberStatsColumn += ("memoryUsageTooltip" -> "Memory(Heap + Off-Heap) used by Member")
  memberStatsColumn += ("usedMemory" -> "Used Memory")
  memberStatsColumn += ("usedMemoryTooltip" -> "Used Memory")
  memberStatsColumn += ("totalMemory" -> "Total Memory")
  memberStatsColumn += ("totalMemoryTooltip" -> "Total Memory")
  memberStatsColumn += ("clients" -> "Connections")
  memberStatsColumn += ("clientsTooltip" -> "Number of JDBC connections to Member")
  memberStatsColumn += ("memberType" -> "Type")
  memberStatsColumn += ("memberTypeTooltip" -> "Member is Lead / Locator / Data Server")
  memberStatsColumn += ("lead" -> "Lead")
  memberStatsColumn += ("leadTooltip" -> "Member is Lead")
  memberStatsColumn += ("locator" -> "Locator")
  memberStatsColumn += ("locatorTooltip" -> "Member is Locator")
  memberStatsColumn += ("server" -> "Server")
  memberStatsColumn += ("serverTooltip" -> "Member is Server")
  memberStatsColumn += ("storageMemoryUsed" -> "StorageUsed")
  memberStatsColumn += ("storageMemoryToolTip" -> "Total storage pool memory used")
  memberStatsColumn += ("storageMemoryPoolSize" -> "StoragePoolSize")
  memberStatsColumn += ("storageMemorySizeToolTip" -> "Max storage pool memory size")
  memberStatsColumn += ("executionMemoryUsed" -> "ExecutionUsed")
  memberStatsColumn += ("executionMemoryToolTip" -> "Total execution pool memory used")
  memberStatsColumn += ("executionMemoryPoolSize" -> "ExecutionPoolSize")
  memberStatsColumn += ("executionMemorySizeToolTip" -> "Max execution pool memory size")
  memberStatsColumn += ("heapMemory" -> "Heap Memory (Used / Total)")
  memberStatsColumn += ("heapMemoryTooltip" -> "Members used and total Heap Memory")
  memberStatsColumn += ("offHeapMemory" -> "Off-Heap Memory (Used / Total)")
  memberStatsColumn += ("offHeapMemoryTooltip" -> "Members used and total Off Heap Memory")
  memberStatsColumn += ("jvmHeapMemory" -> "JVM Heap (Used / Total)")
  memberStatsColumn += ("jvmHeapMemoryTooltip" -> "Members used and total JVM Heap")

  val sparkConnectorsStatsTitle = "Spark Connectors"
  val sparkConnectorsStatsTitleTooltip = "Spark Connectors Summary"
  val sparkConnectorsStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  sparkConnectorsStatsColumn += ("id" -> "Id")
  sparkConnectorsStatsColumn += ("idTooltip" -> "Spark Connectors unique Identifier")
  sparkConnectorsStatsColumn += ("name" -> "Name")
  sparkConnectorsStatsColumn += ("nameTooltip" -> "Connector Name")
  sparkConnectorsStatsColumn += ("nameOrId" -> "Member")
  sparkConnectorsStatsColumn += ("nameOrIdTooltip" -> "Connector Name/Id")
  sparkConnectorsStatsColumn += ("host" -> "Host")
  sparkConnectorsStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  sparkConnectorsStatsColumn += ("cpuUsage" -> "CPU Usage")
  sparkConnectorsStatsColumn += ("cpuUsageTooltip" -> "CPU used by Connector")
  sparkConnectorsStatsColumn += ("memoryUsage" -> "Memory Usage")
  sparkConnectorsStatsColumn += ("memoryUsageTooltip" -> "Memory used by Connector")
  sparkConnectorsStatsColumn += ("usedMemory" -> "Used Memory")
  sparkConnectorsStatsColumn += ("usedMemoryTooltip" -> "Used Memory")
  sparkConnectorsStatsColumn += ("totalMemory" -> "Total Memory")
  sparkConnectorsStatsColumn += ("totalMemoryTooltip" -> "Total Memory")

  val tablesStatsTitle = "Tables"
  val tablesStatsTitleTooltip = "SnappyData Tables Summary"
  val tableStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  tableStatsColumn += ("id" -> "Id")
  tableStatsColumn += ("idTooltip" -> "Tables unique Identifier")
  tableStatsColumn += ("name" -> "Name")
  tableStatsColumn += ("nameTooltip" -> "Tables Name")
  tableStatsColumn += ("storageModel" -> "Storage Model")
  tableStatsColumn += ("storageModelTooltip" -> "Storage Model is either COLUMN or ROW ")
  tableStatsColumn += ("distributionType" -> "Distribution Type")
  tableStatsColumn += ("distributionTypeTooltip" -> "Distribution Type is either PARTITIONED or REPLICATED table ")
  tableStatsColumn += ("rowCount" -> "Row Count")
  tableStatsColumn += ("rowCountTooltip" -> "Total Rows in Table")
  tableStatsColumn += ("sizeInMemory" -> "Memory Size")
  tableStatsColumn += ("sizeInMemoryTooltip" -> "Tables Size in Memory")
  tableStatsColumn += ("totalSize" -> "Total Size")
  tableStatsColumn += ("totalSizeTooltip" -> "Tables Total Size (In Memory size + Disk Overflow Size)")

  val indexStatsTitle = "Indexes"
  val indexStatsTitleTooltip = "SnappyData Index Summary"
  val indexStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  indexStatsColumn += ("id" -> "Id")
  indexStatsColumn += ("idTooltip" -> "Index unique Identifier")
  indexStatsColumn += ("name" -> "Name")
  indexStatsColumn += ("nameTooltip" -> "Index Name")
  indexStatsColumn += ("rowCount" -> "Row Count")
  indexStatsColumn += ("rowCountTooltip" -> "Total Rows in Index")
  indexStatsColumn += ("totalSize" -> "Total Size")
  indexStatsColumn += ("totalSizeTooltip" -> "Total Size of Index")

}
