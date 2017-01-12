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

package org.apache.spark.ui

import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.Node

import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[ui] class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText : String  = SnappyDashboardPage.pageHeaderText

    var clusterStatsMap = scala.collection.mutable.HashMap.empty[String, Any]
    clusterStatsMap += ("status" -> SnappyDashboardPage.status.normal)
    clusterStatsMap += ("numMembers" -> 0)
    clusterStatsMap += ("numLeads" -> 0)
    clusterStatsMap += ("numLocators" -> 0)
    clusterStatsMap += ("numTables" -> 0)
    clusterStatsMap += ("numDataServers" -> 0)
    clusterStatsMap += ("numClients" -> 0)
    clusterStatsMap += ("memoryUsage" -> 0)

    val allMembers = SnappyTableStatsProviderService.getMembersStatsFromService

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

    val tablesBuf = SnappyTableStatsProviderService.getAggregatedTableStatsOnDemand

    updateClusterStats(clusterStatsMap, clusterMembers, tablesBuf)

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle, SnappyDashboardPage.clusterStatsTitleTooltip)

    val clusterDetails = clusterStats(clusterStatsMap)

    val keyStatsSection = clustersStatsTitle ++ clusterDetails

    val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle, SnappyDashboardPage.membersStatsTitleTooltip)

    val membersStatsTable = memberStats(clusterMembers)

    val membersStatsDetails = membersStatsTitle ++ membersStatsTable

    val sparkConnectorsStatsDetails = {
      val sparkConnectorsStatsTitle = createTitleNode(SnappyDashboardPage.sparkConnectorsStatsTitle, SnappyDashboardPage.sparkConnectorsStatsTitleTooltip)
      val sparkConnectorsStatsTable = connectorStats(sparkConnectors)

      if(sparkConnectors.size > 0)
        sparkConnectorsStatsTitle ++ sparkConnectorsStatsTable
      else
        mutable.Seq.empty[Node]
    }

    val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle, SnappyDashboardPage.tablesStatsTitleTooltip)

    val tablesStatsTable = tableStats(tablesBuf)

    val tablesStatsDetails = tablesStatsTitle ++ tablesStatsTable

    val pageContent = pageTitleNode ++ keyStatsSection ++ membersStatsDetails ++ sparkConnectorsStatsDetails ++ tablesStatsDetails

    UIUtils.simpleSparkPageWithTabs(pageHeaderText, pageContent, parent, Some(500))

  }

  private def updateClusterStats(
      clusterStatsMap: mutable.HashMap[String, Any],
      membersBuf: mutable.Map[String, mutable.Map[String, Any]],
      tablesBuf: Map[String, SnappyRegionStats] ) : Unit = {

    var isClusterStateNormal = true
    var numLead = 0
    var numLocator = 0
    var numDataServers = 0
    var numClients = 0
    var numClientsToLocator = 0
    var numClientsToDataServers = 0

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

    })

    numClientsToDataServers = numClients - numClientsToLocator

    clusterStatsMap += ("status" -> {if(isClusterStateNormal) "Normal" else "Warning"})
    clusterStatsMap += ("numMembers" -> membersBuf.size)
    clusterStatsMap += ("numTables" -> tablesBuf.size)
    clusterStatsMap += ("numLeads" -> numLead)
    clusterStatsMap += ("numLocators" -> numLocator)
    clusterStatsMap += ("numDataServers" -> numDataServers)
    clusterStatsMap += ("numClients" -> numClients)
    clusterStatsMap += ("numClientsToLocator" -> numClientsToLocator)
    clusterStatsMap += ("numClientsToDataServers" -> numClientsToDataServers)

  }

  private def createPageTitleNode(title:String): Seq[Node] = {

    val sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss")
    val lastUpdatedOn = sdf.format(new Date())

    <div class="row-fluid">
      <div class="span12">
        <h3 style="vertical-align: bottom; display: inline-block;">
          {title}
        </h3>
        <span style="float:right; font-size: 12px;" data-toggle="tooltip" title="" data-original-title="Reload page to refresh Dashboard." >Last updated on {lastUpdatedOn}</span>
      </div>
    </div>
  }

  private def createTitleNode(title:String, tooltip:String): Seq[Node] = {
    <div class="row-fluid">
      <div class="span12">
        <h4 style="vertical-align: bottom; display: inline-block;">
          {title}
          {UIUtils.tooltip(tooltip, "bottom")}
        </h4>
      </div>
    </div>
  }


  private def clusterStats(clusterDetails:mutable.Map[String, Any]): Seq[Node] = {

    val status = clusterDetails.getOrElse("status", "")
    val statusNode = {
      if (status.toString.equalsIgnoreCase("normal")) {
        <div class="keyStatsValue statusTextNormal">
          {status}
        </div>
      } else {
        <div class="keyStatsValue statusTextWarning">
          {status}
        </div>
      }
    }

    val statusImgUri = if(status.toString.equalsIgnoreCase("normal")) {
      "/static/snappydata/running-status-icon-70x68.png"
    } else {
      "/static/snappydata/warning-status-icon-70x68.png"
    }

    <div>
      <div class="keyStatesLeft" style="width: 300px; max-width: 300px;">
        <div class="clusterHealthImageBox">
          <img style="padding-left:10px; padding-top: 15px;" src={statusImgUri} />
        </div>
        <div class="clusterHealthTextBox">
          {statusNode}
          <div class="keyStatesText">{SnappyDashboardPage.clusterStats("status")}</div>
        </div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numMembers","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("members")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numLeads","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("leads")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numLocators","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("locators")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numDataServers","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("servers")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numTables","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("tables")}</div>
      </div>
      <div class="keyStatesRight">
        <div class="keyStatsValue" data-toggle="tooltip" title="" data-original-title={
        val numClientsToLocator = clusterDetails.getOrElse("numClientsToLocator",0).toString.toInt
        val numClientsToDataServers = clusterDetails.getOrElse("numClientsToDataServers",0).toString.toInt
        "Control Connections : " + numClientsToLocator + " Data Server Connections : " + numClientsToDataServers }>
          {clusterDetails.getOrElse("numClients","NA")}
        </div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("clients")}</div>
      </div>
      <!-- <div class="keyStatesRight">
        <div class="keyStatsValue">{clusterDetails.getOrElse("memoryUsage","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("memoryUsage")}</div>
      </div> -->
    </div>
  }

  private def memberStats(membersBuf: mutable.Map[String,mutable.Map[String, Any]]): Seq[Node] = {
    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; width: 150px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("statusTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("status")}
              </span>
            </th>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("nameOrIdTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("nameOrId")}
              </span>
            </th>
            <!-- <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("hostTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("host")}
              </span>
            </th> -->
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("memberTypeTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("memberType")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("cpuUsageTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("cpuUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("memoryUsageTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("memoryUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 150px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("usedMemoryTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("usedMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 150px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("totalMemoryTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("totalMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 100px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("clientsTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("clients")}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {membersBuf.map(mb => memberRow(mb._2))}
        </tbody>
      </table>
    </div>
  }

  private def connectorStats(sparkConnectors: mutable.Map[String,mutable.Map[String, Any]]): Seq[Node] = {
    <div>
      <table class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.sparkConnectorsStatsColumn("nameOrIdTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("nameOrId")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.sparkConnectorsStatsColumn("cpuUsageTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("cpuUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.sparkConnectorsStatsColumn("memoryUsageTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("memoryUsage")}
              </span>
            </th>
            <th style="text-align:center; width: 150px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.sparkConnectorsStatsColumn("usedMemoryTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.sparkConnectorsStatsColumn("usedMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 150px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.sparkConnectorsStatsColumn("totalMemoryTooltip")} style="font-size: 17px;">
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
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("nameTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("name")}
              </span>
            </th>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("storageModelTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("storageModel")}
              </span>
            </th>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("distributionTypeTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("distributionType")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("rowCountTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("rowCount")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("sizeInMemoryTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("sizeInMemory")}
              </span>
            </th>
            <th style="text-align:center; width: 250px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("totalSizeTooltip")} style="font-size: 17px;">
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

  private def memberRow(memberDetails:mutable.Map[String, Any]): Seq[Node] = {

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

    val totalMemory = memberDetails.getOrElse("totalMemory", 0).asInstanceOf[Long]
    val usedMemory = memberDetails.getOrElse("usedMemory",0).asInstanceOf[Long]
    val memoryUsage: Double = (usedMemory * 100) / totalMemory

    <tr>
      <td>
        <div style="float: left; border-right: thin inset; height: 24px; padding: 0 5px;">
          <img src={statusImgUri} />
        </div><div style="float: left; height: 24px; padding-left: 15px; "><b>{memberDetails.getOrElse("status","NA")}</b></div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{nameOrId}</div>
      </td>
      <!-- <td>
        <div style="width:100%; padding-left:10px;">{memberDetails.getOrElse("host","NA")}</div>
      </td> -->
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
        <div style="text-align:right; padding-right:15px;">{Utils.bytesToString(usedMemory)}</div>
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{Utils.bytesToString(totalMemory).toString}</div>
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{memberDetails.getOrElse("clients","NA")}</div>
      </td>
    </tr>
  }

  private def connectorRow(memberDetails:mutable.Map[String, Any]): Seq[Node] = {

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
        <div style="text-align:right; padding-right:15px;">{Utils.bytesToString(usedMemory)}</div>
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{Utils.bytesToString(totalMemory).toString}</div>
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


  def makeProgressBar(completed: Double): Seq[Node] = {
    val completeWidth = "width: %s%%".format(completed)

    <div style="width:100%;">
      <div style="float: left; width: 80%;">
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

  object status {
    val normal = "Normal"
    val warning = "Warning"
    val error = "Error"
    val severe = "Severe"
  }

  val clusterStatsTitle = "Cluster"
  val clusterStatsTitleTooltip = "SnappyData Clusters Summary"
  val clusterStats = scala.collection.mutable.HashMap.empty[String, Any]
  clusterStats += ("status" -> "Cluster Status")
  clusterStats += ("members" -> "Members")
  clusterStats += ("servers" -> "Data Servers")
  clusterStats += ("leads" -> "Leads")
  clusterStats += ("locators" -> "Locators")
  clusterStats += ("clients" -> "Clients")
  clusterStats += ("tables" -> "Tables")
  clusterStats += ("memoryUsage" -> "Memory Usage")

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
  memberStatsColumn += ("host" -> "Host")
  memberStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  memberStatsColumn += ("cpuUsage" -> "CPU Usage")
  memberStatsColumn += ("cpuUsageTooltip" -> "CPU used by Member")
  memberStatsColumn += ("memoryUsage" -> "Memory Usage")
  memberStatsColumn += ("memoryUsageTooltip" -> "Memory used by Member")
  memberStatsColumn += ("usedMemory" -> "Used Memory")
  memberStatsColumn += ("usedMemoryTooltip" -> "Used Memory")
  memberStatsColumn += ("totalMemory" -> "Total Memory")
  memberStatsColumn += ("totalMemoryTooltip" -> "Total Memory")
  memberStatsColumn += ("clients" -> "Clients")
  memberStatsColumn += ("clientsTooltip" -> "Number of Clients connected to Member")
  memberStatsColumn += ("memberType" -> "Type")
  memberStatsColumn += ("memberTypeTooltip" -> "Member is Lead / Locator / Data Server")
  memberStatsColumn += ("lead" -> "Lead")
  memberStatsColumn += ("leadTooltip" -> "Member is Lead")
  memberStatsColumn += ("locator" -> "Locator")
  memberStatsColumn += ("locatorTooltip" -> "Member is Locator")
  memberStatsColumn += ("server" -> "Server")
  memberStatsColumn += ("serverTooltip" -> "Member is Server")

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
  tableStatsColumn += ("sizeInMemory" -> "In Memory Size")
  tableStatsColumn += ("sizeInMemoryTooltip" -> "Tables Size in Memory")
  tableStatsColumn += ("totalSize" -> "Total Size")
  tableStatsColumn += ("totalSizeTooltip" -> "Total Size of Tables")


}