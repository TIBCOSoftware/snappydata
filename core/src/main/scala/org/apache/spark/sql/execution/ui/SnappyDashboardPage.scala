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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.{NodeBuffer, Node}
import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SnappyContext
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * Created by skapse on 18/10/16.
 */
class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText : String  = SnappyDashboardPage.pageHeaderText

    var clusterStatsMap = scala.collection.mutable.HashMap.empty[String, Any]
    clusterStatsMap += ("status" -> SnappyDashboardPage.status.normal)
    clusterStatsMap += ("numMembers" -> 0)
    clusterStatsMap += ("numLeads" -> 0)
    clusterStatsMap += ("numLocators" -> 0)
    clusterStatsMap += ("numTables" -> 0)
    clusterStatsMap += ("numServers" -> 0)
    clusterStatsMap += ("numClients" -> 0)
    clusterStatsMap += ("memoryUsage" -> 0)

    //val membersBuf = SnappyTableStatsProviderService.getAggregatedMemberStatsOnDemand
    val membersBuf = SnappyTableStatsProviderService.getMembersStatsFromService

    val tablesBuf = SnappyTableStatsProviderService.getAggregatedTableStatsOnDemand(SnappyContext.globalSparkContext)

    updateClusterStats(clusterStatsMap, membersBuf, tablesBuf)

    // Generate Pages HTML
    val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle, SnappyDashboardPage.clusterStatsTitleTooltip)

    val clusterDetails = clusterStats(clusterStatsMap)

    val keyStatsSection = clustersStatsTitle ++ clusterDetails

    val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle, SnappyDashboardPage.membersStatsTitleTooltip)

    val membersStatsTable = memberStats(membersBuf)

    val membersStatsDetails = membersStatsTitle ++ membersStatsTable

    val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle, SnappyDashboardPage.tablesStatsTitleTooltip)

    val tablesStatsTable = tableStats(tablesBuf)

    val tablesStatsDetails = tablesStatsTitle ++ tablesStatsTable

    val pageContent = keyStatsSection ++ membersStatsDetails ++ tablesStatsDetails

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500))

  }

  private def updateClusterStats(
      clusterStatsMap: mutable.HashMap[String, Any],
      membersBuf: mutable.ArrayBuffer[mutable.Map[String, Any]],
      tablesBuf: Map[String, SnappyRegionStats] ) : Unit = {

    var numLead = 0
    var numLocator = 0
    var numServers = 0
    var numClients = 0
    var numClientsToLocator = 0
    var numClientsToDataServers = 0

    membersBuf.foreach(m => {
      if(m("lead").toString.toBoolean){
        numLead += 1
      }
      if(m("locator").toString.toBoolean){
        numLocator += 1
        numClientsToLocator = m("clients").toString.toInt
      }
      if(m("cacheServer").toString.toBoolean){
        numServers += 1
      }

      numClients += m("clients").toString.toInt

    })

    numClientsToDataServers = numClients - numClientsToLocator

    clusterStatsMap += ("numMembers" -> membersBuf.size)
    clusterStatsMap += ("numTables" -> tablesBuf.size)
    clusterStatsMap += ("numLeads" -> numLead)
    clusterStatsMap += ("numLocators" -> numLocator)
    clusterStatsMap += ("numServers" -> numServers)
    clusterStatsMap += ("numClients" -> numClients)
    clusterStatsMap += ("numClientsToLocator" -> numClientsToLocator)
    clusterStatsMap += ("numClientsToDataServers" -> numClientsToDataServers)

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

    val status = clusterDetails.getOrElse("status","")
    val statusImgUri = if(status.equals("severe")) {
      "/static/snappydata/severe-status-70x68.png"
    } else if(status.equals("error")) {
      "/static/snappydata/error-status-70x68.png"
    } else if(status.equals("warning")) {
      "/static/snappydata/warning-status-70x68.png"
    } else {
      "/static/snappydata/normal-status-70x68.png"
    }

    <div>
      <div class="keyStatesLeft" style="width: 300px; max-width: 300px;">
        <div class="clusterHealthImageBox">
          <img style="padding-left:10px; padding-top: 15px;" src={statusImgUri} />
        </div>
        <div class="clusterHealthTextBox">
          <div class="keyStatsValue statusTextNormal">{status}</div>
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
        <div class="keyStatsValue">{clusterDetails.getOrElse("numServers","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("servers")}</div>
      </div>
      <!-- <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numClients","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("clients")}</div>
      </div> -->
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

  private def memberStats(membersBuf: mutable.ArrayBuffer[mutable.Map[String, Any]]): Seq[Node] = {

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
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("hostTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("host")}
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
            <th style="text-align:center; width: 100px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("clientsTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("clients")}
              </span>
            </th>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("leadTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("lead")}
              </span>
            </th>
            <th style="text-align:center;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("locatorTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("locator")}
              </span>
            </th>
            <th style="text-align:center; ">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("serverTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("server")}
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {membersBuf.map(memberRow(_))}
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
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("typeTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("type")}
              </span>
            </th>
            <th style="text-align:center; width: 300px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("rowCountTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("rowCount")}
              </span>
            </th>
            <th style="text-align:center; width: 300px;">
              <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.tableStatsColumn("sizeTooltip")} style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("size")}
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
    val statusImgUri = if(status.equals("severe")) {
      "/static/snappydata/severe-status-20x19.png"
    } else if(status.equals("error")) {
      "/static/snappydata/error-status-20x19.png"
    } else if(status.equals("warning")) {
      "/static/snappydata/warning-status-20x19.png"
    } else {
      "/static/snappydata/normal-status-20x19.png"
    }

    val nameOrId = {
      if(memberDetails.getOrElse("name","NA").equals("NA")
          || memberDetails.getOrElse("name","NA").equals("")){
        memberDetails.getOrElse("id","NA")
      }else{
        memberDetails.getOrElse("name","NA")
      }
    }

    <tr>
      <td>
        <div style="float: left; border-right: thin inset; height: 24px; padding: 0 5px;">
          <img src={statusImgUri} />
        </div><div style="float: left; height: 24px; padding-left: 15px; "><b>{memberDetails.getOrElse("status","NA")}</b></div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{nameOrId}</div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{memberDetails.getOrElse("host","NA")}</div>
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("cpuUsage",0.toDouble).asInstanceOf[Double])}
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("memoryUsage",0.toDouble).asInstanceOf[Double])}
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{memberDetails.getOrElse("clients","NA")}</div>
      </td>
      <td>
        <div style="text-align:center;">{SnappyDashboardPage.booleanToEnglish(memberDetails.getOrElse("lead",false).toString.toBoolean)}</div>
      </td>
      <td>
        <div style="text-align:center;">{SnappyDashboardPage.booleanToEnglish(memberDetails.getOrElse("locator",false).toString.toBoolean)}</div>
      </td>
      <td>
        <div style="text-align:center;">{SnappyDashboardPage.booleanToEnglish(memberDetails.getOrElse("cacheServer",false).toString.toBoolean)}</div>
      </td>
    </tr>
  }

  //private def tableRow(tableDetails:mutable.Map[String, Any]): Seq[Node] = {
  private def tableRow(tableDetails: SnappyRegionStats): Seq[Node] = {

    val numFormatter = java.text.NumberFormat.getIntegerInstance
    val columnTable = if (tableDetails.isColumnTable) " COLUMN " else " ROW "

    <tr>
      <td>
        <div style="width:100%; padding-left:10px;">
          {tableDetails.getRegionName}
        </div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">
          {columnTable}
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
  }

  val clusterStatsTitle = "Cluster"
  val clusterStatsTitleTooltip = "SnappyData Clusters Summary"
  val clusterStats = scala.collection.mutable.HashMap.empty[String, Any]
  clusterStats += ("status" -> "Cluster Status")
  clusterStats += ("members" -> "Members")
  clusterStats += ("servers" -> "Servers")
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
  memberStatsColumn += ("nameOrId" -> "Name/Id")
  memberStatsColumn += ("nameOrIdTooltip" -> "Members Name/Id")
  memberStatsColumn += ("host" -> "Host")
  memberStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  memberStatsColumn += ("cpuUsage" -> "CPU Usage")
  memberStatsColumn += ("cpuUsageTooltip" -> "CPU used by Member")
  memberStatsColumn += ("memoryUsage" -> "Memory Usage")
  memberStatsColumn += ("memoryUsageTooltip" -> "Memory used by Member")
  memberStatsColumn += ("clients" -> "Clients")
  memberStatsColumn += ("clientsTooltip" -> "Number of Clients connected to Member")
  memberStatsColumn += ("lead" -> "Lead")
  memberStatsColumn += ("leadTooltip" -> "Member is Lead")
  memberStatsColumn += ("locator" -> "Locator")
  memberStatsColumn += ("locatorTooltip" -> "Member is Locator")
  memberStatsColumn += ("server" -> "Server")
  memberStatsColumn += ("serverTooltip" -> "Member is Server")

  val tablesStatsTitle = "Tables"
  val tablesStatsTitleTooltip = "SnappyData Tables Summary"
  val tableStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  tableStatsColumn += ("id" -> "Id")
  tableStatsColumn += ("idTooltip" -> "Tables unique Identifier")
  tableStatsColumn += ("name" -> "Name")
  tableStatsColumn += ("nameTooltip" -> "Tables Name")
  tableStatsColumn += ("type" -> "Type")
  tableStatsColumn += ("typeTooltip" -> "Tables Type")
  tableStatsColumn += ("rowCount" -> "Row Count")
  tableStatsColumn += ("rowCountTooltip" -> "Total Rows in Table")
  tableStatsColumn += ("size" -> "Size")
  tableStatsColumn += ("sizeTooltip" -> "Tables Size")



  def booleanToEnglish(value: Boolean): String = {
    if(value)
      "YES"
    else
      "NO"
  }
}