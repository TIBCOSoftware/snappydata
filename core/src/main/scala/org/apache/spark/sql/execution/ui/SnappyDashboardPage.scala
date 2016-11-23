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
import scala.xml.{NodeBuffer, Node}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}

/**
 * Created by skapse on 18/10/16.
 */
class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText : String  = SnappyDashboardPage.pageHeaderText

    val clusterStatsMap = scala.collection.mutable.HashMap.empty[String, Any]
    clusterStatsMap += ("status" -> "normal")
    clusterStatsMap += ("numMembers" -> 6)
    clusterStatsMap += ("numLeads" -> 1)
    clusterStatsMap += ("numLocators" -> 1)
    clusterStatsMap += ("numTables" -> 8)
    clusterStatsMap += ("numServers" -> 4)
    clusterStatsMap += ("numClients" -> 3)
    clusterStatsMap += ("memoryUsage" -> "12.3 GB (76%)")

    val membersBuf = scala.collection.mutable.ArrayBuffer.empty[mutable.Map[String, Any]]

    for(i <- 0 to 5) {
      val map = scala.collection.mutable.HashMap.empty[String, Any]
      map += ("status" -> "Normal")
      map += ("id" -> ("snap-cl-1-1011-" + i))
      map += ("name" -> ("snap-cl-1-1011-" + i))
      map += ("host" -> "snap-cl-1-1011")
      map += ("cpuUsage" -> 72)
      map += ("memoryUsage" -> 64)
      map += ("clients" -> 2)

      membersBuf += map
    }

    val tablesBuf = scala.collection.mutable.ArrayBuffer.empty[mutable.Map[String, Any]]

    for(i <- 0 to 5) {
      val map = scala.collection.mutable.HashMap.empty[String, Any]

      map += ("name" -> ("table-" + i))
      map += ("type" -> "PARTITION")
      map += ("rowCount" -> (72 + i))
      map += ("size" -> 64)

      tablesBuf += map
    }



    val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle, SnappyDashboardPage.clusterStatsTitleTooltip)

    val clusterDetails = clusterStats(clusterStatsMap)

    val keyStatsSection = clustersStatsTitle ++ clusterDetails

    val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle, SnappyDashboardPage.membersStatsTitleTooltip)

    val membersStatsTable = {
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
                <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("nameTooltip")} style="font-size: 17px;">
                  {SnappyDashboardPage.memberStatsColumn("name")}
                </span>
              </th>
              <th style="text-align:center;">
                <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("hostTooltip")} style="font-size: 17px;">
                  {SnappyDashboardPage.memberStatsColumn("host")}
                </span>
              </th>
              <th style="text-align:center; width: 300px;">
                <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("cpuUsageTooltip")} style="font-size: 17px;">
                  {SnappyDashboardPage.memberStatsColumn("cpuUsage")}
                </span>
              </th>
              <th style="text-align:center; width: 300px;">
                <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("memoryUsageTooltip")} style="font-size: 17px;">
                  {SnappyDashboardPage.memberStatsColumn("memoryUsage")}
                </span>
              </th>
              <th style="text-align:center; width: 150px;">
                <span data-toggle="tooltip" title="" data-original-title={SnappyDashboardPage.memberStatsColumn("clientsTooltip")} style="font-size: 17px;">
                  {SnappyDashboardPage.memberStatsColumn("clients")}
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

    val membersStatsDetails = membersStatsTitle ++ membersStatsTable

    val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle, SnappyDashboardPage.tablesStatsTitleTooltip)

    val tablesStatsTable ={
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
            {tablesBuf.map(tableRow(_))}
          </tbody>
        </table>
      </div>
    }

    val tablesStatsDetails = tablesStatsTitle ++ tablesStatsTable

    val pageContent = keyStatsSection ++ membersStatsDetails ++ tablesStatsDetails

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500))

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
    val statusImgUri = if(status.equals("error")) {
      "/static/snappydata/cluster-status-error-62x90.png"
    } else if(status.equals("warning")) {
      "/static/snappydata/cluster-status-warning-62x90.png"
    } else {
      "/static/snappydata/cluster-status-normal-62x90.png"
    }

    <div>
      <div class="keyStatesLeft" style="width: 300px; max-width: 300px;">
        <div class="clusterHealthImageBox">
          <img style="padding-left:12px; padding-top: 5px;" src={statusImgUri} />
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
        <div class="keyStatsValue">{clusterDetails.getOrElse("numClients","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("clients")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue">{clusterDetails.getOrElse("numTables","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("tables")}</div>
      </div>
      <div class="keyStatesRight">
        <div class="keyStatsValue">{clusterDetails.getOrElse("memoryUsage","NA")}</div>
        <div class="keyStatesText">{SnappyDashboardPage.clusterStats("memoryUsage")}</div>
      </div>
    </div>
  }

  private def memberRow(memberDetails:mutable.Map[String, Any]): Seq[Node] = {

    val status = memberDetails.getOrElse("status","")
    val statusImgUri = if(status.equals("error")) {
      "/static/snappydata/cluster-status-error-16x23.png"
    } else if(status.equals("warning")) {
      "/static/snappydata/cluster-status-warning-16x23.png"
    } else {
      "/static/snappydata/cluster-status-normal-16x23.png"
    }

    <tr>
      <td>
        <div style="float: left; border-right: thin inset; height: 24px; padding: 0 5px;">
          <img src={statusImgUri} />
        </div><div style="float: left; height: 24px; padding-left: 15px; "><b>{memberDetails.getOrElse("status","NA")}</b></div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{memberDetails.getOrElse("name","NA")}</div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{memberDetails.getOrElse("host","NA")}</div>
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("cpuUsage",0).asInstanceOf[Int])}
      </td>
      <td>
        {makeProgressBar(memberDetails.getOrElse("memoryUsage",0).asInstanceOf[Int])}
      </td>
      <td>
        <div style="text-align:right; padding-right:15px;">{memberDetails.getOrElse("clients","NA")}</div>
      </td>
    </tr>
  }

  private def tableRow(tableDetails:mutable.Map[String, Any]): Seq[Node] = {
    <tr>
      <td>
        <div style="width:100%; padding-left:10px;"><b>{tableDetails.getOrElse("name","NA")}</b></div>
      </td>
      <td>
        <div style="width:100%; padding-left:10px;">{tableDetails.getOrElse("type","NA")}</div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">{tableDetails.getOrElse("rowCount","NA")}</div>
      </td>
      <td>
        <div style="padding-right:10px; text-align:right;">{tableDetails.getOrElse("size","NA")}</div>
      </td>
    </tr>
  }


  def makeProgressBar(completed: Int): Seq[Node] = {
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
  memberStatsColumn += ("host" -> "Host")
  memberStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  memberStatsColumn += ("cpuUsage" -> "CPU Usage")
  memberStatsColumn += ("cpuUsageTooltip" -> "CPU used by Member")
  memberStatsColumn += ("memoryUsage" -> "Memory Usage")
  memberStatsColumn += ("memoryUsageTooltip" -> "Memory used by Member")
  memberStatsColumn += ("clients" -> "Clients")
  memberStatsColumn += ("clientsTooltip" -> "Number of Clients connected to Member")

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

}