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

import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyRegionStats}
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging

private[ui] class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText: String = SnappyDashboardPage.pageHeaderText

    /* // Commenting for now, as not being used
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

        if (!m._2("status").toString.equalsIgnoreCase("stopped")) {
          sparkConnectors += m
        }

      } else {
        clusterMembers += m
      }
    })

    val (tableBuff, indexBuff, externalTableBuff) =
      SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand

    updateClusterStats(clusterStatsMap, clusterMembers, tableBuff, externalTableBuff)
    */

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    val clusterStatsDetails = {
      val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle,
                                 SnappyDashboardPage.clusterStatsTitleTooltip)
      val clusterDetails = clusterStats

      clustersStatsTitle ++ clusterDetails
    }

    val membersStatsDetails = {
      val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle,
                                SnappyDashboardPage.membersStatsTitleTooltip)
      val membersStatsTable = memberStats

      membersStatsTitle ++ membersStatsTable
    }

    val tablesStatsDetails = {
      val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle,
                                SnappyDashboardPage.tablesStatsTitleTooltip)
      val tablesStatsTable = tableStats

      tablesStatsTitle ++ tablesStatsTable
    }

    val extTablesStatsDetails = {
      val extTablesStatsTitle = createTitleNode(SnappyDashboardPage.extTablesStatsTitle,
        SnappyDashboardPage.extTablesStatsTitleTooltip)
      val extTablesStatsTable = extTableStats

      extTablesStatsTitle ++ extTablesStatsTable
    }

    val jsScripts = <script src={
                              UIUtils.prependBaseUri("/static/snappydata/snappy-dashboard.js")
                            }></script>

    val pageContent = jsScripts ++ pageTitleNode ++ clusterStatsDetails ++ membersStatsDetails ++
                      tablesStatsDetails ++ extTablesStatsDetails

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500),
      useDataTables = true, isSnappyPage = true)

  }

  // todo: to be removed as not being used anymore
  private def updateClusterStats(
      clusterStatsMap: mutable.HashMap[String, Any],
      membersBuf: mutable.Map[String, mutable.Map[String, Any]],
      tablesBuf: Map[String, SnappyRegionStats],
      extTablesBuf: Map[String, SnappyExternalTableStats]): Unit = {

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
    var cpuUsage: Double = 0
    var memoryUsage: Double = 0;
    var heapUsage: Double = 0;
    var offHeapUsage: Double = 0;
    var jvmHeapUsage: Double = 0;

    var totalCPUSize: Int = 0;
    var totalCPUUsage: Int = 0;
    var totalMemorySize: Long = 0;
    var totalMemoryUsage: Long = 0;
    var totalHeapSize: Long = 0;
    var totalHeapUsage: Long = 0;
    var totalOffHeapSize: Long = 0;
    var totalOffHeapUsage: Long = 0;
    var totalJvmHeapSize: Long = 0;
    var totalJvmHeapUsage: Long = 0;

    val hostsList: mutable.Set[String] = mutable.Set.empty[String]

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

      if(totalMemorySize > 0) {
        memoryUsage = totalMemoryUsage * 100 / totalMemorySize
      }
      if(totalHeapSize > 0) {
        heapUsage = totalHeapUsage * 100 / totalHeapSize
      }
      if(totalOffHeapSize > 0) {
        offHeapUsage = totalOffHeapUsage * 100 / totalOffHeapSize
      }
      if(totalJvmHeapSize > 0) {
        jvmHeapUsage = totalJvmHeapUsage * 100 / totalJvmHeapSize
      }

      cpuUsage = totalCPUUsage / totalCPUSize
    }

    numClientsToDataServers = numClients - numClientsToLocator

    tablesBuf.foreach(tb => {
      val tbl = tb._2

      if(tbl.isColumnTable) {
        numColumnTables += 1
      }
      else {
        numRowTables += 1
      }
    })

    clusterStatsMap += ("status" -> { if (isClusterStateNormal) { "Normal" } else { "Warning" }})
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
    clusterStatsMap += ("numExtTables" -> extTablesBuf.size)
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

  private def clusterStats(): Seq[Node] = {
    <div class="container-fluid">
      <div id="cpuUsageContainer"
           style="width: 350px; height: 200px; float:left; margin: 10px 20px;
             border: solid 1px darkgray; box-shadow: 5px 5px 5px grey;">
      </div>
      <div id="heapUsageContainer"
           style="width: 350px; height: 200px; float:left; margin: 10px 20px;
             border: solid 1px darkgray; box-shadow: 5px 5px 5px grey;">
      </div>
      <div id="offheapUsageContainer"
           style="width: 350px; height: 200px; float:left; margin: 10px 20px;
             border: solid 1px darkgray; box-shadow: 5px 5px 5px grey;">
      </div>
      <div id="getsputsContainer"
           style="width: 350px; height: 200px; float:left; margin: 10px 20px;
             border: solid 1px darkgray; box-shadow: 5px 5px 5px grey;">
      </div>
    </div>
  }

  private def memberStats(): Seq[Node] = {
    <div class="container-fluid">
      <table id="memberStatsGrid" class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; vertical-align: middle; width: 60px;">
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
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("cpuUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("cpuUsage")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("memoryUsageTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.memberStatsColumn("memoryUsage")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("heapMemoryTooltip")
                    }
                    style="font-size: 17px;">
                Heap Memory<br/>(Used / Total)
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
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
      </table>
    </div>
  }

  private def tableStats(): Seq[Node] = {
    <div class="container-fluid">
      <table id="tableStatsGrid" class="table table-bordered table-condensed table-striped">
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
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("rowCountTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("rowCount")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("sizeInMemoryTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("sizeInMemory")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("totalSizeTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("totalSize")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("bucketCountTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.tableStatsColumn("bucketCount")}
              </span>
            </th>
          </tr>
        </thead>
      </table>
    </div>
  }

  private def extTableStats(): Seq[Node] = {

    <div class="container-fluid">
      <table id="extTableStatsGrid" class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th style="text-align:center; vertical-align: middle; width:300px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("nameTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.extTableStatsColumn("name")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle; width:300px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("providerTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.extTableStatsColumn("provider")}
              </span>
            </th>
            <th style="text-align:center; vertical-align: middle;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("externalSourceTooltip")
                    }
                    style="font-size: 17px;">
                {SnappyDashboardPage.extTableStatsColumn("externalSource")}
              </span>
            </th>
          </tr>
        </thead>
      </table>
    </div>
  }

}

object SnappyDashboardPage {
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
  tableStatsColumn += ("distributionTypeTooltip" ->
      "Distribution Type is either PARTITIONED or REPLICATED table ")
  tableStatsColumn += ("rowCount" -> "Row Count")
  tableStatsColumn += ("rowCountTooltip" -> "Total Rows in Table")
  tableStatsColumn += ("sizeInMemory" -> "Memory Size")
  tableStatsColumn += ("sizeInMemoryTooltip" -> "Tables Size in Memory")
  tableStatsColumn += ("totalSize" -> "Total Size")
  tableStatsColumn += ("totalSizeTooltip" ->
      "Tables Total Size (In Memory size + Disk Overflow Size)")
  tableStatsColumn += ("bucketCount" -> "Buckets")
  tableStatsColumn += ("bucketCountTooltip" -> "Number of Buckets in Table")

  val extTablesStatsTitle = "External Tables"
  val extTablesStatsTitleTooltip = "SnappyData ExternalTables Summary"
  val extTableStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  extTableStatsColumn += ("id" -> "Id")
  extTableStatsColumn += ("idTooltip" -> "External Tables unique Identifier")
  extTableStatsColumn += ("name" -> "Name")
  extTableStatsColumn += ("nameTooltip" -> "External Tables Name")
  extTableStatsColumn += ("type" -> "Type")
  extTableStatsColumn += ("typeTooltip" -> "External Tables Type")
  extTableStatsColumn += ("provider" -> "Provider")
  extTableStatsColumn += ("providerTooltip" -> "External Tables Provider")
  extTableStatsColumn += ("externalSource" -> "Source")
  extTableStatsColumn += ("externalSourceTooltip" -> "External Source of Tables ")

}
