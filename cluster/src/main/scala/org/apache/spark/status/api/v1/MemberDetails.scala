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
package org.apache.spark.status.api.v1

import java.util.UUID

import scala.collection.mutable.ListBuffer

import io.snappydata.SnappyTableStatsProviderService

object MemberDetails {

  def getAllMembersInfo: Seq[MemberSummary] = {
    val allMembers = SnappyTableStatsProviderService.getService.getMembersStatsOnDemand
    val membersBuff: ListBuffer[MemberSummary] = ListBuffer.empty[MemberSummary]

    allMembers.foreach(mem => {
      val memberDetails = mem._2

      val status = memberDetails.getOrElse("status", "").toString
      /*
      val statusImgUri = if (status.toString.toLowerCase.equals("running")) {
        "/static/snappydata/running-status-icon-20x19.png"
      } else {
        "/static/snappydata/stopped-status-icon-20x19.png"
      }
      */

      val memberId = memberDetails.getOrElse("id", "NA").toString

      val nameOrId = {
        if (memberDetails.getOrElse("name", "NA").equals("NA")
            || memberDetails.getOrElse("name", "NA").equals("")) {
          memberDetails.getOrElse("id", "NA")
        } else {
          memberDetails.getOrElse("name", "NA")
        }
      }

      val host = memberDetails.getOrElse("host", "").toString
      val fullDirName = memberDetails.getOrElse("userDir", "").toString
      val shortDirName = fullDirName.substring(
        fullDirName.lastIndexOf(System.getProperty("file.separator")) + 1)
      val logFile = memberDetails.getOrElse("logFile", "").toString
      val processId = memberDetails.getOrElse("processId", "").toString

      // val distStoreUUID = memberDetails.get("diskStoreUUID").get.asInstanceOf[UUID]
      // val distStoreName = memberDetails.getOrElse("status", "").toString

      val isLead: Boolean = memberDetails.getOrElse("lead", false).toString.toBoolean
      val isActiveLead: Boolean = memberDetails.getOrElse("activeLead", false).toString.toBoolean
      val isLocator: Boolean = memberDetails.getOrElse("locator", false).toString.toBoolean
      val isDataServer: Boolean = memberDetails.getOrElse("dataServer", false).toString.toBoolean
      val memberType = {
        if (isLead || isActiveLead) {
          "LEAD"
        } else if (isLocator) {
          "LOCATOR"
        } else if (isDataServer) {
          "DATA SERVER"
        } else {
          "CONNECTOR"
        }
      }

      val cpuActive = memberDetails.getOrElse("cpuActive", 0).asInstanceOf[Integer]
      // val cpuActive = memberDetails("cpuActive").asInstanceOf[Int]
      val clients = memberDetails.getOrElse("clients", 0).asInstanceOf[Long]
      // val clients = memberDetails("clients").asInstanceOf[Long]

      val heapStoragePoolUsed =
        memberDetails.getOrElse("heapStoragePoolUsed", 0).asInstanceOf[Long]
      val heapStoragePoolSize =
        memberDetails.getOrElse("heapStoragePoolSize", 0).asInstanceOf[Long]
      val heapExecutionPoolUsed =
        memberDetails.getOrElse("heapExecutionPoolUsed", 0).asInstanceOf[Long]
      val heapExecutionPoolSize =
        memberDetails.getOrElse("heapExecutionPoolSize", 0).asInstanceOf[Long]

      val offHeapStoragePoolUsed =
        memberDetails.getOrElse("offHeapStoragePoolUsed", 0).asInstanceOf[Long]
      val offHeapStoragePoolSize =
        memberDetails.getOrElse("offHeapStoragePoolSize", 0).asInstanceOf[Long]
      val offHeapExecutionPoolUsed =
        memberDetails.getOrElse("offHeapExecutionPoolUsed", 0).asInstanceOf[Long]
      val offHeapExecutionPoolSize =
        memberDetails.getOrElse("offHeapExecutionPoolSize", 0).asInstanceOf[Long]

      val heapMemorySize = memberDetails.getOrElse("heapMemorySize", 0).asInstanceOf[Long]
      val heapMemoryUsed = memberDetails.getOrElse("heapMemoryUsed", 0).asInstanceOf[Long]
      val offHeapMemorySize = memberDetails.getOrElse("offHeapMemorySize", 0).asInstanceOf[Long]
      val offHeapMemoryUsed = memberDetails.getOrElse("offHeapMemoryUsed", 0).asInstanceOf[Long]


      val jvmHeapMax = memberDetails.getOrElse("maxMemory", 0).asInstanceOf[Long]
      val jvmHeapTotal = memberDetails.getOrElse("totalMemory", 0).asInstanceOf[Long]
      val jvmHeapUsed = memberDetails.getOrElse("usedMemory", 0).asInstanceOf[Long]
      val jvmHeapFree = memberDetails.getOrElse("freeMemory", 0).asInstanceOf[Long]

      membersBuff += new MemberSummary(memberId, nameOrId.toString, host, shortDirName, fullDirName,
        logFile, processId, status, memberType, isLocator, isDataServer, isLead, isActiveLead,
        cpuActive, clients, jvmHeapMax, jvmHeapUsed, jvmHeapTotal, jvmHeapFree,
        heapStoragePoolUsed, heapStoragePoolSize, heapExecutionPoolUsed, heapExecutionPoolSize,
        heapMemorySize, heapMemoryUsed, offHeapStoragePoolUsed, offHeapStoragePoolSize,
        offHeapExecutionPoolUsed, offHeapExecutionPoolSize, offHeapMemorySize, offHeapMemoryUsed)

    })

    membersBuff.toList
  }

}
