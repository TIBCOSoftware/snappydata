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

import scala.collection.mutable.ArrayBuffer

import io.snappydata.gemxd.SnappyDataVersion
import scala.util.control.Breaks._

import org.apache.spark.internal.Logging
import org.apache.spark.ui.JettyUtils._

class SnappyDashboardTab(sparkUI: SparkUI) extends SparkUITab(sparkUI, "dashboard") with Logging {
  val parent = sparkUI
  // Attaching dashboard ui page and tab
  attachPage(new SnappyDashboardPage(this))
  parent.attachTab(this)

  // Move Dashboard tab to first place
  val tabsList = parent.getTabs
  val newTabsList = ArrayBuffer[WebUITab]()
  // Add dashboard first
  newTabsList += tabsList.last
  // Add remaining tabs in tabs list
  tabsList.foreach(tab => {
    if(!tab.prefix.equalsIgnoreCase("dashboard")){
      newTabsList += tab
    }
  })

  // Set updated tabs list
  parent.setTabs(newTabsList)

  // Set SnappyData Product Version in SparkUI
  SparkUI.setProductVersion(SnappyDataVersion.getSnappyDataProductVersion)

  updateRedirectionHandler

  // Replace default spark jobs page redirection handler by Snappy Dashboard page redirection handler
  def updateRedirectionHandler: Unit = {
    val handlers = parent.getHandlers
    breakable {
      handlers.foreach(h => {
        if (h.getContextPath.equals("/")) {
          // Detach DEFAULT JOBS page redirection handler
          parent.detachHandler(h)
          // Attach DASHBOARD page redirection handler
          parent.attachHandler(createRedirectHandler("/", "/dashboard/", basePath = basePath))
          break
        }
      })
    }
  }

}
