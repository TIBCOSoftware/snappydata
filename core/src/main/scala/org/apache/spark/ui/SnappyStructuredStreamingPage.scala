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

private[ui] class  SnappyStructuredStreamingPage (parent: SnappyStreamingTab)
    extends WebUIPage("") with Logging {


  val activeQueries = parent.listener.activeQueries
  val activeQueryProgress = parent.listener.activeQueryProgress

  override def render(request: HttpServletRequest): Seq[Node] = {
    val pageHeaderText: String = "Structured Streaming Queries"

    val someTitle = <h4>Some page content to go here</h4>

    val pageContent = someTitle ++ createHiddenDataNode

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500),
      useDataTables = true)

  }

  private def createHiddenDataNode: Seq[Node] = {

    <div id="hiddenData"
         data-clusterstarttime="">
      <div> <span>activeQueries: </span>
        {
         var str: String = "| "
         parent.listener.activeQueries.foreach(aq => {
           str = str + aq._1 + " >> "  + aq._2 + " | "
         })
        str
        }
      </div>
      <div><span>activeQueryProgress : </span>
        {
        var str: String = "| "
        parent.listener.activeQueryProgress.foreach(aq => {
          str = str + aq._1 + " >> " + aq._2 + " | "
        })
        str
        }
      </div>
    </div>
  }

}
