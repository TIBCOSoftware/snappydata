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
package org.apache.spark.status.api.v1

import javax.ws.rs._

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer


/**
 * Main entry point for serving snappy application data as json, using JAX-RS.
 *
 * Each resource should have endpoints that return **public** classes defined in snappy-api.scala.
 * Mima binary compatibility checks ensure that we don't inadvertently make changes that break the
 * api.
 * The returned objects are automatically converted to json by jackson with JacksonMessageWriter.
 * In addition, there are a number of tests in HistoryServerSuite that compare the json to "golden
 * files".  Any changes and additions should be reflected there as well -- see the notes in
 * HistoryServerSuite.
 */

// todo : need to add tests to test below apis

@Path("/services")
private[v1] class SnappyApiRootResource extends ApiRequestContext {

  @Path("clusterinfo")
  def getClusterInfo(): ClusterInfoResource = {
    new ClusterInfoResource
  }

  @Path("allmembers")
  def getAllMembers(): AllMembersResource = {
    new AllMembersResource
  }

  @Path("memberdetails/{memberId}")
  def getMemberDetails(): MembersDetailsResource = {
    new MembersDetailsResource
  }

  @Path("alltables")
  def getAllTables(): AllTablesResource = {
    new AllTablesResource
  }

  @Path("allexternaltables")
  def getAllExternalTables(): AllExternalTablesResource = {
    new AllExternalTablesResource
  }

}

private[spark] object SnappyApiRootResource {

  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/snappy-api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}