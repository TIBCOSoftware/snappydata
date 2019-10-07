/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import io.snappydata.gemxd.ClusterCallbacksImpl
import io.snappydata.impl.{LeadImpl, LocatorImpl}

object ServiceManager {

  ClusterCallbacksImpl.initialize()

  private def contextLock = ServerManager.contextLock

  /**
   * Get the singleton instance of `Server`.
   */
  def getServerInstance: Server = ServerManager.getServerInstance

  /**
   * Get the singleton instance of `Locator`.
   */
  def getLocatorInstance: Locator = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkLocatorInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance == null) {
        val locator: Locator = new LocatorImpl
        FabricServiceImpl.setInstance(locator)
        return locator
      }
      return checkLocatorInstance(instance)
    }
  }

  /**
   * Get the singleton instance of `Lead`.
   */
  def getLeadInstance: Lead = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkLeadInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance == null) {
        val lead: Lead = new LeadImpl
        FabricServiceImpl.setInstance(lead)
        return lead
      }
      return checkLeadInstance(instance)
    }
  }

  /**
   * Get the current instance of either `Server` or `Locator` or `Lead`.
   * This can be null if neither of `getServerInstance` or `getLeadInstance` or
   * `getLocatorInstance` have been invoked, or the instance has been stopped.
   */
  def currentFabricServiceInstance: FabricService = FabricServiceImpl.getInstance

  private def checkLocatorInstance(instance: FabricService): Locator = {
    instance match {
      case locator: Locator => locator
      case _ => throw new IllegalStateException(
        s"Found an instance of another snappy component $instance.")
    }
  }

  private def checkLeadInstance(instance: FabricService): Lead = {
    instance match {
      case lead: Lead => lead
      case _ => throw new IllegalStateException(
        s"Found an instance of another snappy component $instance.")
    }
  }
}
