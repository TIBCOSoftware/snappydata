package io.snappydata

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import io.snappydata.impl.{LeadImpl, LocatorImpl, ServerImpl}


/**
 * Created by hemantb.
 */
object ServiceManager {

  private[this] val contextLock = new AnyRef

  /**
   * Get the singleton instance of {@link Server}.
   */
  def getServerInstance: Server = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkServerInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance  == null) {
        val server: Server = new ServerImpl
        FabricServiceImpl.setInstance(server)
        return server
      }
      return checkServerInstance(instance)
    }
  }

  /**
   * Get the singleton instance of {@link Locator}.
   */
  def getLocatorInstance: Locator = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkLocatorInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance  == null) {
        val locator: Locator = new LocatorImpl
        FabricServiceImpl.setInstance(locator)
        return locator
      }
      return checkLocatorInstance(instance)
    }
  }

  /**
   * Get the singleton instance of {@link Lead}.
   */
  def getLeadInstance: Lead = {
    var instance: FabricService = FabricServiceImpl.getInstance
    if (instance != null) {
      return checkLeadInstance(instance)
    }
    contextLock.synchronized {
      instance = FabricServiceImpl.getInstance
      if (instance  == null) {
        val lead: Lead = new LeadImpl
        FabricServiceImpl.setInstance(lead)
        return lead
      }
      return checkLeadInstance(instance)
    }
  }

  /**
   * Get the current instance of either {@link Server} or
   * {@link Locator} or {@link Lead}. This can be null if neither of
   * {@link #getServerInstance()} or {@link #getLeadInstance()} or
   * {@link #getLocatorInstance()} have been invoked, or the instance
   * has been stopped.
   */
  def currentFabricServiceInstance: FabricService = {
    return FabricServiceImpl.getInstance
  }

  private def checkServerInstance(instance: FabricService): Server = {
    if (instance.isInstanceOf[Server]) {
      return instance.asInstanceOf[Server]
    }
    throw new IllegalStateException(s"Found an instance of another snappy component ${instance}.")
  }

  private def checkLocatorInstance(instance: FabricService): Locator = {
    if (instance.isInstanceOf[Locator]) {
      return instance.asInstanceOf[Locator]
    }
    throw new IllegalStateException(s"Found an instance of another snappy component ${instance}.")
  }

  private def checkLeadInstance(instance: FabricService): Lead = {
    if (instance.isInstanceOf[Lead]) {
      return instance.asInstanceOf[Lead]
    }
    throw new IllegalStateException(s"Found an instance of another snappy component ${instance}.")
  }

}
