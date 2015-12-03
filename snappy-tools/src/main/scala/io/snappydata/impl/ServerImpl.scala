package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.GfxdConstants
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServerImpl
import io.snappydata.{Constant, Property, Utils, ProtocolOverrides, Server}

/**
  * This class ties up few things that is Snappy specific.
  * for e.g. Connection url & ClusterCallback
  */
class ServerImpl extends FabricServerImpl with Server with ProtocolOverrides {

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties): Unit = start(bootProperties, false)

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit =
    this.synchronized {
      super.start(bootProperties, ignoreIfStarted)
    }

  override def isServer: Boolean = {
    return true
  }
}