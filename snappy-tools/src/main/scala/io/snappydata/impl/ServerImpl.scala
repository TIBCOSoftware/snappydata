package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.fabricservice.{FabricServerImpl, FabricServiceImpl}
import io.snappydata.Server

class ServerImpl extends FabricServerImpl
  with  Server{

  override def start(bootProperties: Properties) = {
    start(bootProperties, false)
  }

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean) = {
    synchronized {
      super.start(bootProperties, ignoreIfStarted)
    }
  }

  override def isServer: Boolean = {
    return true
  }
}