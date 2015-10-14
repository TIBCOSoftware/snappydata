package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.fabricservice.{FabricServerImpl}
import com.pivotal.gemfirexd.internal.snappy.CallbackFactoryHolder
import io.snappydata.Server
import io.snappydata.gemxd.ClusterCallbacksImpl

class ServerImpl extends FabricServerImpl
  with  Server{

  override def start(bootProperties: Properties) = {
    start(bootProperties, false)
  }

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean) = {
    synchronized {
      CallbackFactoryHolder.setClusterCallbacks(ClusterCallbacksImpl)
      super.start(bootProperties, ignoreIfStarted)
    }
  }

  override def isServer: Boolean = {
    return true
  }
}