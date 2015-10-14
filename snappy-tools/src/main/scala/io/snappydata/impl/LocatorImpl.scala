package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.fabricservice.{FabricLocatorImpl}
import io.snappydata.Locator

class LocatorImpl extends FabricLocatorImpl
  with  Locator{

  @throws(classOf[SQLException])
  override def start(bindAddress: String, port: Int,
                     bootProperties: Properties) = {
    start(bindAddress, port, bootProperties, false)
  }

  @throws(classOf[SQLException])
  override def start(bindAddress: String, port: Int,
                     bootProperties: Properties, ignoreIfStarted: Boolean) = {
    synchronized {
      super.start(bindAddress, port, bootProperties, ignoreIfStarted)
    }
  }

}