package io.snappydata

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}
import io.snappydata.gemxd.ClusterCallback

// TODO: Documentation
trait Server extends FabricServer with ClusterCallback {

}

// TODO: Documentation
trait Lead extends Server {
  val LEADER_SERVERGROUP = "IMPLICIT_LEADER_SERVERGROUP"

  def waitUntilPrimary()
}

// TODO: Documentation
trait Locator extends FabricLocator with ClusterCallback {

}

/**
 * Created by soubhikc on 16/10/15.
 */
trait ProtocolOverrides extends FabricServiceImpl {

  abstract override def getProtocol: java.lang.String = {
    "jdbc:snappydata:"
  }

  abstract override def getNetProtocol = {
    "jdbc:snappydata:"
  }

  abstract override def getDRDAProtocol: String = {
    return "jdbc:snappydata:drda://"
  }

  abstract override def getThriftProtocol: String = {
    return "jdbc:snappydata:thrift://"
  }
}
