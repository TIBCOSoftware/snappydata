package io.snappydata

/**
 * Created by soubhikc on 16/10/15.
 */

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}

// TODO: Documentation
trait Server extends FabricServer {

}

// TODO: Documentation
trait Lead extends Server {

}

// TODO: Documentation
trait Locator extends FabricLocator {

}


trait ProtocolOverrides extends FabricServiceImpl {

  abstract override def getProtocol : java.lang.String = {
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
