package io.snappydata

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricServer}
import io.snappydata.gemxd.ClusterCallback
import org.apache.spark.sql.columntable.StoreCallback

// TODO: Documentation
trait Server extends FabricServer with ClusterCallback with StoreCallback {

}

// TODO: Documentation
trait Lead extends Server {

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

  abstract override def getNetProtocol: String = {
    "jdbc:snappydata:"
  }

  abstract override def getDRDAProtocol: String = {
    return "jdbc:snappydata:drda://"
  }

  abstract override def getThriftProtocol: String = {
    return "jdbc:snappydata:thrift://"
  }
}
