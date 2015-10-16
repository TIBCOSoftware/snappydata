package io.snappydata

import com.gemstone.gemfire.internal.AvailablePort
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

/**
 * Created by soubhikc on 16/10/15.
 */
class ServiceManagerSpec  extends WordSpec with Matchers {

  "locator" when {
    "started" should {
      "start successfully" in {
        Try {
          ServiceManager.getLocatorInstance.start("localhost", AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS), new java.util.Properties())
        } transform({ _ =>
          Try {
            ServiceManager.getLocatorInstance.stop(null)
          }
        }, throw _)
      }
    }
  }

}
