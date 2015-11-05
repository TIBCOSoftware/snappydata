package io.snappydata

import java.util.Properties

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import scala.collection.JavaConversions._
/**
 * Created by soubhikc on 20/10/15.
 */
object Utils {
  val SnappyDataThreadGroup = new GemFireThreadGroup("SnappyData Thread Group") {
    override def uncaughtException(t: Thread, e: Throwable) {
      if (e.isInstanceOf[Error] && SystemFailure.isJVMFailureError(e.asInstanceOf[Error])) {
        SystemFailure.setFailure(e.asInstanceOf[Error])
      }
      Thread.dumpStack
    }
  }

  def getLocatorClientURL(): String ={
    val urlPrefix = "jdbc:snappydata://"
    val drdaServerOnLocator = GemFireXDUtils.getGfxdAdvisor.getAllDRDAServersOnLOcators()
    val locatorURL = drdaServerOnLocator.entrySet().filter(entry => entry.getValue !=null && !entry.getValue.isEmpty ).head.getValue
    val port = locatorURL.substring(locatorURL.indexOf("[") + 1, locatorURL.indexOf("]"))
    s"${urlPrefix}localhost:$port"
  }

}
