package io.snappydata

import java.util.Properties
import java.util.regex.Pattern

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import scala.collection.JavaConversions._
/**
 * Created by soubhikc on 20/10/15.
 */
object Utils {
  val LocatorURLPattern = Pattern.compile("(.+:[0-9]+)|(.+\\[[0-9]+\\])");

  val SnappyDataThreadGroup = new GemFireThreadGroup("SnappyData Thread Group") {
    override def uncaughtException(t: Thread, e: Throwable) {
      if (e.isInstanceOf[Error] && SystemFailure.isJVMFailureError(e.asInstanceOf[Error])) {
        SystemFailure.setFailure(e.asInstanceOf[Error])
      }
      Thread.dumpStack
    }
  }

}
