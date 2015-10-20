package io.snappydata

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup

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

}
