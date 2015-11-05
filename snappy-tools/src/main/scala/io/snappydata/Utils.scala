package io.snappydata

import java.util.Properties

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup
import com.pivotal.gemfirexd.internal.catalog.ExternalCatalog
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.util.ShutdownHookManager

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

  def initializeAndGetMetastore:ExternalCatalog = {
    Misc.getMemStore.getExternalCatalog
  }

}
