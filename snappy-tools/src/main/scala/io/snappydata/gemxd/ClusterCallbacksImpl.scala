package io.snappydata.gemxd

import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks}
import io.snappydata.cluster.ExecutorInitiator
import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
 * Callbacks that are sent by GemXD to Snappy for cluster management
 *
 * Created by hemantb on 10/12/15.
 */
object ClusterCallbacksImpl extends ClusterCallbacks {

  override def launchExecutor(driver_url: String) = {
    val url = if (driver_url == null || driver_url == "")
      None
    else Some(driver_url)
    ExecutorInitiator.startOrTransmuteExecutor(url)

  }

  override def getDriverURL: String = {
    //TODO: Hemant: If the driverURL is null, GfxdProfile exchange
    //TODO: Hemant: may not change it and may point to a stale url.
    return SnappyEmbeddedModeClusterManager.schedulerBackend match {
      case Some(x) =>
        x.driverUrl

      case None => null
    }
  }

  override def stopExecutor = {
    ExecutorInitiator.stop()
  }
}

/**
 * Created by soubhikc on 19/10/15.
 */
trait ClusterCallback {
  CallbackFactoryProvider.setClusterCallbacks(ClusterCallbacksImpl)
}
