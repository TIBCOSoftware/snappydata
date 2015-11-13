package io.snappydata.gemxd

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks}
import io.snappydata.cluster.ExecutorInitiator
import org.slf4j.LoggerFactory
import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
  * Callbacks that are sent by GemXD to Snappy for cluster management
  *
  * Created by hemantb on 10/12/15.
  */
object ClusterCallbacksImpl extends ClusterCallbacks {

  val logger = LoggerFactory.getLogger(getClass)

  override def launchExecutor(driverUrl: String, driverDM: InternalDistributedMember): Unit = {
    val url = if (driverUrl == null || driverUrl == "") {
      logger.info(s"call to launchExecutor but driverUrl is invalid. ${driverUrl}")
      None
    }
    else {
      Some(driverUrl)
    }
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)
  }

  override def getDriverURL: String = {
    return SnappyEmbeddedModeClusterManager.schedulerBackend match {
      case Some(x) =>
        logger.info(s"returning driverUrl ${x.driverUrl}")
        x.driverUrl
      case None =>
        null
    }
  }

  override def stopExecutor: Unit = {
    ExecutorInitiator.stop()
  }
}

/**
  * Created by soubhikc on 19/10/15.
  */
trait ClusterCallback {
  CallbackFactoryProvider.setClusterCallbacks(ClusterCallbacksImpl)
}
