package io.snappydata.gemxd

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks, LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.cluster.ExecutorInitiator

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
 * Callbacks that are sent by GemXD to Snappy for cluster management
 *
 * Created by hemantb on 10/12/15.
 */
object ClusterCallbacksImpl extends ClusterCallbacks {

  override def launchExecutor(driver_url: String, driverDM: InternalDistributedMember) = {
    val url = if (driver_url == null || driver_url == "")
      None
    else Some(driver_url)
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)

  }

  override def getDriverURL: String = {
    return SnappyEmbeddedModeClusterManager.schedulerBackend match {
      case Some(x) =>
        x.driverUrl

      case None => null
    }
  }

  override def stopExecutor = {
    ExecutorInitiator.stop()
  }

  override def getSQLExecute(sql: String, ctx: LeadNodeExecutionContext,
      v: Version): SparkSQLExecute = new SparkSQLExecuteImpl(sql, ctx, v)
}

/**
 * Created by soubhikc on 19/10/15.
 */
trait ClusterCallback {
  CallbackFactoryProvider.setClusterCallbacks(ClusterCallbacksImpl)
}
