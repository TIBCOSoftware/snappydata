package io.snappydata.gemxd

import java.util

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks,
  LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl

import org.apache.spark.Logging
import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
  * Callbacks that are sent by GemXD to Snappy for cluster management
  *
  * Created by hemantb on 10/12/15.
  */
object ClusterCallbacksImpl extends ClusterCallbacks with Logging {

  override def getLeaderGroup : util.HashSet[String] = {
    val leaderServerGroup = new util.HashSet[String]
    leaderServerGroup.add(LeadImpl.LEADER_SERVERGROUP)
    return leaderServerGroup;
  }

  override def launchExecutor(driverUrl: String, driverDM: InternalDistributedMember): Unit = {
    val url = if (driverUrl == null || driverUrl == "") {
      logInfo(s"call to launchExecutor but driverUrl is invalid. ${driverUrl}")
      None
    }
    else {
      Some(driverUrl)
    }
    logInfo(s"invoking startOrTransmute with. ${url}")
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)
  }

  override def getDriverURL: String = {
    return SnappyEmbeddedModeClusterManager.schedulerBackend match {
      case Some(x) =>
        logInfo(s"returning driverUrl=${x.driverUrl}")
        x.driverUrl
      case None =>
        null
    }
  }

  override def stopExecutor: Unit = {
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
