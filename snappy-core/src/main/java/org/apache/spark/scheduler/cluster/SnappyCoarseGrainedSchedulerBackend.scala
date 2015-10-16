package org.apache.spark.scheduler.cluster

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * Created by hemantb on 10/5/15.
 *
 */
class SnappyCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, override val rpcEnv: RpcEnv)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  var driverUrl: String = ""

  override def start() {
    super.start()
    driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(driverEndpoint.address.host, driverEndpoint.address.port),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
  }

  override def stop() {
    super.stop()
    driverUrl = ""
  }
}

object SnappyClusterManager extends ExternalClusterManager {

  var schedulerBackend: Option[SnappyCoarseGrainedSchedulerBackend] = None

  def createTaskScheduler(sc: SparkContext): TaskScheduler = new TaskSchedulerImpl(sc)

  def canCreate(masterURL: String): Boolean = if (masterURL == "snappy") true else false

  def createSchedulerBackend(sc: SparkContext,
                             scheduler: TaskScheduler): SchedulerBackend = {
    schedulerBackend = Some(
      new SnappyCoarseGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl], sc.env.rpcEnv))
    schedulerBackend.get
  }

  def intialize(scheduler: TaskScheduler,
                backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }

}
