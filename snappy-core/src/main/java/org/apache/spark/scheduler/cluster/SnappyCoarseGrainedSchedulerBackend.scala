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

  private val snappyAppId = "snappy-app-" + System.currentTimeMillis

  /**
   * Overriding the spark app id function to provide a snappy specific app id.
   * @return An application ID
   */
  override def applicationId(): String = snappyAppId

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

  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    // keep the app id as part of driver property so that it can be retrieved
    // by the executor when driver properties are fetched using
    // [org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps]
    super.createDriverEndpoint(properties ++
        Seq[(String, String)](("spark.app.id", applicationId())))
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
