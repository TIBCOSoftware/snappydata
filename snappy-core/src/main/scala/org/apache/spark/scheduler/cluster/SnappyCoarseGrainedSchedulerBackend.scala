package org.apache.spark.scheduler.cluster

import org.slf4j.LoggerFactory

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.scheduler.TaskSchedulerImpl

/**
 * Created by hemantb on 10/5/15.
 *
 */
class SnappyCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, override val rpcEnv: RpcEnv)
    extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  val logger = LoggerFactory.getLogger(getClass)

  private val snappyAppId = "snappy-app-" + System.currentTimeMillis

  /**
   * Overriding the spark app id function to provide a snappy specific app id.
   * @return An application ID
   */
  override def applicationId(): String = snappyAppId

  @volatile private var _driverUrl: String = ""

  def driverUrl: String = _driverUrl

  override def start() {

    super.start()
    _driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(driverEndpoint.address.host, driverEndpoint.address.port),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    logger.info(s"started with driverUrl $driverUrl")
  }

  override def stop() {
    super.stop()
    _driverUrl = ""
    logger.info(s"stopped successfully")
  }

  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    // keep the app id as part of driver property so that it can be retrieved
    // by the executor when driver properties are fetched using
    // [org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps]
    super.createDriverEndpoint(properties ++
        Seq[(String, String)](("spark.app.id", applicationId())))
  }
}

