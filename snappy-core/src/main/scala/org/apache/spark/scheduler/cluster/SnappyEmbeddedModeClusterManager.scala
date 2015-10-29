package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl, TaskScheduler, ExternalClusterManager}

/**
 * Snappy's cluster manager that is responsible for creating
 * scheduler and scheduler backend.
 *
 * Created by hemant
 */
object SnappyEmbeddedModeClusterManager extends ExternalClusterManager {

  var schedulerBackend: Option[SnappyCoarseGrainedSchedulerBackend] = None

  def createTaskScheduler(sc: SparkContext): TaskScheduler = {
    //TODO: hemant: throw an exception if there is already a driver running
    //TODO: hemant: in the distributed system
    new TaskSchedulerImpl(sc)
  }

  def canCreate(masterURL: String): Boolean =
    if (masterURL == "snappy") true else false

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