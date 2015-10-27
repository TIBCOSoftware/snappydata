package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.scheduler.{TaskScheduler, SchedulerBackend, ExternalClusterManager}

/**
 * A cluster manager that delegates the execution to another cluster manager
 * but does some initialization activities after the scheduler
 * backend is started.
 *
 * Created by hemant
 */
object DelegateClusterManager extends ExternalClusterManager {

  var masterurl: String = ""
  var schedulerBackend: Option[SchedulerBackend] = None
  var taskScheduler: Option[TaskScheduler] = None
  var isLocal = false

  def createTaskScheduler(sc: SparkContext): TaskScheduler = {
    val (sb, ts) = SparkContext.createTaskScheduler(
      sc, masterurl.replaceFirst("snappy:", ""))
    taskScheduler = Some(ts)
    schedulerBackend = Some(sb)
    isLocal = sb.isInstanceOf[LocalBackend]
    ts
  }

  def canCreate(masterURL: String): Boolean = {
    if (masterURL.toLowerCase().startsWith("snappy:")) {
      masterurl = masterURL.toLowerCase()
      true
    } else false
  }

  def createSchedulerBackend(sc: SparkContext,
      scheduler: TaskScheduler): SchedulerBackend = {
    schedulerBackend.get
  }

  def intialize(scheduler: TaskScheduler,
      backend: SchedulerBackend): Unit = {
    //TODO: hemant: for local mode start job server here.
    //TODO: hemant: for non embedded mode, hive metastore initialization should be done here
  }
}

