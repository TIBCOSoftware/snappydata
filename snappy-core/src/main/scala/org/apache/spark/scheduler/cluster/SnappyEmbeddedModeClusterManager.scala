package org.apache.spark.scheduler.cluster

import io.snappydata.{Const, Prop}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.sql.SnappyContext

/**
  * Snappy's cluster manager that is responsible for creating
  * scheduler and scheduler backend.
  *
  * Created by hemant
  */
object SnappyEmbeddedModeClusterManager extends ExternalClusterManager {

  SparkContext.registerClusterManager(SnappyEmbeddedModeClusterManager)

  var schedulerBackend: Option[SnappyCoarseGrainedSchedulerBackend] = None

  def createTaskScheduler(sc: SparkContext): TaskScheduler = {
    // If there is an application that is trying to join snappy
    // as lead in embedded mode, we need the locator to connect
    // to the snappy distributed system and hence the locator is
    // passed in masterurl itself.
    if (sc.master.startsWith(Const.jdbcUrlPrefix)) {
      val locator = sc.master.replaceFirst(Const.jdbcUrlPrefix, "").trim
      if (locator.isEmpty ||
          locator == "" ||
          locator == "null" ||
          !locator.matches(".+\\[[0-9]+\\]")) {
        throw new Exception(s"locator info not provided in the snappy embedded url ${sc.master}")
      }
      sc.conf.set(Prop.locators, locator)
    }
    new TaskSchedulerImpl(sc)
  }

  def canCreate(masterURL: String): Boolean =
    if (masterURL.startsWith("snappydata")) true else false

  def createSchedulerBackend(sc: SparkContext,
      scheduler: TaskScheduler): SchedulerBackend = {
    schedulerBackend = Some(
      new SnappyCoarseGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl], sc.env.rpcEnv))

    schedulerBackend.get
  }

  def initialize(scheduler: TaskScheduler,
      backend: SchedulerBackend): Unit = {
    assert(scheduler.isInstanceOf[TaskSchedulerImpl])
    val schedulerImpl = scheduler.asInstanceOf[TaskSchedulerImpl]

    schedulerImpl.initialize(backend)

    SnappyContext.toolsCallback.invokeLeadStart(schedulerImpl.sc.conf)
    SnappyContext.setGlobalContext(schedulerImpl.sc)
  }
}