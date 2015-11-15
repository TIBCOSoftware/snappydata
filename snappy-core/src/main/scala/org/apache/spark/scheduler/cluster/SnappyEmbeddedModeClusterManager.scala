package org.apache.spark.scheduler.cluster

import io.snappydata.{Const, Prop}
import org.slf4j.LoggerFactory

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

  SparkContext.registerClusterManager(this)

  val logger = LoggerFactory.getLogger(getClass)

  var schedulerBackend: Option[SnappyCoarseGrainedSchedulerBackend] = None

  var sc: SparkContext = _


  def createTaskScheduler(sc: SparkContext): TaskScheduler = {
    // If there is an application that is trying to join snappy
    // as lead in embedded mode, we need the locator to connect
    // to the snappy distributed system and hence the locator is
    // passed in masterurl itself.
    if (sc.master.startsWith(Const.jdbcUrlPrefix)) {
      val locator = sc.master.replaceFirst(Const.jdbcUrlPrefix, "").trim

      val (prop, value) = {
        if (locator.indexOf("mcast-port") >= 0) {
          val split = locator.split("=")
          (split(0).trim, split(1).trim)
        }
        else if (locator.isEmpty ||
            locator == "" ||
            locator == "null" ||
            !locator.matches(".+\\[[0-9]+\\]")
        ) {
          throw new Exception(s"locator info not provided in the snappy embedded url ${sc.master}")
        }
        (Prop.locators, locator)
      }

      logger.info(s"setting from url ${prop} with ${value}")
      sc.conf.set(prop, value)
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
    sc = schedulerImpl.sc
  }

  def postStartHook(): Unit = {
    SnappyContext(sc)
  }

  def stop(): Unit = {
    sc = null
    SnappyContext.toolsCallback.invokeLeadStop(null)
  }

}