/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.scheduler.cluster

import io.snappydata.impl.LeadImpl
import io.snappydata.util.ServiceUtils
import io.snappydata.{Constant, Property, ServiceManager}
import org.slf4j.LoggerFactory

import org.apache.spark.scheduler._
import org.apache.spark.{SparkContext, SparkException}

/**
 * Snappy's cluster manager that is responsible for creating
 * scheduler and scheduler backend.
 */
class SnappyEmbeddedModeClusterManager extends ExternalClusterManager {

  private val logger = LoggerFactory.getLogger(getClass)

  SnappyClusterManager.init(this)

  @volatile var schedulerBackend: SnappyCoarseGrainedSchedulerBackend = _

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    // If there is an application that is trying to join snappy
    // as lead in embedded mode, we need the locator to connect
    // to the snappy distributed system and hence the locator is
    // passed in masterurl itself.
    if (sc.master.startsWith(Constant.SNAPPY_URL_PREFIX)) {
      val locator = sc.master.replaceFirst(Constant.SNAPPY_URL_PREFIX, "").trim

      val (prop, value) = {
        if (locator.indexOf("mcast-port") >= 0) {
          val split = locator.split("=")
          (split(0).trim, split(1).trim)
        }
        else if (locator.isEmpty ||
            locator == "null" ||
            !ServiceUtils.LOCATOR_URL_PATTERN.matcher(locator).matches()
        ) {
          throw new Exception(s"locator info not provided in the snappy embedded url ${sc.master}")
        }
        (Property.Locators.name, locator)
      }

      logger.info(s"setting from url $prop with $value")
      sc.conf.set(prop, value)
    }
    new SnappyTaskSchedulerImpl(sc)
  }

  override def canCreate(masterURL: String): Boolean =
    masterURL.startsWith("snappydata")

  override def createSchedulerBackend(sc: SparkContext, masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    sc.addSparkListener(new BlockManagerIdListener(sc))
    schedulerBackend = new SnappyCoarseGrainedSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl], sc.env.rpcEnv)

    schedulerBackend
  }

  def initialize(scheduler: TaskScheduler,
      backend: SchedulerBackend): Unit = {
    assert(scheduler.isInstanceOf[TaskSchedulerImpl])
    val schedulerImpl = scheduler.asInstanceOf[TaskSchedulerImpl]

    schedulerImpl.initialize(backend)

    // fail if not invoked by launcher
    ServiceManager.currentFabricServiceInstance match {
      case _: LeadImpl => // ok
      case null => throw new SparkException(
        "Lead creation only supported from ServiceManager API")
      case service => throw new SparkException(
        s"Trying to start lead on node already booted as $service")
    }

    // wait for store to initialize (acquire lead lock or go to standby)
    LeadImpl.invokeLeadStart(schedulerImpl.sc.conf)
  }

  def stopLead(): Unit = {
    LeadImpl.invokeLeadStop()
  }

}

object SnappyClusterManager {

  private[this] var _cm: SnappyEmbeddedModeClusterManager = _

  def init(mgr: SnappyEmbeddedModeClusterManager): Unit = {
    _cm = mgr
  }

  def cm: Option[SnappyEmbeddedModeClusterManager] = Option(_cm)
}
