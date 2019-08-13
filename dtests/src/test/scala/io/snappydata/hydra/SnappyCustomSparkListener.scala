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
package io.snappydata.hydra

import java.lang.reflect.{Constructor, Method}

import org.apache.spark.scheduler._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.{Logging, SparkContext}

class SnappyCustomSparkListener extends SparkListener with Logging {

  override def onJobStart(jobStart: SparkListenerJobStart) {
    // scalastyle:off println
    println(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println(s"Job completed with Result :  ${jobEnd.jobResult}")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    println("Spark ApplicationStart: " + applicationStart.appName);
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("Spark ApplicationEnd: " + applicationEnd.time);
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(s"Stage ${stageCompleted.stageInfo.stageId} completed with  ${
      stageCompleted
          .stageInfo.numTasks
    } tasks.")
  }

  override def onExecutorAdded(added: SparkListenerExecutorAdded): Unit = {
    try {
      logInfo(s"Listener: onExecutorAdded: added $added")
      val sncClass = Utils.classForName(
        "org.apache.spark.sql.SnappySession")

      val constructor: Constructor[_] = sncClass
        .getConstructor(classOf[SparkContext])
      val snc = constructor.newInstance(SparkContext.getOrCreate())
      logInfo(s"Listener : onExecutorAdded: The snc after loading $snc")

      val method: Method = sncClass.getMethod(
        "executorAdded", classOf[scala.Boolean])

      logInfo(s"Listener: onExecutorAdded method $method")

      method.invoke(snc, Boolean.box(true))

    } catch {
      case _: ClassNotFoundException => {
        logInfo(s"Listener: onExecutorAdded: Did not find SNC")
      }
    }
  }
}
