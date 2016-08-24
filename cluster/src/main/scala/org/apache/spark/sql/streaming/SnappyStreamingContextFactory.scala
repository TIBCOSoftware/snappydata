/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.streaming

import com.typesafe.config.{ConfigException, Config}
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{SparkJobValidation, ContextLike, SparkJobBase}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SnappyJobValidation, SnappyJobValidate}
import org.apache.spark.streaming.{JavaSnappyStreamingJob, Milliseconds, SnappyStreamingContext}

abstract class SnappyStreamingJob extends SparkJobBase {
  override type C = SnappyStreamingContext
  final override def validate(sc: C, config: Config): SparkJobValidation = {
    SnappyJobValidate.validate(isValidJob(sc.asInstanceOf[SnappyStreamingContext], config))
  }

  final override def runJob(sc: C, jobConfig: Config): Any = {
    runSnappyJob(sc.asInstanceOf[SnappyStreamingContext], jobConfig)
  }

  def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation

  def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any;

}

class SnappyStreamingContextFactory extends SparkContextFactory {

  override type C = SnappyStreamingContext with ContextLike

  override def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")

    new SnappyStreamingContext(LeadImpl.getInitializingSparkContext,
      Milliseconds(interval)) with ContextLike {

      override def isValidJob(job: SparkJobBase): Boolean =
        job.isInstanceOf[SnappyStreamingJob] || job.isInstanceOf[JavaSnappyStreamingJob]

      override def stop(): Unit = {
        try {
          val stopGracefully = config.getBoolean("streaming.stopGracefully")
          stop(stopSparkContext = false, stopGracefully = stopGracefully)
        } catch {
          case _: ConfigException.Missing => stop(stopSparkContext = false, stopGracefully = true)
        }
      }
    }
  }
}