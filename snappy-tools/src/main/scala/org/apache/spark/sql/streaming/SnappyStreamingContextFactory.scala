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

import com.typesafe.config.Config
import io.snappydata.impl.LeadImpl
import org.apache.spark.sql.SnappyContext
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}


/**
  * Created by soubhikc on 14/12/15.
  */
trait SnappyStreamingJob extends SparkJobBase {
  type C = SnappyStreamingContext
}


/**
  * Created by soubhikc on 14/12/15.
  */
class SnappyStreamingContextFactory extends SparkContextFactory {

  override type C = SnappyStreamingContext with ContextLike

  override def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")

    new SnappyStreamingContext(new SnappyContext(LeadImpl.getInitializingSparkContext()),
      Milliseconds(interval)) with ContextLike {

      override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappyStreamingJob]

      override def stop(): Unit = {
        val stopGracefully = config.getBoolean("streaming.stopGracefully")
        stop(false, stopGracefully)
      }
    }
  }
}