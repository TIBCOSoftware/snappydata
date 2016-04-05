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
package org.apache.spark.sql

import com.typesafe.config.Config
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase}

import org.apache.spark.SparkConf

trait SnappySQLJob extends SparkJobBase {
  type C = SnappyContext
}

class SnappyContextFactory extends SparkContextFactory {

  type C = SnappyContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    SnappyContextFactory.newSession()
  }
}

object SnappyContextFactory {

  private[this] val snappyContextLike =
    SnappyContext.getOrCreate(LeadImpl.getInitializingSparkContext)

  protected def newSession(): SnappyContext with ContextLike =
    new SnappyContext(snappyContextLike.sparkContext,
      snappyContextLike.listener,
      false,
      snappyContextLike.snappyContextFunctions) with ContextLike {
    override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]
    override def stop(): Unit =
    {
      // not stopping anything here because SQLContext doesn't have one.
    }
  }

}

