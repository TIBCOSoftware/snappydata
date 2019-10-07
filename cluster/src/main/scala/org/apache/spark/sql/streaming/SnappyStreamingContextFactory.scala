/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import com.typesafe.config.{Config, ConfigException}
import io.snappydata.ServiceManager
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase, SparkJobValidation}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyJobValidate, SnappyJobValidation, SnappySessionFactory}
import org.apache.spark.streaming.{JavaSnappyStreamingJob, Milliseconds, SnappyStreamingContext}
import org.apache.spark.util.SnappyUtils
import spark.jobserver.util.ContextURLClassLoader

abstract class SnappyStreamingJob extends SparkJobBase {
  override type C = SnappyStreamingContext

  final override def validate(sc: C, config: Config): SparkJobValidation = {
    SnappyJobValidate.validate(isValidJob(sc.asInstanceOf[SnappyStreamingContext],
      SnappySessionFactory.updateCredentials(sc.asInstanceOf[SnappyStreamingContext]
          .snappySession, config, fromStreamCtx = true)))
  }

  final override def runJob(sc: C, jobConfig: Config): Any = {
    val snc = sc.asInstanceOf[SnappyStreamingContext]
    try {
      SnappyUtils.setSessionDependencies(
        snc.sparkContext,
        appName = this.getClass.getCanonicalName,
        classLoader = Thread.currentThread().getContextClassLoader)

      runSnappyJob(snc, SnappySessionFactory.updateCredentials(snc.snappySession, jobConfig,
        fromStreamCtx = true))
    } finally {
    }
  }

  def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation

  def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any

}

class SnappyStreamingContextFactory extends SparkContextFactory {

  override type C = SnappyStreamingContext with ContextLike

  override def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")

    new SnappyStreamingContext(SparkContext.getActive.get,
      Milliseconds(interval)) with ContextLike {

      override def isValidJob(job: SparkJobBase): Boolean =
        job.isInstanceOf[SnappyStreamingJob] || job.isInstanceOf[JavaSnappyStreamingJob]

      override def stop(): Unit = {
        try {
          val stopGracefully = config.getBoolean("streaming.stopGracefully")
          SnappyStreamingContext.getActive
              .foreach(c => c.stop(stopSparkContext = false, stopGracefully = stopGracefully))
        } catch {
          case _: ConfigException.Missing => SnappyStreamingContext.getActive
              .foreach(c => c.stop(stopSparkContext = false, stopGracefully = true))
        } finally {
          SnappyUtils.clearSessionDependencies(sparkContext)
        }
      }

      // Callback added to provide our classloader to load job classes.
      // If Job class directly refers to any jars which has been provided
      // by install_jars, this can help.
      override def makeClassLoader(parent: ContextURLClassLoader): ContextURLClassLoader = {
        val cl = SnappyUtils.getSnappyContextURLClassLoader(parent)
        val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
        val loader = lead.urlclassloader
        if (loader != null) {
          loader.getURLs.foreach(u => {
            cl.addURL(u)
          })
        }
        cl
      }
    }
  }
}
