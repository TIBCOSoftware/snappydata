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

import com.typesafe.config.{Config, ConfigException}
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.{ContextLike, SparkJobBase, SparkJobValidation}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SnappyJobValidate, SnappyJobValidation}
import org.apache.spark.streaming.{JavaSnappyStreamingJob, Milliseconds, SnappyStreamingContext}
import org.apache.spark.util.SnappyUtils
import spark.jobserver.util.ContextURLClassLoader

abstract class SnappyStreamingJob extends SparkJobBase {
  override type C = SnappyStreamingContext
  final override def validate(sc: C, config: Config): SparkJobValidation = {
    val parentLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
    val currentLoader = SnappyUtils.getSnappyStoreContextLoader(parentLoader)
    Thread.currentThread().setContextClassLoader(currentLoader)
    SnappyJobValidate.validate(isValidJob(sc.asInstanceOf[SnappyStreamingContext], config))
  }

  final override def runJob(sc: C, jobConfig: Config): Any = {
    val snc = sc.asInstanceOf[SnappyStreamingContext]
    runSnappyJob(snc, jobConfig)
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

      private val addedJars = scala.collection.mutable.ArrayBuffer.empty[String]

      override def isValidJob(job: SparkJobBase): Boolean =
        job.isInstanceOf[SnappyStreamingJob] || job.isInstanceOf[JavaSnappyStreamingJob]

      override def stop(): Unit = {
        try {
          val stopGracefully = config.getBoolean("streaming.stopGracefully")
          stop(stopSparkContext = false, stopGracefully = stopGracefully)
          addedJars.foreach { jarName =>
            SnappyUtils.removeJobJar(sparkContext, jarName)
          }
        } catch {
          case _: ConfigException.Missing => stop(stopSparkContext = false, stopGracefully = true)
        }
      }

      // Callback added to provide our classloader to load job classes.
      // If Job class directly refers to any jars which has been provided
      // by install_jars, this can help.
      override def makeClassLoader(parent: ContextURLClassLoader): ContextURLClassLoader = {
        SnappyUtils.getSnappyContextURLClassLoader(parent)
      }

      override def addJobJar(jarName : String): Unit = {
       addedJars.append(jarName)
      }
    }
  }
}