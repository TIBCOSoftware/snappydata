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

import scala.util.Try

import com.typesafe.config.Config
import io.snappydata.Constant
import io.snappydata.impl.LeadImpl
import org.joda.time.DateTime
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.util.ContextURLClassLoader
import spark.jobserver.{ContextLike, SparkJobBase, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.SparkConf
import org.apache.spark.util.{SnappyContextLoader, SnappyContextURLLoader, SnappyUtils}


class SnappySessionFactory extends SparkContextFactory {

  type C = SnappySession with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    SnappySessionFactory.newSession()
  }
}

object SnappySessionFactory {

  val job_jar_path = "JOB_JAR_PATH"
  private[this] val snappySession =
    new SnappySession(LeadImpl.getInitializingSparkContext)

  protected def newSession(): SnappySession with ContextLike =
    new SnappySession(snappySession.sparkContext) with ContextLike {

      override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]

      override def stop(): Unit = {
        // not stopping anything here because SQLContext doesn't have one.
      }

      // Callback added to provide our classloader to load job classes.
      // If Job class directly refers to any jars which has been provided
      // by install_jars, this can help.
      override def makeClassLoader(parent: ContextURLClassLoader): ContextURLClassLoader = {
        SnappyUtils.getSnappyContextURLClassLoader(parent)
      }

      override def addJobJar(jarPath : String): Unit = {
        addContextObject[String](job_jar_path, jarPath)
      }
    }
}


trait SnappySQLJob extends SparkJobBase {
  type C = Any

  final override def validate(sc: C, config: Config): SparkJobValidation = {
    SnappyJobValidate.validate(isValidJob(sc.asInstanceOf[SnappySession], config))
  }

  final override def runJob(sc: C, jobConfig: Config): Any = {
    val appName = this.getClass.getCanonicalName
    val dependentJars =
      Thread.currentThread().getContextClassLoader.asInstanceOf[SnappyContextURLLoader].getURLs
    val localProperty = (Seq(appName, DateTime.now) ++ dependentJars.toSeq).mkString(",")
    val snSession = sc.asInstanceOf[SnappySession]
    val sparkContext = snSession.sparkContext
    try {
      sparkContext.setLocalProperty(Constant.CHANGEABLE_JAR_NAME, localProperty)
      runSnappyJob(snSession, jobConfig)
    } finally {
      sparkContext.setLocalProperty(Constant.CHANGEABLE_JAR_NAME, null)
    }
  }

  def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation

  def runSnappyJob(sc: SnappySession, jobConfig: Config): Any

}

abstract class JavaSnappySQLJob extends SnappySQLJob

object SnappyJobValidate {
  def validate(status: SnappyJobValidation): SparkJobValidation = {
    status match {
      case _: SnappyJobValid => SparkJobValid
      case j: SnappyJobInvalid => SparkJobInvalid(j.reason)
      case _ => SparkJobInvalid("isValid method is not correct")
    }
  }
}

trait SnappyJobValidation

case class SnappyJobValid() extends SnappyJobValidation

case class SnappyJobInvalid(reason: String) extends SnappyJobValidation
