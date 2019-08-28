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
package org.apache.spark.sql


import com.pivotal.gemfirexd.internal.engine.Misc
import com.typesafe.config.{Config, ConfigException}
import io.snappydata.{Constant, ServiceManager}
import io.snappydata.impl.LeadImpl
import spark.jobserver.context.SparkContextFactory
import spark.jobserver.util.ContextURLClassLoader
import spark.jobserver.{ContextLike, SparkJobBase, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.SnappyUtils


class SnappySessionFactory extends SparkContextFactory {

  type C = SnappySession with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    SnappySessionFactory.newSession()
  }
}

object SnappySessionFactory {

  def updateCredentials(snc: SnappySession, jobConfig: Config,
      fromStreamCtx: Boolean = false): Config = {
    if (Misc.isSecurityEnabled) {
      try {
        // Pass job credentials to snappy session
        val username = jobConfig.getString("snappydata.user")
        val password = jobConfig.getString("snappydata.password")
        if (fromStreamCtx) {
          val old = snc.sqlContext.getConf(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, "")
          if (!old.isEmpty && !old.equalsIgnoreCase(username)) {
            throw new UnsupportedOperationException("Cannot submit a streaming job using an " +
                "existing streaming context and a different username, when cluster is secure.")
          }
        }
        snc.sqlContext.setConf(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, username)
        snc.sqlContext.setConf(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR, password)
        snc.sqlContext.setConf(Constant.STORE_PROPERTY_PREFIX
            + com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, "*****")
        // Clear admin user/password from jobConfig before passing it to user job.
        cleanJobConfig(jobConfig)
      } catch {
        case _: ConfigException.Missing => jobConfig // Config not found
      }
    } else {
      jobConfig
    }
  }

  def cleanJobConfig(c: Config): Config = {
    c.withoutPath(Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd.Attribute.USERNAME_ATTR)
        .withoutPath(Constant.STORE_PROPERTY_PREFIX + com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)
        .withoutPath(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR)
        .withoutPath(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)
        .withoutPath("gemfire.sys.security-password")
        .withoutPath("javax.jdo.option.ConnectionURL")
    // Remove snappydata properties file path when available.
  }

  protected def newSession(): SnappySession with ContextLike =
    new SnappySession(SparkContext.getActive.get) with ContextLike {

      override def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SnappySQLJob]

      // Calling this method from JobKill.
      override def stop(): Unit = {
        // Stopping all StreamingQueries started by the session.
        // If it's a normal job there won't be any streaming query and it will be a no -op.
        this.sessionState.streamingQueryManager.active.foreach(q => q.stop())
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


trait SnappySQLJob extends SparkJobBase {
  type C = Any

  final override def validate(sc: C, config: Config): SparkJobValidation = {
    SnappyJobValidate.validate(isValidJob(sc.asInstanceOf[SnappySession],
      SnappySessionFactory.updateCredentials(sc.asInstanceOf[SnappySession], config)))
  }

  final override def runJob(sc: C, jobConfig: Config): Any = {
    CodeGenerator.jobClassLoader.set(Thread.currentThread().getContextClassLoader)
    val snSession = sc.asInstanceOf[SnappySession]
    val sparkContext = snSession.sparkContext
    try {
      SnappyUtils.setSessionDependencies(sparkContext,
        appName = this.getClass.getCanonicalName,
        classLoader = Thread.currentThread().getContextClassLoader)
      runSnappyJob(snSession, SnappySessionFactory.updateCredentials(snSession, jobConfig))
    } finally {
      SnappyUtils.clearSessionDependencies(sparkContext)
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
