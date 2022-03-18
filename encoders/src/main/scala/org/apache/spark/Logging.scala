/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for TIBCO Project SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.util.Utils

/**
 * Utility trait for classes that want to log data. Creates an SLF4J logger
 * for the class and allows logging messages at different levels using methods
 * that only evaluate parameters lazily if the log level is enabled.
 */
trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient protected final var log_ : Logger = _

  @transient protected final var levelFlags: Int = _

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary()
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  private def setLevel(value: Boolean, enabled: Int, disabled: Int): Unit = {
    if (value) levelFlags |= enabled else levelFlags |= disabled
  }

  final def isInfoEnabled: Boolean = {
    val levelFlags = this.levelFlags
    if ((levelFlags & Logging.INFO_ENABLED) != 0) true
    else if ((levelFlags & Logging.INFO_DISABLED) != 0) false
    else {
      val value = log.isInfoEnabled
      setLevel(value, Logging.INFO_ENABLED, Logging.INFO_DISABLED)
      value
    }
  }

  final def isDebugEnabled: Boolean = {
    val levelFlags = this.levelFlags
    if ((levelFlags & Logging.DEBUG_DISABLED) != 0) false
    else if ((levelFlags & Logging.DEBUG_ENABLED) != 0) true
    else {
      val value = log.isDebugEnabled
      setLevel(value, Logging.DEBUG_ENABLED, Logging.DEBUG_DISABLED)
      value
    }
  }

  final def isTraceEnabled: Boolean = {
    val levelFlags = this.levelFlags
    if ((levelFlags & Logging.TRACE_DISABLED) != 0) false
    else if ((levelFlags & Logging.TRACE_ENABLED) != 0) true
    else {
      val value = log.isTraceEnabled
      setLevel(value, Logging.TRACE_ENABLED, Logging.TRACE_DISABLED)
      value
    }
  }

  // Log methods that take only a String
  def logInfo(msg: => String): Unit = {
    if (isInfoEnabled) log.info(msg)
  }

  def logDebug(msg: => String): Unit = {
    if (isDebugEnabled) log.debug(msg)
  }

  def logTrace(msg: => String): Unit = {
    if (isTraceEnabled) log.trace(msg)
  }

  def logWarning(msg: => String): Unit = {
    if (log.isWarnEnabled) log.warn(msg)
  }

  def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (isInfoEnabled) log.info(msg, throwable)
  }

  def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (isDebugEnabled) log.debug(msg, throwable)
  }

  def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (isTraceEnabled) log.trace(msg, throwable)
  }

  def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  def logError(msg: => String, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def resetLogger(): Unit = {
    Logging.initLock.synchronized {
      log_ = null
    }
  }

  protected def initializeLogIfNecessary(): Unit = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging()
        }
      }
    }
  }

  private def initializeLogging(): Unit = {
    ClientSharedUtils.initLog4j(null, null, "org/apache/spark/log4j${version}-defaults.properties")
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private object Logging {

  private val INFO_ENABLED = 0x1
  private val INFO_DISABLED = 0x2
  private val DEBUG_ENABLED = 0x4
  private val DEBUG_DISABLED = 0x8
  private val TRACE_ENABLED = 0x10
  private val TRACE_DISABLED = 0x20

  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case _: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
