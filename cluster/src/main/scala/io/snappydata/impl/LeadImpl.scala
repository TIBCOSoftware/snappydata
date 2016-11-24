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
package io.snappydata.impl

import java.lang
import java.lang.reflect.{Constructor, Method}
import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import akka.actor.ActorSystem
import com.gemstone.gemfire.distributed.internal.DistributionConfig
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils
import com.pivotal.gemfirexd.{FabricService, NetworkInterface}
import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata._
import io.snappydata.util.ServiceUtils
import org.apache.thrift.transport.TTransportException
import spark.jobserver.JobServer

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException}

class LeadImpl extends ServerImpl with Lead with Logging {

  self =>

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  private val bootProperties = new Properties()

  private lazy val dls = {

    val gfCache = GemFireCacheImpl.getInstance

    if (gfCache == null || gfCache.isClosed) {
      throw new Exception("GemFire Cache not initialized")
    }

    val dSys = gfCache.getDistributedSystem

    DLockService.create(LOCK_SERVICE_NAME, dSys, true, true, true)
  }

  private var sparkContext: SparkContext = _

  private var notifyStatusChange: ((FabricService.State) => Unit) = _

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)

  private[snappydata] val snappyProperties = Utils.getFields(Property).collect {
    case (_, SparkProperty(prop)) => prop
    case (_, SnappySparkProperty(prop)) => prop
    case (_, SparkSQLProperty(prop)) => prop
    case (_, SnappySparkSQLProperty(prop)) => prop
    case _ => ""
  }.toSet

  var _directApiInvoked: Boolean = false

  def directApiInvoked: Boolean = _directApiInvoked

  private var remoteInterpreterServerClass: Class[_] = _
  private var remoteInterpreterServerObj: Any = _

  @throws[SQLException]
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit = {

    _directApiInvoked = true

    try {

      val locator = {
        bootProperties.getProperty(DistributionConfig.LOCATORS_NAME) match {
          case v if v != null => v
          case _ => Property.Locators.getProperty(bootProperties)
        }
      }

      val conf = new SparkConf()
      conf.setMaster(Constant.SNAPPY_URL_PREFIX + s"$locator").
          setAppName("leaderLauncher").
          set(Property.JobserverEnabled.name, "true").
          set("spark.scheduler.mode", "FAIR")

      Utils.setDefaultSerializerAndCodec(conf)

      // disable somewhat expensive CallSite.longForm
      if (System.getProperty("spark.callstack.depth") == null) {
        System.setProperty("spark.callstack.depth", "1")
      }

      // inspect user input and add appropriate prefixes
      // if property doesn't contain '.'
      // if input prop key is found in io.snappydata.Property,
      // its prefixed with 'snappydata.' otherwise its assumed
      // to be snappydata.store.
      bootProperties.asScala.foreach({ case (k, v) =>
        val key = if (k.indexOf(".") < 0) {
          if (snappyProperties(k)) {
            Constant.PROPERTY_PREFIX + k
          } else {
            Constant.STORE_PROPERTY_PREFIX + k
          }
        }
        else {
          k
        }
        conf.set(key, v)
      })


      if (bootProperties.getProperty(Constant.ENABLE_ZEPPELIN_INTERPRETER,
        "false").equalsIgnoreCase("true")) {

        try {

          val zeppelinIntpUtilClass = Utils.classForName(
            "org.apache.zeppelin.interpreter.ZeppelinIntpUtil")

          /**
           * This will initialize the zeppelin repl interpreter.
           * This should be done before spark context is created as zeppelin
           * interpreter will set some properties for classloader for repl
           * which needs to be specified while creating sparkcontext in lead
           */
          logInfo("About to initialize SparkContext with SparkConf")
          val method: Method = zeppelinIntpUtilClass.getMethod(
            "initializeZeppelinReplAndGetConfig")
          val obj: Object = method.invoke(null)
          val props: Properties = obj.asInstanceOf[Properties]
          props.asScala.foreach(kv => conf.set(kv._1, kv._2))
        } catch {
          /* [Sachin] So we need to log warning that
          interpreter not started or do we need to exit? */
          case e: Throwable => logWarning("Cannot find zeppelin interpreter in the classpath")
            throw e;
        }
      }
      sparkContext = new SparkContext(conf)

      checkAndStartZeppelinInterpreter(bootProperties)

    } catch {
      case ie: InterruptedException =>
        logInfo(s"Thread interrupted, aborting.")
      case e: Throwable =>
        logWarning("Exception while starting lead node", e)
        throw e
    }


  }

  @throws[SparkException]
  private[snappydata] def internalStart(sc: SparkContext): Unit = {

    val conf = sc.getConf // this will get you a cloned copy
    initStartupArgs(conf, sc)

    logInfo("cluster configuration after overriding certain properties \n"
        + conf.toDebugString)

    val confProps = conf.getAll
    val storeProps = ServiceUtils.getStoreProperties(confProps)

    logInfo("passing store properties as " + storeProps)
    super.start(storeProps, false)

    status() match {
      case State.RUNNING =>
        bootProperties.putAll(confProps.toMap.asJava)
        logInfo("ds connected. About to check for primary lead lock.")
        // check for leader's primary election

        val startStatus = primaryLeaderLock.tryLock()

        startStatus match {
          case true =>
            logInfo("Primary lead lock acquired.")
          // let go.
          case false =>
            if (!_directApiInvoked) {
              // cleanup before throwing exception
              internalStop(bootProperties)
              throw new SparkException("Primary Lead node (Spark Driver) is " +
                  "already running in the system. You may use split cluster " +
                  "mode to connect to SnappyData cluster.")
            }
            serverstatus = State.STANDBY
            val callback = notifyStatusChange
            if (callback != null) {
              logInfo("Notifying standby status ...")
              callback(serverstatus)
            }

            logInfo("Primary Lead node (Spark Driver) is already running in the system." +
                "Standing by as secondary.")
            primaryLeaderLock.lockInterruptibly()

            // TODO: check cancelInProgress and other shutdown possibilities.

            logInfo("Resuming startup sequence from STANDBY ...")
            serverstatus = State.STARTING
            if (callback != null) {
              callback(serverstatus)
            }
        }
      case _ =>
        logWarning(LocalizedMessages.res.getTextMessage("SD_LEADER_NOT_READY", status()))
    }
  }

  @throws[SQLException]
  override def stop(shutdownCredentials: Properties): Unit = {
    val servers = GemFireXDUtils.getGfxdAdvisor.adviseDataStores(null)
    if (servers.size() > 0) {
      SnappyContext.flushSampleTables()
    }

   assert(sparkContext != null, "Mix and match of LeadService api " +
        "and SparkContext is unsupported.")
    if (!sparkContext.isStopped) {
      sparkContext.stop()
      sparkContext = null
    }

    if (null != remoteInterpreterServerObj) {
      val method: Method = remoteInterpreterServerClass.getMethod("isAlive")
      val isAlive: lang.Boolean = method.invoke(remoteInterpreterServerObj)
          .asInstanceOf[lang.Boolean]
      val shutdown: Method = remoteInterpreterServerClass.getMethod("shutdown",
        classOf[lang.Boolean])

      if (isAlive) {
        shutdown.invoke(remoteInterpreterServerObj, true.asInstanceOf[AnyRef])
      }
    }
  }

  private[snappydata] def internalStop(shutdownCredentials: Properties): Unit = {
    bootProperties.clear()
    val sc = SnappyContext.globalSparkContext
    if (sc != null) sc.stop()
    // TODO: [soubhik] find a way to stop jobserver.
    sparkContext = null
    if (null != remoteInterpreterServerObj) {
      val method: Method = remoteInterpreterServerClass.getMethod("isAlive")
      val isAlive: lang.Boolean = method.invoke(remoteInterpreterServerObj)
          .asInstanceOf[lang.Boolean]
      val shutdown: Method = remoteInterpreterServerClass.getMethod("shutdown",
        classOf[lang.Boolean])

      if (isAlive) {
        shutdown.invoke(remoteInterpreterServerObj, true.asInstanceOf[AnyRef])
      }
    }
    super.stop(shutdownCredentials)
  }

  private[snappydata] def initStartupArgs(conf: SparkConf, sc: SparkContext = null) = {

    def changeOrAppend(attr: String, value: String,
        overwrite: Boolean = false, ignoreIfPresent: Boolean = false,
        sparkPrefix: String = null): Unit = {
      val attrKey = if (sparkPrefix == null) attr else sparkPrefix + attr
      conf.getOption(attrKey) match {
        case None => if (sparkPrefix == null) {
          changeOrAppend(attr, value, overwrite, ignoreIfPresent,
            sparkPrefix = Constant.SPARK_PREFIX)
        } else conf.set(attr, value)
        case v if ignoreIfPresent => // skip setting property
        case v if overwrite => conf.set(attr, value)
        case Some(x) => conf.set(attr, x ++ s""",$value""")
      }
    }

    changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
        com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, LeadImpl.LEADER_SERVERGROUP)

    assert(Property.Locators.getOption(conf).orElse(
      Property.McastPort.getOption(conf)).isDefined,
      s"Either ${Property.Locators} or ${Property.McastPort} " +
          s"must be defined for SnappyData cluster to start")
    import org.apache.spark.sql.collection.Utils
    // skip overriding host-data if loner VM.
    if (sc != null && Utils.isLoner(sc)) {
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA,
        "true", overwrite = true)
    } else {
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA,
        "false", overwrite = true)
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD,
        "false", overwrite = true)
    }
    changeOrAppend(Property.JobserverEnabled.name, "false",
      ignoreIfPresent = true)

    conf
  }

  protected[snappydata] def notifyOnStatusChange(f: (FabricService.State) => Unit): Unit =
    this.notifyStatusChange = f

  @throws[Exception]
  private[snappydata] def startAddOnServices(sc: SparkContext): Unit = this.synchronized {
    LeadImpl.setInitializingSparkContext(sc)

    if (status() == State.UNINITIALIZED || status() == State.STOPPED) {
      // for SparkContext.setMaster("local[xx]"), ds.connect won't happen
      // until now.
      logInfo("Connecting to snappydata cluster now...")
      internalStart(sc)
    }

    val jobServerEnabled = Property.JobserverEnabled.getProperty(
      bootProperties).toBoolean
    if (_directApiInvoked) {
      assert(jobServerEnabled,
        "JobServer must have been enabled with lead.start(..) invocation")
    }
    if (jobServerEnabled) {
      logInfo("Starting job server...")

      val confFile = bootProperties.getProperty("jobserver.configFile") match {
        case null => Array[String]()
        case c => Array(c)
      }

      JobServer.start(confFile, getConfig, createActorSystem)
    }

    // This will use GfxdDistributionAdvisor#distributeProfileUpdate
    // which inturn will create a new profile object via #instantiateProfile
    // whereby ClusterCallbacks#getDriverURL should be now returning
    // the correct URL given SparkContext is fully initialized.
    logInfo("About to send profile update after initialization completed.")
    ServerGroupUtils.sendUpdateProfile()

    LeadImpl.clearInitializingSparkContext()
  }

  def getConfig(args: Array[String]): Config = {

    System.setProperty("config.trace", "loads")

    val notConfigurable = ConfigFactory.parseResources("jobserver-overrides.conf")

    val bootConfig = notConfigurable.withFallback(ConfigFactory.parseProperties(bootProperties))

    val snappyDefaults = bootConfig.withFallback(
      ConfigFactory.parseResources("jobserver-defaults.conf"))

    val builtIn = ConfigFactory.load()

    val finalConf = snappyDefaults.withFallback(builtIn).resolve()

    logInfo("Passing JobServer with config " + finalConf.root.render())

    finalConf
  }


  def createActorSystem(conf: Config): ActorSystem = {
    ActorSystem("SnappyLeadJobServer", conf)
  }

  @throws[SparkException]
  override def startNetworkServer(bindAddress: String,
      port: Int,
      networkProperties: Properties): NetworkInterface = {
    throw new SparkException("Network server cannot be started on lead node.")
  }

  @throws[SparkException]
  override def startThriftServer(bindAddress: String,
      port: Int,
      networkProperties: Properties): NetworkInterface = {
    throw new SparkException("Thrift server cannot be started on lead node.")
  }

  @throws[SparkException]
  override def startDRDAServer(bindAddress: String,
      port: Int,
      networkProperties: Properties): NetworkInterface = {
    throw new SparkException("DRDA server cannot be started on lead node.")
  }

  override def stopAllNetworkServers(): Unit = {
    // nothing to do as none of the net servers are allowed to start.
  }

  /**
   * This method is used to start the zeppelin interpreter thread.
   * As discussed by default zeppelin interpreter will be enabled.User can disable it by
   * setting "zeppelin.interpreter.enable" to false in leads conf file.User can also specify
   * the port on which intrepreter should listen using  property zeppelin.interpreter.port
   */
  private def checkAndStartZeppelinInterpreter(bootProperties: Properties): Unit = {
    // As discussed ZeppelinRemoteInterpreter Server will be enabled by default.
    // [sumedh] Our startup times are already very high and we are looking to
    // cut that down and not increase further with these external utilities.
    if (bootProperties.getProperty(Constant.ENABLE_ZEPPELIN_INTERPRETER,
      "false").equalsIgnoreCase("true")) {
      val port = bootProperties.getProperty(Constant.ZEPPELIN_INTERPRETER_PORT,
        "3768").toInt
      try {
        remoteInterpreterServerClass = Utils.classForName(
          "org.apache.zeppelin.interpreter.SnappyInterpreterServer")
        val constructor: Constructor[_] = remoteInterpreterServerClass
            .getConstructor(classOf[Integer])
        remoteInterpreterServerObj = constructor.newInstance(port.asInstanceOf[AnyRef])

        remoteInterpreterServerClass.getSuperclass.getSuperclass
            .getDeclaredMethod("start").invoke(remoteInterpreterServerObj)
        logInfo(s"Starting Zeppelin RemoteInterpreter at port " + port)
      } catch {
        case tTransportException: TTransportException =>
          logWarning("Error while starting zeppelin interpreter.Actual exception : " +
              tTransportException.getMessage)
      }
      // Add memory listener for zeppelin will need it for zeppelin
      // val listener = new LeadNodeMemoryListener();
      // GemFireCacheImpl.getInstance().getResourceManager().
      //   addResourceListener(InternalResourceManager.ResourceType.ALL, listener)

    }
  }
}

object LeadImpl {

  val LEADER_SERVERGROUP = "IMPLICIT_LEADER_SERVERGROUP"
  private[this] val startingContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  def invokeLeadStart(sc: SparkContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStart(sc)
  }

  def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.startAddOnServices(sc)
  }

  def invokeLeadStop(shutdownCredentials: Properties): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStop(shutdownCredentials)
  }

  def setInitializingSparkContext(sc: SparkContext): Unit = {
    assert(sc != null)
    startingContext.set(sc)
  }

  def getInitializingSparkContext: SparkContext = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null) {
      return sc
    }

    val initSC = startingContext.get()
    assert(initSC != null)

    initSC
  }

  def clearInitializingSparkContext(): Unit = {
    startingContext.set(null)
  }
}
