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
package io.snappydata.impl

import java.lang.reflect.{Constructor, Method}
import java.net.{URL, URLClassLoader}
import java.security.Permission
import java.sql.SQLException
import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import akka.actor.ActorSystem
import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.{CacheServerLauncher, Status}
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.{Attribute, Constants, FabricService, NetworkInterface}
import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata.Constant.{SPARK_PREFIX, SPARK_SNAPPY_PREFIX, JOBSERVER_PROPERTY_PREFIX => JOBSERVER_PREFIX, PROPERTY_PREFIX => SNAPPY_PREFIX, STORE_PROPERTY_PREFIX => STORE_PREFIX}
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.recovery.RecoveryService
import io.snappydata.util.ServiceUtils
import io.snappydata.{Constant, Lead, LocalizedMessages, Property, ProtocolOverrides, ServiceManager, SnappyTableStatsProviderService}
import org.apache.thrift.transport.TTransportException
import spark.jobserver.JobServer
import spark.jobserver.auth.{AuthInfo, SnappyAuthenticator, User}
import spray.routing.authentication.UserPass
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.SecurityUtils
import org.apache.spark.sql.hive.thriftserver.SnappyHiveThriftServer2
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.{Logging, SparkCallbacks, SparkConf, SparkContext, SparkException}

class LeadImpl extends ServerImpl with Lead
    with ProtocolOverrides with Logging {

  self =>

  val DEFAULT_LEADER_MEMBER_WEIGHT_NAME = "gemfire.member-weight"

  val DEFAULT_LEADER_MEMBER_WEIGHT = "17"

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  private val bootProperties = new Properties()

  private var notifyStatusChange: FabricService.State => Unit = _

  @volatile private var servicesStarted: Boolean = _

  var _directApiInvoked: Boolean = false
  var isTestSetup = false

  def directApiInvoked: Boolean = _directApiInvoked

  private var remoteInterpreterServerClass: Class[_] = _
  private var remoteInterpreterServerObj: Any = _

  var urlclassloader: ExtendibleURLClassLoader = _

  private def setPropertyIfAbsent(props: Properties, name: String, value: => String): Unit = {
    if (!props.containsKey(name)) props.setProperty(name, value)
  }

  @throws[SQLException]
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit = {
    _directApiInvoked = true

    isTestSetup = bootProperties.getProperty("isTest", "false").toBoolean
    bootProperties.remove("isTest")
    val authSpecified = Misc.checkLDAPAuthProvider(bootProperties)

    ServiceUtils.setCommonBootDefaults(bootProperties, forLocator = false)

    // prefix all store properties with "snappydata.store" for SparkConf

    // first the passed in bootProperties
    val propNames = bootProperties.stringPropertyNames().iterator()
    while (propNames.hasNext) {
      val propName = propNames.next()
      if (propName.startsWith(SPARK_PREFIX)) {
        if (propName.startsWith(SPARK_SNAPPY_PREFIX)) {
          // remove the "spark." prefix for uniformity (e.g. when looking up a property)
          bootProperties.setProperty(propName.substring(SPARK_PREFIX.length),
            bootProperties.getProperty(propName))
          bootProperties.remove(propName)
        }
      } else if (!propName.startsWith(SNAPPY_PREFIX) &&
          !propName.startsWith(JOBSERVER_PREFIX) &&
          !propName.startsWith("zeppelin.") &&
          !propName.startsWith("hive.")) {
        bootProperties.setProperty(STORE_PREFIX + propName, bootProperties.getProperty(propName))
        bootProperties.remove(propName)
      }
    }
    // next the system properties that cannot override above
    val sysProps = System.getProperties
    val sysPropNames = sysProps.stringPropertyNames().iterator()
    // check if user has set gemfire.member-weight property
    if (System.getProperty(DEFAULT_LEADER_MEMBER_WEIGHT_NAME) eq null) {
      System.setProperty(DEFAULT_LEADER_MEMBER_WEIGHT_NAME, DEFAULT_LEADER_MEMBER_WEIGHT)
    }

    while (sysPropNames.hasNext) {
      val sysPropName = sysPropNames.next()
      if (sysPropName.startsWith(SPARK_PREFIX)) {
        if (sysPropName.startsWith(SPARK_SNAPPY_PREFIX)) {
          // remove the "spark." prefix for uniformity (e.g. when looking up a property)
          setPropertyIfAbsent(bootProperties, sysPropName.substring(SPARK_PREFIX.length),
            sysProps.getProperty(sysPropName))
        } else {
          setPropertyIfAbsent(bootProperties, sysPropName, sysProps.getProperty(sysPropName))
        }
      } else if (sysPropName.startsWith(SNAPPY_PREFIX) ||
          sysPropName.startsWith(JOBSERVER_PREFIX) ||
          sysPropName.startsWith("zeppelin.") ||
          sysPropName.startsWith("hive.")) {
        setPropertyIfAbsent(bootProperties, sysPropName, sysProps.getProperty(sysPropName))
      }
    }

    // add default lead properties that cannot be overridden
    val serverGroupsProp = STORE_PREFIX + Attribute.SERVER_GROUPS
    val groups = bootProperties.getProperty(serverGroupsProp) match {
      case null => LeadImpl.LEADER_SERVERGROUP
      case v => v + ',' + LeadImpl.LEADER_SERVERGROUP
    }
    bootProperties.setProperty(serverGroupsProp, groups)
    bootProperties.setProperty(STORE_PREFIX + Attribute.GFXD_HOST_DATA, "false")
    bootProperties.setProperty(STORE_PREFIX + Attribute.GFXD_PERSIST_DD, "false")

    // copy store related properties into a separate properties bag
    // to be used by store boot while original will be used by SparkConf
    val storeProperties = ServiceUtils.getStoreProperties(bootProperties.stringPropertyNames()
        .iterator().asScala.map(k => k -> bootProperties.getProperty(k)).toSeq)

    val productName = {
      if (SnappySession.isEnterpriseEdition) {
        "TIBCO ComputeDB"
      } else {
        "SnappyData"
      }
    }

    // initialize store and Spark in parallel (Spark will wait in
    // cluster manager start on internalStart)
    val initServices = Future {
      val locator = bootProperties.getProperty(Property.Locators.name)
      val conf = new SparkConf(false) // system properties already in bootProperties
      conf.setMaster(s"${Constant.SNAPPY_URL_PREFIX}$locator").
          setAppName(productName).
          set(Property.JobServerEnabled.name, "true").
          set("spark.scheduler.mode", "FAIR").
          setIfMissing("spark.memory.manager",
            ExecutorInitiator.SNAPPY_MEMORY_MANAGER)

      Utils.setDefaultSerializerAndCodec(conf)

      conf.setAll(bootProperties.asScala)
      // set spark ui port to 5050 that is snappy's default
      conf.set("spark.ui.port",
        bootProperties.getProperty("spark.ui.port", LeadImpl.SPARKUI_PORT.toString))

      // wait for log service to initialize so that Spark also uses the same
      while (!ClientSharedUtils.isLoggerInitialized && status() != State.RUNNING) {
        Thread.sleep(50)
      }
      resetLogger()

      val zeppelinEnabled = bootProperties.getProperty(
        Constant.ENABLE_ZEPPELIN_INTERPRETER, "false").equalsIgnoreCase("true")
      if (zeppelinEnabled && !authSpecified) {
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

      // The auth service is not yet initialized at this point.
      // So simply check the auth-provider property value.
      if (authSpecified) {
        logInfo("Enabling user authentication for SnappyData Pulse")
        SparkCallbacks.setAuthenticatorForJettyServer()
      }

      // take out the password property from SparkConf so that it is not logged
      // or seen by Spark layer
      val passwordKey = STORE_PREFIX + Attribute.PASSWORD_ATTR
      val password = conf.getOption(passwordKey)
      password match {
        case Some(_) => conf.remove(passwordKey)
        case _ =>
      }

      val parent = Thread.currentThread().getContextClassLoader
      urlclassloader = new ExtendibleURLClassLoader(parent)
      Thread.currentThread().setContextClassLoader(urlclassloader)

      val sc = new SparkContext(conf)

      // This will use GfxdDistributionAdvisor#distributeProfileUpdate
      // which inturn will create a new profile object via #instantiateProfile
      // whereby ClusterCallbacks#getDriverURL should be now returning
      // the correct URL given SparkContext is fully initialized.
      logInfo("About to send profile update after initialization completed.")
      ServerGroupUtils.sendUpdateProfile()

      val startHiveServer = Property.HiveServerEnabled.get(conf)
      val startHiveServerDefault = Property.HiveServerEnabled.defaultValue.get &&
          !conf.contains(Property.HiveServerEnabled.name)
      val useHiveSession = Property.HiveServerUseHiveSession.get(conf)
      val hiveSessionKind = if (useHiveSession) "session=hive" else "session=snappy"

      var jobServerWait = false
      var confFile: Array[String] = null
      var jobServerConfig: Config = null
      var startupString: String = null
      if (Property.JobServerEnabled.get(conf)) {
        jobServerWait = (!startHiveServerDefault && startHiveServer) ||
            Property.JobServerWaitForInit.get(conf)
        confFile = conf.getOption("jobserver.configFile") match {
          case None => Array[String]()
          case Some(c) => Array(c)
        }
        jobServerConfig = getConfig(confFile)
        val bindAddress = jobServerConfig.getString("spark.jobserver.bind-address")
        val port = jobServerConfig.getInt("spark.jobserver.port")
        startupString = s"job server on: $bindAddress[$port]"
      }
      // add default startup message for hive-thriftserver
      if (startHiveServerDefault) {
        addStartupMessage(s"Starting hive thrift server ($hiveSessionKind)")
      }
      if (!jobServerWait) {
        // mark RUNNING (job server and zeppelin will continue to start in background)
        markLauncherRunning(if (startupString ne null) s"Starting $startupString" else null)
      }

      // Add a URL classloader to the main thread so that new URIs can be added

      // wait for a while until servers get registered
      val endWait = System.currentTimeMillis() + 120000
      while (!SnappyContext.hasServerBlockIds && System.currentTimeMillis() <= endWait) {
        Thread.sleep(100)
      }
      // initialize global state
      password match {
        case Some(p) =>
          // set the password back and remove after initialization
          SparkCallbacks.setSparkConf(sc, passwordKey, p)
          SnappyContext(sc)
          SparkCallbacks.setSparkConf(sc, passwordKey, value = null)

        case _ => SnappyContext(sc)
      }

      // start the service to gather table statistics
      SnappyTableStatsProviderService.start(sc, url = null)

      if (startHiveServer) {
        val hiveService = SnappyHiveThriftServer2.start(useHiveSession)
        if (jobServerWait) SnappyHiveThriftServer2.getHostPort(hiveService) match {
          case None => addStartupMessage(s"Started hive thrift server ($hiveSessionKind)")
          case Some((host, port)) =>
            addStartupMessage(s"Started hive thrift server ($hiveSessionKind) on: $host[$port]")
        }
      }

      // update the Spark UI to add the dashboard and other SnappyData pages
      ToolsCallbackInit.toolsCallback.updateUI(sc)

      // start other add-on services (job server)
      startAddOnServices(conf, confFile, jobServerConfig)

      // finally start embedded zeppelin interpreter if configured and security is not enabled.
      if (!authSpecified) {
        checkAndStartZeppelinInterpreter(zeppelinEnabled, bootProperties)
      }

      if (jobServerWait) {
        // mark RUNNING after job server, hive server and zeppelin initialization if so configured
        markLauncherRunning(if (startupString ne null) s"Started $startupString" else null)
      }
    }

    try {
      internalStart(() => storeProperties)
      Await.result(initServices, Duration.Inf)
      // If recovery mode then initialize the recovery service
      if(Misc.getGemFireCache.isSnappyRecoveryMode) {
        RecoveryService.collectViewsAndRecoverDDLs();
      }
      // mark status as RUNNING at the end in any case
      markRunning()
    } catch {
      case _: InterruptedException =>
        logInfo(s"Thread interrupted, aborting.")
      case e: Throwable =>
        logWarning("Exception while starting lead node", e)
        throw e
    }
  }

  @throws[SparkException]
  private def internalStart(initStoreProps: () => Properties): Unit = synchronized {
    if (status() != State.UNINITIALIZED && status() != State.STOPPED) {
      // already started or in the process of starting
      return
    }
    val storeProps = initStoreProps()
    checkAuthProvider(storeProps)

    super.start(storeProps, ignoreIfStarted = false)

    resetLogger()

    val cache = Misc.getGemFireCache
    cache.getDistributionManager.addMembershipListener(SnappyContext.membershipListener)

    status() match {
      case State.RUNNING =>
        bootProperties.putAll(storeProps)
        logInfo("ds connected. About to check for primary lead lock.")
        // check for leader's primary election

        val dls = DLockService.create(LOCK_SERVICE_NAME, cache.getDistributedSystem,
          true, true, true)
        val primaryLeaderLock = new DistributedMemberLock(dls,
          LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
          DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)

        val startStatus = primaryLeaderLock.tryLock()
        // noinspection SimplifyBooleanMatch
        startStatus match {
          case true =>
            logInfo("Primary lead lock acquired.")
          // let go.
          case false =>
            if (!_directApiInvoked) {
              // cleanup before throwing exception
              internalStop(bootProperties)
              throw new SparkException("Primary Lead node (Spark Driver) is " +
                  "already running in the system. You may use smart connector " +
                  "mode to connect to SnappyData cluster.")
            }
            serverstatus = State.STANDBY
            val callback = notifyStatusChange
            if (callback != null) {
              logInfo("Notifying standby status ...")
              callback(serverstatus)
            }

            logInfo("Primary Lead node (Spark Driver) is already running in the system." +
                " Standing by as secondary.")
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

  override def serviceStatus(): State = {
    // show as running only after everything has initialized
    status() match {
      case State.RUNNING if !servicesStarted => State.STARTING
      case state => state
    }
  }

  private def markRunning(): Unit = {
    if (GemFireXDUtils.TraceFabricServiceBoot) {
      logInfo("Accepting RUNNING notification")
    }
    notifyRunningInLauncher(Status.RUNNING)
    serverstatus = State.RUNNING
    servicesStarted = true
  }

  private def addStartupMessage(message: String): Unit = {
    if ((message ne null) && !message.isEmpty) {
      val launcher = CacheServerLauncher.getCurrentInstance
      if (launcher ne null) {
        val startupMessage = launcher.getServerStartupMessage
        if (startupMessage eq null) {
          launcher.setServerStartupMessage(message)
        } else {
          launcher.setServerStartupMessage(startupMessage + "\n  " + message)
        }
      }
    }
  }

  private def markLauncherRunning(message: String): Unit = {
    addStartupMessage(message)
    notifyRunningInLauncher(Status.RUNNING)
  }

  private def checkAuthProvider(props: Properties): Unit = {
    doCheck(props.getProperty(Attribute.AUTH_PROVIDER))
    doCheck(props.getProperty(Attribute.SERVER_AUTH_PROVIDER))

    def doCheck(authP: String): Unit = {
      if (authP != null && !Constants.AUTHENTICATION_PROVIDER_LDAP.equalsIgnoreCase(authP) &&
          !"NONE".equalsIgnoreCase(authP)) {
        throw new UnsupportedOperationException(
          "LDAP is the only supported auth-provider currently.")
      }
      if (authP != null && !SnappySession.isEnterpriseEdition) {
        throw new UnsupportedOperationException("Security feature is available in SnappyData " +
            "Enterprise Edition.")
      }
    }
  }

  @throws[SQLException]
  override def stop(shutdownCredentials: Properties): Unit = {
    /* (sample reservoir region is now persistent by default)
    val servers = GemFireXDUtils.getGfxdAdvisor.adviseDataStores(null)
    if (servers.size() > 0) {
      SnappyContext.flushSampleTables()
    }
    */
    if (shutdownCredentials eq null) internalStop(null)
    else internalStop(ServiceUtils.getStoreProperties(shutdownCredentials.asScala.toSeq))
  }

  private[snappydata] def internalStop(shutdownCredentials: Properties): Unit = {
    if (!servicesStarted && bootProperties.isEmpty) return

    try {
      Misc.getGemFireCache.getDistributionManager
          .removeMembershipListener(SnappyContext.membershipListener)
    } catch {
      case _: CacheClosedException =>
    }
    SnappyHiveThriftServer2.close()
    val sc = SnappyContext.globalSparkContext
    if (sc != null) sc.stop()
    servicesStarted = false
    // TODO: [soubhik] find a way to stop jobserver.
    if (null != remoteInterpreterServerObj) {
      val method: Method = remoteInterpreterServerClass.getMethod("isAlive")
      val isAlive: java.lang.Boolean = method.invoke(remoteInterpreterServerObj)
          .asInstanceOf[java.lang.Boolean]
      val shutdown: Method = remoteInterpreterServerClass.getMethod("shutdown",
        classOf[java.lang.Boolean])

      if (isAlive) {
        shutdown.invoke(remoteInterpreterServerObj, true.asInstanceOf[AnyRef])
      }
    }
    val sys = InternalDistributedSystem.getConnectedInstance
    if (sys ne null) {
      try {
        super.stop(shutdownCredentials)
      } catch {
        case sqle: SQLException =>
          val sqlState = sqle.getSQLState
          if (SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN.startsWith(sqlState)
              || SQLState.SHUTDOWN_DATABASE.startsWith(sqlState)
              || SQLState.GFXD_NODE_SHUTDOWN.startsWith(sqlState)) {
            // ignore if already stopped
          } else throw sqle
        case _: CancelException => // ignore if already stopped
      }
    }
    bootProperties.clear()
  }

  private[snappydata] def initStartupArgs(conf: SparkConf, sc: SparkContext = null) = {

    def changeOrAppend(attr: String, value: String,
        overwrite: Boolean = false, ignoreIfPresent: Boolean = false,
        sparkPrefix: String = null): Unit = {
      val attrKey = if (sparkPrefix == null) attr else sparkPrefix + attr
      conf.getOption(attrKey) match {
        case None => if (sparkPrefix == null) {
          changeOrAppend(attr, value, overwrite, ignoreIfPresent,
            sparkPrefix = SPARK_PREFIX)
        } else conf.set(attr, value)
        case _ if ignoreIfPresent => // skip setting property
        case _ if overwrite => conf.set(attr, value)
        case Some(v) =>
          // ignore if already set
          val prefixedValue = "," + value
          if (v != value && !value.contains(prefixedValue)) conf.set(attr, v + prefixedValue)
      }
    }

    changeOrAppend(STORE_PREFIX + Attribute.SERVER_GROUPS, LeadImpl.LEADER_SERVERGROUP)

    assert(Property.Locators.getOption(conf).orElse(
      Property.McastPort.getOption(conf)).isDefined,
      s"Either ${Property.Locators} or ${Property.McastPort} " +
          s"must be defined for SnappyData cluster to start")
    // skip overriding host-data if loner VM.
    if (sc != null && Utils.isLoner(sc)) {
      changeOrAppend(STORE_PREFIX + Attribute.GFXD_HOST_DATA,
        "true", overwrite = true)
    } else {
      changeOrAppend(STORE_PREFIX + Attribute.GFXD_HOST_DATA,
        "false", overwrite = true)
      changeOrAppend(STORE_PREFIX + Attribute.GFXD_PERSIST_DD,
        "false", overwrite = true)
    }
    changeOrAppend(Property.JobServerEnabled.name, "false",
      ignoreIfPresent = true)

    conf
  }

  protected[snappydata] def notifyOnStatusChange(f: FabricService.State => Unit): Unit =
    this.notifyStatusChange = f

  @throws[Exception]
  private def startAddOnServices(conf: SparkConf,
      confFile: Array[String], jobServerConfig: Config): Unit = this.synchronized {
    if (_directApiInvoked && !isTestSetup) {
      assert(jobServerConfig ne null,
        "JobServer must have been enabled with lead.start(..) invocation")
    }
    if (jobServerConfig ne null) {
      logInfo("Starting job server...")

      configureAuthenticatorForSJS()
      JobServer.start(confFile, _ => jobServerConfig, createActorSystem)
    }
  }

  def configureAuthenticatorForSJS(): Unit = {
    if (Misc.isSecurityEnabled) {
      logInfo("Configuring authenticator for Snappy Job users.")
      SnappyAuthenticator.auth = new SnappyAuthenticator {

        override def authenticate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
          Future(checkCredentials(userPass))
        }

        def checkCredentials(userPass: Option[UserPass]): Option[AuthInfo] = {
          userPass match {
            case Some(u) =>
              try {
                SecurityUtils.checkCredentials(u.user, u.pass) match {
                  case None => Option(new AuthInfo(User(u.user, u.pass)))
                  case _ => None
                }
              } catch {
                case t: Throwable => logWarning(s"Failed to authenticate the snappy job. $t")
                  None
              }
            case None => None
          }
        }
      }
    }
  }

  def getConfig(args: Array[String]): Config = {

    val notConfigurable = ConfigFactory.parseProperties(getDynamicOverrides).
        withFallback(ConfigFactory.parseResources("jobserver-overrides.conf"))

    val bootConfig = notConfigurable.withFallback(ConfigFactory.parseProperties(bootProperties))

    val snappyDefaults = bootConfig.withFallback(
      ConfigFactory.parseResources("jobserver-defaults.conf"))

    val builtIn = ConfigFactory.load()

    val finalConf = snappyDefaults.withFallback(builtIn).resolve()

    logDebug("Passing JobServer with config " + finalConf.root.render())

    finalConf
  }

  def getDynamicOverrides: Properties = {
    val dynamicOverrides = new Properties()
    val replaceString = "<basedir>"

    def replace(key: String, value: String, newValue: String) = {
      assert(value.indexOf(replaceString) >= 0)
      dynamicOverrides.setProperty(key, value.replace(replaceString, newValue))
    }

    val workingDir = System.getProperty(
      com.pivotal.gemfirexd.internal.iapi.reference.Property.SYSTEM_HOME_PROPERTY, ".")
    val defaultConf = ConfigFactory.parseResources("jobserver-defaults.conf")

    var key = "spark.jobserver.filedao.rootdir"
    replace(key, defaultConf.getString(key), workingDir)
    key = "spark.jobserver.datadao.rootdir"
    replace(key, defaultConf.getString(key), workingDir)

    val overrideConf = ConfigFactory.parseResources("jobserver-overrides.conf")
    key = "spark.jobserver.sqldao.rootdir"
    replace(key, overrideConf.getString(key), workingDir)

    dynamicOverrides
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
   * By default, zeppelin interpreter will be disabled. User can enable it by
   * setting "zeppelin.interpreter.enable" to true in leads conf file. User can also specify
   * the port on which interpreter should listen using  property "zeppelin.interpreter.port"
   */
  private def checkAndStartZeppelinInterpreter(enabled: Boolean,
      bootProperties: Properties): Unit = {
    // As discussed ZeppelinRemoteInterpreter Server will be disabled by default.
    // [sumedh] Our startup times are already very high and we are looking to
    // cut that down and not increase further with these external utilities.
    if (enabled) {
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
        case t: Throwable => logWarning("Error starting zeppelin interpreter.Actual exception : " +
            t.getMessage, t)
      }
      // Add memory listener for zeppelin will need it for zeppelin
      // val listener = new LeadNodeMemoryListener();
      // Misc.getGemFireCache.getResourceManager.
      //   addResourceListener(InternalResourceManager.ResourceType.ALL, listener)

    }
  }

  class NoExitSecurityManager extends SecurityManager {
    override def checkExit(status: Int): Unit = {
      throw new SecurityException("exit not allowed")
    }

    override def checkPermission(perm: Permission): Unit = {
      // Allow other activities by default
    }
  }

  def closeAndReopenInterpreterServer(): Unit = {
    if (remoteInterpreterServerClass != null) {
      val origSecurityManager = System.getSecurityManager
      System.setSecurityManager(new NoExitSecurityManager)
      try {
        remoteInterpreterServerClass.getSuperclass.
            getDeclaredMethod("shutdown").invoke(remoteInterpreterServerObj)
      } finally {
        System.setSecurityManager(origSecurityManager)
      }
      checkAndStartZeppelinInterpreter(enabled = true, bootProperties)
    }
  }

  def getInterpreterServerClass: Class[_] = {
    remoteInterpreterServerClass
  }
}

class ExtendibleURLClassLoader(parent: ClassLoader)
    extends URLClassLoader(Array.empty[URL], parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }

  override def getURLs: Array[URL] = super.getURLs
}

object LeadImpl {

  val SPARKUI_PORT: Int = 5050
  val LEADER_SERVERGROUP: String = ServerGroupUtils.LEADER_SERVERGROUP

  def invokeLeadStart(conf: SparkConf): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStart(() => ServiceUtils.getStoreProperties(conf.getAll))
  }

  def invokeLeadStop(): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStop(lead.bootProperties)
  }
}
