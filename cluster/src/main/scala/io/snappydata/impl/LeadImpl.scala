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
import com.pivotal.gemfirexd.internal.engine.{GfxdConstants, Misc}
import com.pivotal.gemfirexd.internal.engine.db.FabricDatabase
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager
import com.pivotal.gemfirexd.{Attribute, FabricService, NetworkInterface}
import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata._
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.util.ServiceUtils
import org.apache.thrift.transport.TTransportException
import spark.jobserver.JobServer
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.{Logging, SparkCallbacks, SparkConf, SparkContext, SparkException}
import spark.jobserver.auth.{AuthInfo, SnappyAuthenticator, User}
import spray.routing.authentication.UserPass

import scala.concurrent.Future

class LeadImpl extends ServerImpl with Lead
    with ProtocolOverrides with Logging {

  self =>

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  private val bootProperties = new Properties()

  private lazy val dls = {

    val gfCache = GemFireCacheImpl.getInstance

    if (gfCache == null || gfCache.isClosed) {
      throw new IllegalStateException("GemFire Cache not initialized")
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
  var isTestSetup = false
  def directApiInvoked: Boolean = _directApiInvoked

  private var remoteInterpreterServerClass: Class[_] = _
  private var remoteInterpreterServerObj: Any = _

  @throws[SQLException]
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit = {

    _directApiInvoked = true

    isTestSetup = bootProperties.getProperty("isTest", "false").toBoolean
    bootProperties.remove("isTest")
    try {

      val locator = {
        bootProperties.getProperty(DistributionConfig.LOCATORS_NAME) match {
          case v if v != null => v
          case _ => Property.Locators.getProperty(bootProperties)
        }
      }

      val conf = new SparkConf()
      conf.setMaster(Constant.SNAPPY_URL_PREFIX + s"$locator").
          setAppName("SnappyData").
          set(Property.JobServerEnabled.name, "true").
          set("spark.scheduler.mode", "FAIR").
          setIfMissing("spark.memory.manager",
            ExecutorInitiator.SNAPPY_MEMORY_MANAGER)

      Utils.setDefaultSerializerAndCodec(conf)

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
        } else k
        conf.set(key, v)
      })
      // set spark ui port to 5050 that is snappy's default
      conf.set("spark.ui.port",
        bootProperties.getProperty("spark.ui.port", LeadImpl.SPARKUI_PORT.toString))

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

      // The auth service is not yet initialized at this point.
      // So simply check the auth-provider property value.
      if (Misc.checkAuthProvider(bootProperties.asScala.asJava)) {
        logInfo("Enabling user authentication for SnappyData Pulse")
        SparkCallbacks.setAuthenticatorForJettyServer()
      }

      sparkContext = new SparkContext(conf)

      checkAndStartZeppelinInterpreter(bootProperties)

    } catch {
      case _: InterruptedException =>
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

    val password = conf.getOption(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    if (password.isDefined) conf.remove(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    logInfo("cluster configuration after overriding certain properties \n"
        + conf.toDebugString)
    if (password.isDefined) conf.set(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR,
      password.get)

    val confProps = conf.getAll
    val storeProps = ServiceUtils.getStoreProperties(confProps)
    checkAuthProvider(storeProps)

    val pass = storeProps.remove(Attribute.PASSWORD_ATTR)
    logInfo("passing store properties as " + storeProps)
    if (pass != null) storeProps.setProperty(Attribute.PASSWORD_ATTR, pass.asInstanceOf[String])
    super.start(storeProps, ignoreIfStarted = false)

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

  /**
    * @inheritdoc
    */
  override def notifyRunning() {
    logDebug("Accepting RUNNING notification")
    serverstatus = State.RUNNING
    // status file for CacheServerLauncher is updated in the LeadImpl.internalStart
    // via callback of  notifyStatusChange
  }

  private def checkAuthProvider(props: Properties): Unit = {
    doCheck(props.getProperty(Attribute.AUTH_PROVIDER))
    doCheck(props.getProperty(Attribute.SERVER_AUTH_PROVIDER))

    def doCheck(authP: String): Unit = {
      if (authP != null && !"LDAP".equalsIgnoreCase(authP)) {
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

    assert(sparkContext != null, "Mix and match of LeadService api " +
        "and SparkContext is unsupported.")
    if (!sparkContext.isStopped) {
      sparkContext.stop()
      sparkContext = null
    }
    Utils.clearDefaultSerializerAndCodec()

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
        case _ if ignoreIfPresent => // skip setting property
        case _ if overwrite => conf.set(attr, value)
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
    changeOrAppend(Property.JobServerEnabled.name, "false",
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

    val jobServerEnabled = Property.JobServerEnabled.getProperty(
      bootProperties).toBoolean
    if (_directApiInvoked && !isTestSetup) {
      assert(jobServerEnabled,
        "JobServer must have been enabled with lead.start(..) invocation")
    }
    if (jobServerEnabled) {
      logInfo("Starting job server...")

      val confFile = bootProperties.getProperty("jobserver.configFile") match {
        case null => Array[String]()
        case c => Array(c)
      }

      configureAuthenticatorForSJS()
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

  def configureAuthenticatorForSJS(): Unit = {
    if (Misc.isSecurityEnabled) {
      logInfo("Configuring authenticator for Snappy Job users.")
      SnappyAuthenticator.auth = new SnappyAuthenticator {

        import scala.concurrent.ExecutionContext.Implicits.global

        override def authenticate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
          Future { checkCredentials(userPass) }
        }

        def checkCredentials(userPass: Option[UserPass]): Option[AuthInfo] = {
          userPass match {
            case Some(u) =>
              try {
                val props = new Properties()
                props.setProperty(Attribute.USERNAME_ATTR, u.user)
                props.setProperty(Attribute.PASSWORD_ATTR, u.pass)
                val result = FabricDatabase.getAuthenticationServiceBase.authenticate(Misc
                    .getMemStoreBooting.getDatabaseName, props)
                if (result != null) {
                  val msg = s"ACCESS DENIED, user [${u.user}]. $result"
                  if (GemFireXDUtils.TraceAuthentication) {
                    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AUTHENTICATION, msg)
                  }
                  logInfo(msg)
                  None
                } else {
                  Option(new AuthInfo(User(u.user, u.pass)))
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
      assert (value.indexOf(replaceString) >= 0)
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

  val SPARKUI_PORT = 5050
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
