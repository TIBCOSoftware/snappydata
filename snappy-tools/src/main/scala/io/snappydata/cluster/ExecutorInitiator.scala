package io.snappydata.cluster

import java.io.File
import java.net.URL
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.deploy.SparkHadoopUtil

import scala.collection.mutable
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.executor.SnappyCoarseGrainedExecutorBackend
import org.apache.spark.{SparkCallbacks, SparkEnv, SparkConf}

/**
 * This class is responsible for initiating the executor process inside
 * the jvm. Also, if an executor has to be stopped, driverURL can be set as None
 * and it will take care of stopping the executor.
 *
 * Created by hemant on 15/10/15.
 */
object ExecutorInitiator {

  val executorRunnable: ExecutorRunnable = new ExecutorRunnable

  val executorThread: Thread = new Thread(executorRunnable)

  class ExecutorRunnable() extends Runnable {
    private var driverURL: Option[String] = None
    var stopTask = false
    private val lock = new ReentrantLock

    def setDriverURL(url: Option[String]) = lock.synchronized {
      driverURL = url
      lock.notify()
    }

    override def run(): Unit = {
      var prevDriverURL = ""
      var env: SparkEnv = null
      try {
        while (!stopTask) {
          try {
            Misc.checkIfCacheClosing(null)
            if (prevDriverURL ==  getDriverURL) {
              lock.synchronized {
                lock.wait()
              }
            }
            else {
              // kill if an executor is already running.
              SparkCallbacks.stopExecutor(env)
              env = null
              prevDriverURL = getDriverURL

              driverURL match {
                case Some(url) =>

                  /**
                   * The executor initialization code has been picked from CoarseGrainedExecutorBackend.
                   * We need to track the changes there and merge them here on a regular basis.
                   */
                  val executorHost = GemFireCacheImpl.getInstance().getMyId.getHost
                  val memberId = GemFireCacheImpl.getInstance().getMyId.toString
                  SparkHadoopUtil.get.runAsSparkUser { () =>

                    // Fetch the driver's Spark properties.
                    val executorConf = new SparkConf

                    //TODO: Hemant: run as sparkUser
                    val port = executorConf.getInt("spark.executor.port", 0)
                    val props = SparkCallbacks.fetchDriverProperty(executorHost, executorConf, port, url)


                    val driverConf = new SparkConf()
                    // Specify a default directory for executor, if the local directory for executor
                    // is set via the executor conf, it will override this property later in the code
                    val localDirForExecutor = new File("./" + "executor").getAbsolutePath

                    driverConf.set("spark.local.dir", localDirForExecutor)
                    for ((key, value) <- props) {
                      // this is required for SSL in standalone mode
                      if (!key.equals("spark.local.dir")) {
                        if (SparkCallbacks.isExecutorStartupConf(key)) {
                          driverConf.setIfMissing(key, value)
                        } else {
                          driverConf.set(key, value)
                        }
                      }
                    }
                    //TODO: Hemant: add executor specific properties from local conf to
                    //TODO: this conf that was received from driver.

                    //TODO: Hemant: get the number of cores from spark conf
                    val cores = 6

                    env = SparkCallbacks.createExecutorEnv(
                      driverConf, memberId, executorHost, port, cores, false)

                    // SparkEnv sets spark.executor.port so it shouldn't be 0 anymore.
                    val boundport = env.conf.getInt("spark.executor.port", 0)
                    assert(boundport != 0)

                    // This is not required with snappy
                    val userClassPath = new mutable.ListBuffer[URL]()

                    //TODO: Hemant: Check the parameters of this class
                    val rpcenv = SparkCallbacks.getRpcEnv(env)

                    val executor = new SnappyCoarseGrainedExecutorBackend(
                      rpcenv, url, memberId, executorHost + ":" + boundport,
                      cores, userClassPath, env)

                    val endPoint = rpcenv.setupEndpoint("Executor", executor)
                  }
                case None =>
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace();
              //TODO:Hemant: add a proper log statement
              System.out.println("exception " + e)
              Misc.checkIfCacheClosing(e)
          }
        }
      } finally {
        // kill if an executor is already running.
        SparkCallbacks.stopExecutor(env)
      }
    }

    def getDriverURL: String = driverURL match {
      case Some(x) => x
      case None => ""
    }
  }

  /**
   * This should be called only when the process is terminating.
   * If a process ceases to be an executor, only startOrTransmuteExecutor should be called
   * with None.
   */
  def stop() = {
    executorRunnable.stopTask = true
    executorRunnable.setDriverURL(None)
  }

  /**
   * Set the new driver url and start the thread if not already started
   * @param driverURL
   */
  def startOrTransmuteExecutor(driverURL: Option[String]): Unit = {
    executorRunnable.setDriverURL(driverURL)
    // start the executor thread if driver URL is set and the thread
    // is not already started.
    driverURL match {
      case Some(x) =>
        if (executorThread.getState == Thread.State.NEW) {
          executorThread.setDaemon(true)
          executorThread.start()
        }
      case None =>
    }
  }

}
