package io.snappydata.cluster

import java.net.URL

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import org.apache.spark.executor.SnappyCoarseGrainedExecutorBackend
import org.apache.spark.{SparkCallbacks, SparkEnv, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * Created by hemant on 15/10/15.
 */
object ExecutorInitiator {

  val executorRunnable: ExecutorRunnable = new ExecutorRunnable

  val executorThread: Thread = new Thread(executorRunnable)

  class ExecutorRunnable() extends Runnable {
    var driverURL: Option[String] = None
    var stopTask = false

    override def run(): Unit = {
      var prevDriverURL = getDriverURL
      var env: SparkEnv = null
      while (!stopTask) {
        if (prevDriverURL == getDriverURL)
          Thread.sleep(3000)
        else {
          // kill if an executor is already running.
          SparkCallbacks.stopExecutor(env)
          env = null
          prevDriverURL = getDriverURL

          driverURL match {
            case Some(url) =>
              //TODO: Hemant:  get the spark conf here
              val executorConf = new SparkConf()
              //TODO: Hemant: get the number of cores from spark conf
              val cores = 6

              val boundport = executorConf.getInt("spark.executor.port", 0)
              val host = GemFireCacheImpl.getInstance().getMyId.getHost
              val memberId = GemFireCacheImpl.getInstance().getMyId.toString
              env = SparkCallbacks.createExecutorEnv(
                executorConf, memberId, host, boundport, cores, false)
              val rpcenv = SparkCallbacks.getRpcEnv(env)
              //TODO: Hemant: Check how to get this jar. Without this spark throws an exception
              val userClassPath = new ListBuffer[URL]()
              userClassPath += new URL("file:/hemantb1/snappy/repos/snappy-spark/sql/hive/src/test/resources/TestUDTF.jar")

              //TODO: Hemant: Check the parameters of this class
              val executor = new SnappyCoarseGrainedExecutorBackend(
                rpcenv, url, memberId, host + ":" + boundport,
                cores, userClassPath, env)

              val endPoint = rpcenv.setupEndpoint("Executor_" + memberId, executor)
            case None =>
          }
        }
      }
      // kill if an executor is already running.
      SparkCallbacks.stopExecutor(env)

    }

    def getDriverURL: String = driverURL match {
      case Some(x) => x
      case None => ""
    }
  }

  /**
   * This should be called only when the process is terminating.
   * If a process ceases to be an executor, only transmuteExecutor should be called
   * with None.
   */
  def stop() = {
    executorRunnable.stopTask = true
  }

  /**
   * Set the new driver url and start the thread if not already started
   * @param driverURL
   */
  def transmuteExecutor(driverURL: Option[String]): Unit = {

    executorRunnable.driverURL = driverURL
    // start the executor thread if driver URL is set and the thread
    // is not already started.
    driverURL match {
      case Some(x) =>
        if (executorThread.getState != Thread.State.NEW) {
          executorThread.start()
        }
      case None =>
    }
  }

}
