package org.apache.spark.scheduler.cluster

import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.{SparkContext, SparkConf}

class ClusterManagerSuite extends FunSuite with BeforeAndAfter {

  before {
    SparkContext.registerClusterManager(DelegateClusterManager)
  }
  after {
    SparkContext.unregisterClusterManager(DelegateClusterManager)
  }

  test("launch of localbackend using Delegates") {
    SparkContext.registerClusterManager(DelegateClusterManager)
    val conf = new SparkConf().setMaster("external:snappy:local[*]").setAppName("testcm2")
    val sc = new SparkContext(conf)
    // check if the scheduler components are created
    assert(sc.schedulerBackend.isInstanceOf[LocalBackend])
    assert(sc.taskScheduler.isInstanceOf[TaskSchedulerImpl])
    // check if the intialization has happened.
    println(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].backend)
    assert(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].backend != null)
    sc.stop()
  }
}
