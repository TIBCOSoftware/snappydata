package io.snappydata.externalstore

import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.store.StoreUtils

class ExecutorMessageDUnitTest(s: String) extends ClusterManagerTestBase(s) with Logging {

  def testStoreBlockMapUpdates(): Unit = {

    val snc = SnappyContext(sc)

    // Do something until the executors are initialized
    val tableName = "ColumnTable"
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, Map.empty[String, String])
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    logInfo(s"ABS size in test ${StoreUtils.storeToBlockMap.size}")
    for ((dm, blockId) <- StoreUtils.storeToBlockMap) {
      logInfo(s"ABS dm $dm, bmi $blockId")
    }
    assert(StoreUtils.storeToBlockMap.size == 3)
    for ((dm, blockId) <- StoreUtils.storeToBlockMap) {
      assert(blockId != null)
    }

    snc.dropTable(tableName, ifExists = true)

    // Now verify that the map gets updated when nodes go down.
    vm0.invoke(getClass, "stopMe")
    //Thread.sleep(2000)
    assert(StoreUtils.storeToBlockMap.size == 2)
    for ((dm, blockId) <- StoreUtils.storeToBlockMap) {
      assert(blockId != null)
    }

    vm1.invoke(getClass, "stopMe")
    //Thread.sleep(2000)
    assert(StoreUtils.storeToBlockMap.size == 1)
    for ((dm, blockId) <- StoreUtils.storeToBlockMap) {
      assert(blockId != null)
    }

    vm2.invoke(getClass, "stopMe")
    //Thread.sleep(2000)
    assert(StoreUtils.storeToBlockMap.size == 0)

    getLogWriter.info("Successful")
  }

//  def stopMe(): Unit = {
//    ClusterManagerTestBase.stopAny()
//  }
}

object ExecutorMessageDUnitTest {

  def stopMe(): Unit = {
    ClusterManagerTestBase.stopAny()
  }
}