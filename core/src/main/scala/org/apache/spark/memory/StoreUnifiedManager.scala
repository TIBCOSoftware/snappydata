package org.apache.spark.memory

import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.store.GemFireStore

import org.apache.spark.storage.BlockId
import org.apache.spark.{Logging, SparkEnv}

/**
  * Created by rishim on 13/2/17.
  */
trait StoreUnifiedManager {

  def acquireStorageMemoryForObject(
      objectName : String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean


  def dropStorageMemoryForObject(objectName : String, memoryMode: MemoryMode): Long

  def releaseStorageMemoryForObject(objectName : String, numBytes: Long, memoryMode: MemoryMode): Unit
}

/**
  * This class will store all the memory usage for GemFireXD boot up time when SparkEnv is not initialised.
  * This class will not actually allocate any memory. It is just a temp account holder till SnappyUnifiedManager is started.
  */
class TempMemoryManager extends StoreUnifiedManager {

  val memoryForObject = new mutable.HashMap[String, Long]()

  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    println(s"Acquiing memory for $objectName")
    if (!memoryForObject.contains(objectName)) {
      memoryForObject(objectName) = 0L
    }
    memoryForObject(objectName) += numBytes
    true
  }

  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode): Long = synchronized {
    memoryForObject.remove(objectName).getOrElse(0L)
  }

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryForObject(objectName) -= numBytes
  }
}


class NoOpSnappyMemoryManager extends StoreUnifiedManager {


  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = true


  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode): Long = 0L

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = {}
}

object MemoryManagerCallback extends Logging {

  val tempMemoryManager = new TempMemoryManager
  private var snappyUnifiedManager: StoreUnifiedManager = null
  private val noOpMemoryManager :  StoreUnifiedManager = new NoOpSnappyMemoryManager

  def resetMemoryManager(): Unit ={
    tempMemoryManager.memoryForObject.clear()
    snappyUnifiedManager = null
  }

  private final val isCluster = {
    try {
      val c = org.apache.spark.util.Utils.classForName(
        "org.apache.spark.memory.SnappyUnifiedMemoryManager")
      // Class is loaded means we are running in SnappyCluster mode.

      true
    } catch {
      case cnf: ClassNotFoundException =>
        logWarning("MemoryManagerCallback couldn't be INITIALIZED." +
            "SnappyUnifiedMemoryManager won't be used.")
        false
    }
  }

  def memoryManager: StoreUnifiedManager = {
    //First check if SnappyUnifiedManager is set. If yes no need to look further.
    if (isCluster && (snappyUnifiedManager ne null)) {
      return snappyUnifiedManager
    }
    if (!isCluster) {
      return noOpMemoryManager
    }

    if (GemFireStore.getBootedInstance ne null) {
      if (SparkEnv.get ne null) {
        if (SparkEnv.get.memoryManager.isInstanceOf[StoreUnifiedManager]) {
          snappyUnifiedManager = SparkEnv.get.memoryManager.asInstanceOf[StoreUnifiedManager]
          return snappyUnifiedManager
        } else {
          // For testing purpose if we want to disable SnappyUnifiedManager
          return noOpMemoryManager
        }
      } else {
        return tempMemoryManager
      }

    } else {
      // Till GemXD Boot Time
      return tempMemoryManager
    }
  }

}


