package org.apache.spark.shuffle

import scala.reflect.runtime.{universe => ru}

/**
 * Created by shirishd on 15/10/15.
 */

private[spark] class SnappyShuffleMemoryManager protected(override val maxMemory: Long,
    override val pageSizeBytes: Long) extends ShuffleMemoryManager(maxMemory, pageSizeBytes) {

  override def tryToAcquire(numBytes: Long): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      notifyAll() // Will later cause waiting tasks to wake up and check numThreads again
    }

    if (isCriticalUp) {
      0
    } else {
      super.tryToAcquire(numBytes)
    }
  }

  def isCriticalUp(): Boolean = {
    val companionObject = "org.apache.spark.storage.SnappyMemoryStore"
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val moduleSymbol = mirror.staticModule(companionObject)
    val moduleMirror = mirror.reflectModule(moduleSymbol)
    val instanceMirror = mirror.reflect(moduleMirror.instance)
    val method = instanceMirror.reflectMethod(instanceMirror.symbol.toType.declaration(ru.newTermName("isCriticalUp")).asMethod)
    val ret = method()
    ret.asInstanceOf[Boolean]

//    val className = Class.forName("org.apache.spark.storage.SnappyMemoryStore$") //note the $
//    className.getField("MODULE$").get(null).asInstanceOf[Boolean]
  }


}

