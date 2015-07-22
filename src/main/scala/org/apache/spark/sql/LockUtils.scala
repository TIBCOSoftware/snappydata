package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by hemantb on 7/14/15.
 */
object LockUtils {

  class ReadWriteLock {
    val rwLock = new ReentrantReadWriteLock()
    val readLock = rwLock.readLock
    val writeLock = rwLock.writeLock

    def executeInReadLock[T](body: => T, lockInterruptibly: Boolean = false): T = {
      if (lockInterruptibly) {
        this.readLock.lockInterruptibly()
      } else {
        this.readLock.lock()
      }
      try {
        body
      } finally {
        this.readLock.unlock()
      }
    }

    def executeInWriteLock[T](body: => T, lockInterruptibly: Boolean = false): T = {
      if (lockInterruptibly) {
        this.writeLock.lockInterruptibly()
      } else {
        this.writeLock.lock()
      }
      try {
        body
      } finally {
        this.writeLock.unlock()
      }
    }
  }

}
