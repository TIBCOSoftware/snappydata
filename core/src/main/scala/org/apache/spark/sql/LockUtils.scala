/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock

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
