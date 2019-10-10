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

package io.snappydata.gemxd.remotestore

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

/**
  * This class expects that only one thread will take out
  * result elements. Publisher can be more than one thread.
  *
  */

class BatchResultStream[T: ClassTag](maxResultElem: Int) {

  var totalAdded: Int = 0
  var totalTaken: Int = 0
  var resultHolder: LinkedBlockingQueue[T] = new LinkedBlockingQueue[T](maxResultElem)

  def addResult(elem: T): Unit = {
    this.synchronized {
      if ( totalAdded == maxResultElem) {
        throw new RuntimeException("Cannot add more elements")
      }
      resultHolder.add(elem)
    }
  }

  def getOneElement : T = {
    if (mayReturnResult) {
      throw new NoSuchElementException("Already returned all elements")
    }
    val elem = resultHolder.take()
    totalTaken = totalTaken + 1
    elem
  }

  def mayReturnResult: Boolean = {
    if (totalTaken == maxResultElem) return false
    true
  }
}
