/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.snappydata.hydra

import java.util.Random
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class BenchmarkingReceiver (val maxRecPerSecond: Int,
    val numWarehouses: Int,
    val numDistrictsPerWarehouse: Int,
    val numCustomersPerDistrict: Int,
    val itemCount : Int)
  extends Receiver[ClickStreamCustomer](StorageLevel.MEMORY_AND_DISK) {


  var receiverThread: Thread = null
  var stopThread = false;
  override def onStart() {
    receiverThread = new Thread("BenchmarkingReceiver") {
      override def run() {
        receive()
      }
    }
    receiverThread.start()
  }

  override def onStop(): Unit = {
    receiverThread.interrupt()
  }

  private def receive() {
    while (!isStopped()) {
      val start = System.currentTimeMillis()
      var i = 0;
      for (i <- 1 to maxRecPerSecond) {
        store(generateClickStream())
        if (isStopped()) {
          return
        }
      }
      // If one second hasn't elapsed wait for the remaining time
      // before queueing more.
      val remainingtime = 1000 - (System.currentTimeMillis() - start)
      if (remainingtime > 0) {
        Thread.sleep(remainingtime)
      }
    }
  }

  val rand = new Random(123)

  private def generateClickStream(): ClickStreamCustomer = {

    val warehouseID: Int = rand.nextInt(numWarehouses)
    val districtID: Int = rand.nextInt(this.numDistrictsPerWarehouse)
    val customerID: Int = rand.nextInt(this.numCustomersPerDistrict)
    val itemId: Int = rand.nextInt(this.itemCount)
    // timespent on website is 100 -500 seconds
    val timespent: Int = rand.nextInt(400) + 100

    new ClickStreamCustomer(warehouseID, districtID, customerID, itemId, timespent)
  }
}

class ClickStreamCustomer (val w_id: Int,
                          val d_id: Int,
                          val c_id: Int,
                          val i_id: Int,
                          val c_ts: Int) extends Serializable
