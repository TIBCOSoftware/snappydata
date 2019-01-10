/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.streaming

import io.snappydata.SnappyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}


class SnappyStreamingContextSuite extends SnappyFunSuite with Eventually
    with BeforeAndAfter with BeforeAndAfterAll {

  def framework: String = this.getClass.getSimpleName

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"
  val conf = new SparkConf().setMaster(master).setAppName(appName)

  var snsc: SnappyStreamingContext = null


  val contextSuite: StreamingContextSuite = new StreamingContextSuite
  // context creation is handled by App main
  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAll()
  }

  before {
  }

  after {
    val activeSsc = SnappyStreamingContext.getActive
    activeSsc match {
      case Some(x) => x.stop(stopSparkContext = true, stopGracefully = true)
      case None => //
    }

  }


  test("test simple constructor") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    snsc = new SnappyStreamingContext(sc, batchDuration = batchDuration)
    assert(SnappyStreamingContext.getInstance() != null)

    val input = contextSuite.addInputStream(snsc)
    input.foreachRDD { rdd => rdd.count }
    snsc.start()

    assert(SnappyStreamingContext.getActive != null)
  }

  test("test getOrCreate") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): SnappyStreamingContext = {
      newContextCreated = true
      new SnappyStreamingContext(conf, batchDuration)
    }
    // Call ssc.stop after a body of code
    def testGetOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {
        if (snsc != null) {
          snsc.stop()
        }
        snsc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()

    // getOrCreate should create new context with empty path
    testGetOrCreate {
      snsc = SnappyStreamingContext.getOrCreate(emptyPath, creatingFunction _)
      assert(snsc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val corruptedCheckpointPath = contextSuite.createCorruptedCheckpoint()

    // getOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      snsc = SnappyStreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      snsc = SnappyStreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getOrCreate should create new context with fake checkpoint file and createOnError = true
    testGetOrCreate {
      snsc = SnappyStreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(snsc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val checkpointPath = contextSuite.createValidCheckpoint()

    // getOrCreate should recover context with checkpoint path, and recover old configuration
    testGetOrCreate {
      snsc = SnappyStreamingContext.getOrCreate(checkpointPath, creatingFunction _)
      assert(snsc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(snsc.conf.get("someKey") === "someValue", "checkpointed config not recovered")
    }
  }


  test("getActiveOrCreate with checkpoint") {
    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): SnappyStreamingContext = {
      newContextCreated = true
      new SnappyStreamingContext(sc, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetActiveOrCreate(body: => Unit): Unit = {
      require(SnappyStreamingContext.getActive.isEmpty) // no active context
      newContextCreated = false
      try {
        body
      } finally {
        if (snsc != null) {
          snsc.stop()
        }
        snsc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()



    val corruptedCheckpointPath = contextSuite.createCorruptedCheckpoint()
    val checkpointPath = contextSuite.createValidCheckpoint()

    // getActiveOrCreate should return the current active context if there is one
    testGetActiveOrCreate {
      snsc = new SnappyStreamingContext(
        conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"), batchDuration)
      contextSuite.addInputStream(snsc).register()
      snsc.start()
      val returnedSsc = SnappyStreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(!newContextCreated, "new context created instead of returning")
      assert(returnedSsc.eq(snsc), "returned context is not the activated context")
    }

    // getActiveOrCreate should create new context with empty path
    testGetActiveOrCreate {
      snsc = SnappyStreamingContext.getActiveOrCreate(emptyPath, creatingFunction _)
      assert(snsc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      snsc = SnappyStreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getActiveOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      snsc = SnappyStreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getActiveOrCreate should create new context with fake
    // checkpoint file and createOnError = true
    testGetActiveOrCreate {
      snsc = SnappyStreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(snsc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should recover context with checkpoint path, and recover old configuration
    testGetActiveOrCreate {
      snsc = SnappyStreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(snsc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(snsc.conf.get("someKey") === "someValue")
    }
  }
}
