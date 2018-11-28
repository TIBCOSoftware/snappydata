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
package io.snappydata.hydra

import java.io.File

import io.snappydata.SnappyTestRunner
import scala.sys.process._

import hydra.HostHelper

/**
 * Class extending can mix match methods like searchExceptions
 */
class SnappyHydraTestRunner extends SnappyTestRunner {

  var SNAPPYDATA_SOURCE_DIR = ""

  override def beforeAll(): Unit = {
    snappyHome = System.getenv("SNAPPY_HOME")
    SNAPPYDATA_SOURCE_DIR = s"$snappyHome/../../.."
    if (snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }
    currWorkingDir = System.getProperty("user.dir")
  }

  def searchExceptions(logDir: File): Unit = {
    val c1 = s"grep -r Exception $logDir"
    val c2 = "grep -v  java.net.BindException"
    val c3 = "egrep -v (NoSuchObjectException|NucleusObjectNotFoundException|StateManager)"
    val c4 = "grep -v RegionDestroyedException"
    val c5 = "grep -v statArchive.gfs"
    val c6 = "grep -v DistributedSystemDisconnectedException"
    val c7 = "grep -v newDisconnectedException"
    val c8 = "grep -v CacheClosedException"
    val c12 = "grep -v java.io.FileNotFoundException"
    val c13 = "egrep -v (org.apache.spark.shuffle.FetchFailedException|" +
        "org.glassfish.jersey.server.internal.MappableExceptionWrapperInterceptor)"
    val c14 = "grep -v java.lang.reflect.InvocationTargetException"
    val c15 = "grep -v org.apache.spark.storage.ShuffleBlockFetcherIterator." +
        "throwFetchFailedException"
    val c16 = Seq("grep", "-v", "org.apache.spark.SparkException: Exception thrown in awaitResult")
    /* val c17 = "grep \'status:[[:space:]]*stopping\'[[:space:]]*-e[[:space:]]*\'java.lang" +
        ".IllegalStateException\'" */
    val c18 = "grep -v com.gemstone.gemfire.distributed.LockServiceDestroyedException"
    /*
    val c19 = "grep GemFireIOException:[[:space:]]*Current[[:space:]]*operations[[:space:]]*did" +
        "[[:space:]]*not[[:space:]]*distribute[[:space:]]*within"
    val c20 = "grep SparkException:[[:space:]]*External[[:space:]]*scheduler[[:space:]]*cannot" +
        "[[:space:]]*be[[:space:]]*instantiated" */
    val c21 = Seq("grep", "-v", "Failed to retrieve information for")
    val c22 = "grep -v abrt-watch-log"
    val hostName = HostHelper.getLocalHost()
    val c23 = s"grep -v hoststats_${hostName}.txt"
    val command1 = c1 #| c2 #| c3 #| c4 #| c5 #| c6 #| c7 #| c8 #| c12 #| c13 #| c14 #| c15 #|
        c16 #| /* c17 #| */ c18 #| /* c19 #| c20 #| */ c21 #| c22 #| c23
    // TODO : handle case where the logDir path is incorrect or doesn't exists
    try {
      val output1: String = command1.!!
      throw new Exception(s"smokeBT Failed with below Exceptions:\n" + output1 +
          "\nCheck the logs at " + logDir + " for more details on Exception.... ")
    }
    catch {
      case r: java.lang.RuntimeException =>
        if (r.getMessage.contains("Nonzero exit value: 1")) {
          // scalastyle:off println
          println("No unexpected Exceptions observed during smoke bt run.")
        }
        else {
          throw r
        }
      case i: Throwable => throw i
    }
  }
}
