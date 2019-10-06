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
package io.snappydata.core

import scala.reflect.io.Path
import scala.util.Random

import io.snappydata.Property

import org.apache.spark.SparkConf

/**
 * Test data and test context for Snappy store tests
 */
case class TestData(key1: Int, value: String)

case class TestData2(key1: Int, value: String, ref: Int)

case class Data(col1: Int, col2: Int, col3: Int)

case class Data1(pk: Int, sk: String)

case class Data2(pk: Int, Year: Int)

case class Data3(pk1: Int, sk: String , pk2 : Int)

case class Data4(sk: String , pk1: Int, pk2 : Int)

case class RefData(ref: Int, description: String)

case class TRIPDATA(
    MEDALLION: String,
    HACK_LICENSE: String,
    VENDOR_ID: String,
    RATE_CODE: Integer,
    STORE_AND_FWD_FLAG: String,
    PICKUP_DATETIME: String,
    DROPOFF_DATETIME: String,
    PASSENGER_COUNT: Integer,
    TRIP_TIME_IN_SECS: Integer,
    TRIP_DISTANCE: Double,
    PICKUP_LONGITUDE: Double,
    PICKUP_LATITUDE: Double,
    DROPOFF_LONGITUDE: Double,
    DROPOFF_LATITUDE: Double
    )

object FileCleaner {

  def deletePath(path: String): Boolean = {
    val file = Path(path)
    file.exists && file.deleteRecursively()
  }

  def cleanStoreFiles(): Unit = {
    deletePath("./metastore_db")
    deletePath("./warehouse")
    Path(".").walkFilter { f =>
        f.name.startsWith("BACKUPGFXD-DEFAULT-DISKSTORE") ||
        (f.name.startsWith("locator") && f.name.endsWith(".dat"))
    }.foreach(_.deleteRecursively())
    deletePath("./datadictionary")
  }
}

/** Default SparkConf used for local testing. */
object LocalSparkConf {

  private val random = new Random()

  def newConf(addOn: (SparkConf) => SparkConf = null): SparkConf = {
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    val conf = new SparkConf()
        .setIfMissing("spark.master", "local[4]")
        .setIfMissing("spark.memory.debugFill", "true")
        .set("snappydata.sql.planCaching", random.nextBoolean().toString)
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(Property.UseOptimizedHashAggregateForSingleKey.name, "true")
        .set(Property.TestCodeSplitThresholdInSHA.name, "5")
        .set(Property.TestCodeSplitFunctionParamsSizeInSHA.name, "2")
        .setAppName(getClass.getName)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }
}
