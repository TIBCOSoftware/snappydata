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
package io.snappydata

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * Basic hive meta-store client test in Snappy cluster.
  *
  * @author kneeraj
  */
class HiveMetastoreClientAccessDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {

  import HiveMetastoreClientAccessDUnitTest._

  override val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  def testHelloWorld(): Unit = {
    helloWorld()
  }

  def _testOne(): Unit = {
    val serverNetPort = AvailablePortHelper.getRandomAvailableTCPPort

    val locStr = "localhost[" + ClusterManagerTestBase.locatorPort + ']'
    vm2.invoke(this.getClass, "startDriverApp",
      Array(locStr.asInstanceOf[AnyRef]))

    startHiveMetaClientInGfxdPeerNode(locStr, serverNetPort)
    // Misc.getMemStore.initExternalCatalog
    // val cc = Misc.getMemStore.getExternalCatalog
    // assert(cc.isColumnTable("airline"))
    // assert(cc.isRowTable("row_table"))
  }

  def startHiveMetaClientInGfxdPeerNode(locatorStr: String, netPort: Int): Unit = {
    val dataStoreService = ServiceManager.getServerInstance
    val bootProperties = new Properties()
    bootProperties.setProperty("locators", locatorStr)
    dataStoreService.start(bootProperties)
    getLogWriter.info("Gfxd peer node vm type = " +
        GemFireStore.getBootedInstance.getMyVMKind)
  }
}

object HiveMetastoreClientAccessDUnitTest extends Logging {

  def helloWorld(): Unit = {
    hello("Hello World! " + this.getClass)
  }

  def hello(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
  }

  def startDriverApp(locatorStr: String): Unit = {
    startSnappyLocalModeAndCreateARowAndAColumnTable(locatorStr)
    val dsys = InternalDistributedSystem.getConnectedInstance
    assert(dsys != null)
    logInfo("Driver vm type = " + GemFireStore.getBootedInstance.getMyVMKind)
    logInfo("locator prop in driver app = " + InternalDistributedSystem
        .getConnectedInstance.getConfig.getLocators)
  }

  object ParseUtils extends java.io.Serializable {

    def parseInt(s: String, offset: Int, endOffset: Int): Int = {
      // Check for a sign.
      var num = 0
      var sign = -1
      val ch = s(offset)
      if (ch == '-') {
        sign = 1
      } else {
        num = '0' - ch
      }

      // Build the number.
      var i = offset + 1
      while (i < endOffset) {
        num *= 10
        num -= (s(i) - '0')
        i += 1
      }
      sign * num
    }

    def parseColumn(s: String, offset: Int, endOffset: Int,
        isInteger: Boolean): Any = {
      if (isInteger) {
        if (endOffset != (offset + 2) || s(offset) != 'N' || s(offset + 1) != 'A') {
          parseInt(s, offset, endOffset)
        } else {
          null
        }
      } else {
        s.substring(offset, endOffset)
      }
    }

    def parseRow(s: String, split: Char,
        columnTypes: Array[Boolean],
        row: ReusableRow): Unit = {
      var ai = 0
      var splitStart = 0
      val len = s.length
      var i = 0
      while (i < len) {
        if (s(i) == split) {
          row(ai) = parseColumn(s, splitStart, i, columnTypes(ai))
          ai += 1
          i += 1
          splitStart = i
        } else {
          i += 1
        }
      }
      // append remaining string
      row(ai) = parseColumn(s, splitStart, len, columnTypes(ai))
    }
  }

  case class TestData(c1: Int, c2: String)

  private def startSnappyLocalModeAndCreateARowAndAColumnTable(
      locStr: String): Unit = {
    def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
        arrDelaySlotIndex: Int): Row = {
      val arrDelay =
        if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
      row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
      row
    }

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val loadData: Boolean = true
    val setMaster: String = "local[6]"

    val conf = new org.apache.spark.SparkConf().setAppName("HiveMetastoreTest")
        .set("spark.logConf", "true")
        .set(Property.Locators.name, locStr)

    if (setMaster != null) {
      conf.setMaster(setMaster)
    }

    val sc = new org.apache.spark.SparkContext(conf)
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    if (loadData) {
      val airlineDataFrame: DataFrame =
        if (hfile.endsWith(".parquet")) {
          snContext.read.load(hfile)
        } else {
          val airlineData = sc.textFile(hfile)

          val schemaString = "Year,Month,DayOfMonth,DayOfWeek,DepTime,CRSDepTime," +
              "ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime," +
              "CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn," +
              "TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay," +
              "WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,ArrDelaySlot"
          val schemaArr = schemaString.split(",")
          val schemaTypes = List(IntegerType, IntegerType, IntegerType, IntegerType,
            IntegerType, IntegerType, IntegerType, IntegerType, StringType,
            IntegerType, StringType, IntegerType, IntegerType, IntegerType,
            IntegerType, IntegerType, StringType, StringType, IntegerType,
            IntegerType, IntegerType, IntegerType, StringType, IntegerType,
            IntegerType, IntegerType, IntegerType, IntegerType, IntegerType,
            IntegerType)

          val schema = StructType(schemaArr.zipWithIndex.map {
            case (fieldName, i) => StructField(
              fieldName, schemaTypes(i), i >= 4)
          })

          val columnTypes = schemaTypes.map {
            _ == IntegerType
          }.toArray
          val arrDelayIndex = schemaArr.indexOf("ArrDelay")
          val arrDelaySlotIndex = schemaArr.indexOf("ArrDelaySlot")
          val rowRDD = airlineData.mapPartitions { iter =>
            val row = new ReusableRow(schemaTypes)
            iter.map { s =>
              ParseUtils.parseRow(s, ',', columnTypes, row)
              addArrDelaySlot(row, arrDelayIndex, arrDelaySlotIndex)
            }
          }
          snContext.createDataFrame(rowRDD, schema)
        }

      airlineDataFrame.write.format("column").mode(SaveMode.Append)
          .options(Map.empty[String, String]).saveAsTable("airline")
      // airlineDataFrame.registerAndInsertIntoExternalStore("airline", bootProps)
    }

    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData(i, s"$i")))
    val dataDF = snContext.createDataFrame(rdd)

    snContext.createTable("row_table", "row", dataDF.schema,
      Map.empty[String, String])
  }
}
