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
package io.snappydata.app

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.actors.Futures._


object SparkSQLMultiThreadingTest extends App {

  // Use DebugUtils from the SnappyUtils project later. Not sure how to get SBT
  // configured for this dependency
  def msg(s: String) = {
    println("Thread :: " + Thread.currentThread().getName + " :: " + s)
  }

  def parseInt(s: String, offset: Int, endOffset: Int): Integer = {
    // Check for a sign.
    var num = 0
    var sign = -1
    val ch = s(offset)
    if (ch == '-') {
      sign = 1
    }
    else {
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

  def parseColumn[U <: AnyRef](s: String, offset: Int, endOffset: Int,
      columnType: U): Any = {
    if (columnType eq IntegerType) {
      if (endOffset != (offset + 2) || s(offset) != 'N' || s(offset + 1) != 'A') {
        parseInt(s, offset, endOffset)
      }
      else {
        null
      }
    }
    else {
      s.substring(offset, endOffset)
    }
  }

  def parseRow[U <: AnyRef](s: String, split: Char,
      schema: List[U]): Row = {
    val a = new Array[Any](schema.length)
    var ai = 0
    var splitStart = 0
    val len = s.length
    var i = 0
    while (i < len) {
      if (s(i) == split) {
        a(ai) = parseColumn(s, splitStart, i, schema(ai))
        ai += 1
        i += 1
        splitStart = i
      }
      else {
        i += 1
      }
    }
    // append remaining string
    a(ai) = parseColumn(s, splitStart, len, schema(ai))
    Row.fromSeq(a)
  }

  var hfile: String = "/Users/jramnara/Downloads/2007-8.csv"
  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"

  if (args.length > 0) {
    option(args.toList)
  }

  val conf = new org.apache.spark.SparkConf().setAppName("SparkSQLTest")
    .set("spark.logConf", "true")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.shuffle.spill", "false")
    //.set("spark.shuffle.sort.bypassMergeThreshold", "-1")
    //.set("spark.sql.codegen", "true")
    //.set("spark.eventLog.enabled", "true")
    //.set("spark.eventLog.dir","file:/tmp/spark-events")
    //.set("spark.metrics.conf", "/home/sumedh/Projects/data/metrics.properties")
  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    // Intellij compile output path e.g. /wrk/out-snappy/production/MyTests/
    //conf.setExecutorEnv(Seq(("extraClassPath", executorExtraClassPath)))
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (debug) {
    val agentOpts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    if (executorExtraJavaOptions != null) {
      executorExtraJavaOptions = executorExtraJavaOptions + " " + agentOpts
    } else {
      executorExtraJavaOptions = agentOpts
    }
  }

  if (executorExtraJavaOptions != null) {
    conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
  }

  msg("About to create the spark context")
  val sc = new org.apache.spark.SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  sqlContext.sql("set spark.sql.shuffle.partitions=6")

  if (loadData) {
    val airlinedata = sc.textFile(hfile)

    val schemaString = "Year,Month,DayOfMonth,DayOfWeek,DepTime,CRSDepTime," +
      "ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime," +
      "CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn," +
      "TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay," +
      "WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay"
    val schemaTypes = List(IntegerType, IntegerType, IntegerType, IntegerType,
      IntegerType, IntegerType, IntegerType, IntegerType, StringType,
      IntegerType, StringType, IntegerType, IntegerType, IntegerType,
      IntegerType, IntegerType, StringType, StringType, IntegerType,
      IntegerType, IntegerType, IntegerType, StringType, IntegerType,
      IntegerType, IntegerType, IntegerType, IntegerType, IntegerType)

    /*
    CREATE TABLE airline (
        YearI INT NOT NULL,
        MonthI INT NOT NULL,
        DayOfMonth INT NOT NULL,
        DayOfWeek INT NOT NULL,
        DepTime INT,
        CRSDepTime INT,
        ArrTime INT,
        CRSArrTime INT,
        UniqueCarrier VARCHAR(20) NOT NULL,
        FlightNum INT,
        TailNum VARCHAR(20),
        ActualElapsedTime INT,
        CRSElapsedTime INT,
        AirTime INT,
        ArrDelay INT,
        DepDelay INT,
        Origin VARCHAR(20),
        Dest VARCHAR(20),
        Distance INT,
        TaxiIn INT,
        TaxiOut INT,
        Cancelled INT,
        CancellationCode VARCHAR(20),
        Diverted INT,
        CarrierDelay INT,
        WeatherDelay INT,
        NASDelay INT,
        SecurityDelay INT,
        LateAircraftDelay INT
    )
    */

    val schema = StructType(schemaString.split(",").zipWithIndex.map {
      case (fieldName, i) => StructField(
        fieldName, schemaTypes(i), i >= 4)
    })

    val rowRDD = airlinedata.map(parseRow(_, ',', schemaTypes))
    //val airlineSchemaRDD: DataFrame = sqlContext.createDataFrame(rowRDD,
    //  schema).stratifiedSample("UniqueCarrier,Year,Month", 0.001).coalesce(6)
    val airlineSchemaRDD: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
    airlineSchemaRDD.registerTempTable("airline")
    sqlContext.cacheTable("airline")
  }
  
  // run several queries concurrently in threads ...
  val tasks = for (i <- 1 to 10) yield future {
	  msg("Executing task " + i)
	  Thread.sleep(1000L)
	  runQueries(sqlContext)
  }

  val ret = awaitAll(20000000L, tasks: _*) // wait a lot

  def runQueries(sqlContext: SQLContext ): Unit = {
	  var start: Long = 0
	  var end: Long = 0
	  var results: DataFrame = null

    val sparkSession = sqlContext.sparkSession
    import sparkSession.implicits._
	
	  for (i <- 0 to 3) {
	
	    start = System.currentTimeMillis
	    results = sqlContext.sql("SELECT count(*) FROM airline")
	    results.map(t => "Count: " + t(0)).collect().foreach(msg)
	    end = System.currentTimeMillis
	    msg("Time taken for count(*): " + (end - start) + "ms")
	
	    start = System.currentTimeMillis
	    results = sqlContext.sql("SELECT count(DepTime) FROM airline")
	    results.map(t => "Count: " + t(0)).collect().foreach(msg)
	    end = System.currentTimeMillis
	    msg("Time taken for count(DepTime): " + (end - start) + "ms")
	
	    start = System.currentTimeMillis
	    results = sqlContext.sql("""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
	        Year, Month FROM airline GROUP BY UniqueCarrier, Year, Month""")
	    results.collect()   //.foreach(msg)
	    end = System.currentTimeMillis
	    msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")
	
	    Thread.sleep(3000)
	
	    start = System.currentTimeMillis
	    results = sqlContext.sql("""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM airline GROUP BY UniqueCarrier, Year, Month ORDER BY
	        UniqueCarrier, Year, Month""")
	    //results.explain(true)
	    results.collect()  //.foreach(msg)
	    end = System.currentTimeMillis
	    msg("Time taken for AVG on GROUP BY with ORDER BY: " +
	      (end - start) + "ms")
	
	    Thread.sleep(3000)
	
	    start = System.currentTimeMillis
	    results = sqlContext.sql("""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM airline GROUP BY UniqueCarrier, Year, Month
	        ORDER BY Year, Month""")
	    results.collect()
	    /**
      var worst1 = 0.0
	    var worst2 = 0.0
	    var worst1Carrier: String = null
	    var worst2Carrier: String = null
	    var currYear = 0
	    var currMon = 0
	    results.collect().foreach(t => {
	      val avgDelay = t.getDouble(0)
	      val year = t.getInt(2)
	      val mon = t.getInt(3)
	      if (mon != currMon || year != currYear) {
	        if (currYear != 0) {
	          msg(currYear + "-" + currMon + " Carrier1: " + worst1Carrier +
	            "=" + worst1 + ", Carrier2: " + worst2Carrier + "=" + worst2)
	        }
	        currYear = year
	        currMon = mon
	        worst1 = 0.0
	        worst2 = 0.0
	        worst1Carrier = null
	        worst2Carrier = null
	      } else if (avgDelay > worst1) {
	        worst1 = avgDelay
	        worst1Carrier = t.getString(1)
	      } else if (avgDelay > worst2) {
	        worst2 = avgDelay
	        worst2Carrier = t.getString(1)
	      }
	    })
	    if (currYear != 0) {
	      msg(currYear + "-" + currMon + " Carrier1: " + worst1Carrier +
	        "=" + worst1 + ", Carrier2: " + worst2Carrier + "=" + worst2)
	    }
	    * 
	    */
	    end = System.currentTimeMillis
	    msg("Time taken for worst carrier processing: " + (end - start) + "ms")
	
	    Thread.sleep(3000)
	  }
  }

  def option(list: List[String]): Boolean = {
    list match {
      case "-hfile" :: value :: tail =>
        hfile = value
        print(" hfile " + hfile)
        option(tail)
      case "-noload" :: tail =>
        loadData = false
        print(" loadData " + loadData)
        option(tail)
      case "-set-master" :: value :: tail =>
        setMaster = value
        print(" setMaster " + setMaster)
        option(tail)
      case "-nomaster" :: tail =>
        setMaster = null
        print(" setMaster " + setMaster)
        option(tail)
      case "-set-jars" :: value :: tail =>
        setJars = value
        print(" setJars " + setJars)
        option(tail)
      case "-executor-extraClassPath" :: value :: tail =>
        executorExtraClassPath = value
        print(" executor-extraClassPath " + executorExtraClassPath)
        option(tail)
      case "-executor-extraJavaOptions" :: value :: tail =>
        executorExtraJavaOptions = value
        print(" executor-extraJavaOptions " + executorExtraJavaOptions)
        option(tail)
      case "-debug" :: tail =>
        debug = true
        print(" debug " + debug)
        option(tail)
      case opt :: tail =>
        println(" Unknown option " + opt)
        sys.exit(1)
      case Nil => true
    }
  } // end of option
}

// end of class
