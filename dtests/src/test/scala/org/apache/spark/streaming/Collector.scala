package org.apache.spark.streaming

import com.typesafe.config.Config
// import org.apache.spark._
//import org.apache.spark.streaming.{SnappyStreamingContext, Seconds, Duration}
// import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql._
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Collector extends SnappyStreamingJob {

  // For measurement
  private val logger  = Logger getLogger this.getClass
  private val sw      = new StopWatch("", logger)

  //===========================
  // STREAMING FUNCTION
  //===========================
  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    
    sw.start("Initializing")

    val ss = snsc.snappySession
    //this._hideNoisyLog()
    this._initializeTable(ss,1)
    this._initializeTable(ss,2)
    this._initializeTable(ss,3)

    this._hideNoisyLog()

    sw.stop

    val psr = new CSVParser(ss.sparkContext)

    /** Read CSV as DStream */
    val csv_SRC1    = snsc.textFileStream(Consts.SOURCE1_DIR)
    val csv_SRC2    = snsc.textFileStream(Consts.SOURCE2_DIR)
    val csv_SRC3    = snsc.textFileStream(Consts.SOURCE3_DIR)


    val batchedInput_SRC1 = csv_SRC1.window(
                 Seconds(Consts.DATAFRAME_WIDTH)
                ,Seconds(Consts.DATAFRAME_WIDTH)
              )
    val batchedInput_SRC2 = csv_SRC2.window(
                 Seconds(Consts.DATAFRAME_WIDTH)
                ,Seconds(Consts.DATAFRAME_WIDTH)
              )
//    val batchedInput_SRC3 = csv_SRC3.window(
//                 Seconds(Consts.DATAFRAME_WIDTH)
//                ,Seconds(Consts.DATAFRAME_WIDTH)
//              )

    /** Parse DStream to Intervaled SchemaDStream */
    val parsedInput_SRC1 = psr.parseCSV_SRC1(batchedInput_SRC1, snsc) //to SchemaDStream
    val parsedInput_SRC2 = psr.parseCSV_SRC2(batchedInput_SRC2, snsc) //to SchemaDStream
    //val parsedInput_SRC3 = psr.parseCSV_SRC3(batchedInput_SRC3, snsc) //to SchemaDStream

    /** Register SchemaDStream as Temp Table */
    parsedInput_SRC1.registerAsTable("temp_SRC1")
    parsedInput_SRC2.registerAsTable("temp_SRC2")
    //parsedInput_SRC3.registerAsTable("temp_SRC3")

    /** Register Continuous Query to Temp Table */
    val aggregated_SRC1 = snsc.registerCQ(s"SELECT ${Col.ID}, ${Col.ACC_TS}"
        + s", avg(${Col.ACC_DATA1}) AS ${Col.ACC_DATA1}"
        + s", avg(${Col.ACC_DATA2}) AS ${Col.ACC_DATA2}"
        + s", avg(${Col.ACC_DATA3}) AS ${Col.ACC_DATA3}" 
        + s" FROM temp_SRC1 GROUP BY ${Col.ID}, ${Col.ACC_TS}"
    )


    /** When Input format is vertical, we need pivot*/
    val aggregated_SRC2 = snsc.registerCQ(s"SELECT ${Col.ID}, ${Col.BIO_TS}"
        + s", avg(CASE WHEN ${Col.SENSOR_CODE}=60 THEN ${Col.SENSOR_VALUE} END) AS ${Col.BIO_DATA1}"
        + s", avg(CASE WHEN ${Col.SENSOR_CODE}=61 THEN ${Col.SENSOR_VALUE} END) AS ${Col.BIO_DATA2}" 
        + s" FROM temp_SRC2 GROUP BY ${Col.ID}, ${Col.BIO_TS}"
    )
    
//    val aggregated_SRC3 = snsc.registerCQ(s"SELECT ${Col.ID}, ${Col.ENV_TS}"
//        + s", avg(CASE WHEN ${Col.SENSOR_CODE}=1 THEN ${Col.SENSOR_VALUE} END) AS ${Col.ENV_DATA1}"
//        + s", avg(CASE WHEN ${Col.SENSOR_CODE}=2 THEN ${Col.SENSOR_VALUE} END) AS ${Col.ENV_DATA2}"
//        + s", avg(CASE WHEN ${Col.SENSOR_CODE}=3 THEN ${Col.SENSOR_VALUE} END) AS ${Col.ENV_DATA3}"
//        + s" FROM temp_SRC3 GROUP BY ${Col.ID}, ${Col.ENV_TS}")

    aggregated_SRC1.foreachDataFrame(_.show())
    aggregated_SRC2.foreachDataFrame(_.show())
    // aggregated_SRC3.foreachDataFrame(_.show())


    // When Input format is horizon, we do not need pivot
    // val aggregated_SRC2 = snsc.registerCQ(s"SELECT USER_ID, TIMESTAMP, avg(HEART_RATE) AS HEART_RATE, avg(BREATH_RATE) as BREATH_RATE FROM temp_SRC2 GROUP BY USER_ID, TIMESTAMP")

    // val aggregated_SRC3 = snsc.registerCQ(s"SELECT USER_ID, TIMESTAMP, avg(TEMPERATURE) AS TEMPERATURE, avg(HUMIDITY) as HUMIDITY, avg(ILLUMINANCE) as ILLUMINANCE FROM temp_SRC3 GROUP BY USER_ID, TIMESTAMP")


    /** Put Aggregated SchemaDStrem into ROW Table */
    aggregated_SRC1.foreachDataFrame { df =>
      val isEmpty = df.limit(1).rdd.isEmpty
      if (!isEmpty) df.write.putInto(Consts.SRC1_TABLE_NAME)
    }

    aggregated_SRC2.foreachDataFrame { df =>
    val isEmpty = df.limit(1).rdd.isEmpty
    if (!isEmpty) df.write.putInto(Consts.SRC2_TABLE_NAME)
    }
   
//    aggregated_SRC3.foreachDataFrame { df =>
//
//      val isEmpty = df.limit(1).rdd.isEmpty
//
//      if (!isEmpty) {
//
//        df.write.putInto(Consts.SRC3_TABLE_NAME)
//
//      }
//
//    }

 

    //----------------------------------------------------
    // Test to store SchemaDStream as STREAM TABLE
    // 2017.01.18 added by ide

    /*aggregated_SRC1.registerAsTable(Consts.SRC1_STREAMTABLE_NAME)
    aggregated_SRC2.registerAsTable(Consts.SRC2_STREAMTABLE_NAME)
    aggregated_SRC3.registerAsTable(Consts.SRC3_STREAMTABLE_NAME)

    val sds1FromTable = snsc.registerCQ(
      s"select * from ${Consts.SRC1_STREAMTABLE_NAME}"
    )
    sds1FromTable.foreachDataFrame { df =>
      df.show
    }
    val sds2FromTable = snsc.registerCQ(
      s"select * from ${Consts.SRC2_STREAMTABLE_NAME}"
    )
    sds2FromTable.foreachDataFrame { df =>
      df.show
    }
    val sds3FromTable = snsc.registerCQ(
      s"select * from ${Consts.SRC3_STREAMTABLE_NAME}"
    )
    sds3FromTable.foreachDataFrame { df =>
      df.show
    }*/

//    def analyzer = new Analyzer()
//    analyzer.analyze(snsc)
    //----------------------------------------------------

    parsedInput_SRC1.foreachDataFrame(_.show())
    parsedInput_SRC2.foreachDataFrame(_.show())

    aggregated_SRC1.foreachDataFrame(_.show())
    aggregated_SRC2.foreachDataFrame(_.show())

    snsc.start()
    try {
      if (Consts.RUN_PERSISTENTLY) {
        snsc.awaitTermination()
      }
      else {
        val overhead     = 2  //sec
        Thread.sleep( (Consts.MEASURE_TIME - overhead) * 1000)
      }
    }
    finally {
      ss.clearPlanCache()
      snsc.stop(stopSparkContext=false, stopGracefully=true)
      logger.info("/stop" +
        "/")
    }

  }
  

  //===========================
  // ERROR HANDLING FUNCTION
  //===========================
  override def isValidJob(snsc: SnappyStreamingContext, config: Config)
                          : SnappyJobValidation = {
    SnappyJobValid()
  }


  //===========================
  // My Functions
  //===========================

  /** Re-create tables and indexes of APP.RAW and APP.RESULT */
  private def _initializeTable(ss: SnappySession, src_num: Int) = {
    //Initializing raw table
//    ss.dropIndex(Consts.SRC1_INDEX_NAME, ifExists=true)
    ss.dropTable(Consts.SRC_TABLE_NAME(src_num-1), ifExists=true)
    ss.createTable(Consts.SRC_TABLE_NAME(src_num-1),   Consts.TABLE_TYPE
                 , Consts.SRC_TABLE_SCHEMA(src_num-1), Consts.TABLE_PROPS
                 , allowExisting=true)

/*    ss.sql("create table "
    + Consts.SRC_TABLE_NAME(src_num-1)
    + " "
    + Consts.SRC_TABLE_SCHEMA(src_num-1)
    + " "
    + " using "
    + Consts.TABLE_TYPE
    + " options"
    + "(buckets '113')") */

//    ss.createIndex(Consts.SRC1_INDEX_NAME, Consts.SRC1_TABLE_NAME
//                 , Consts.SRC1_INDEX_COLS, Map.empty[String, String])

  }


  /** Ignore noisy logs in spark/snappystore */
  private def _hideNoisyLog() = {
    
    // Ignore
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("snappystore").setLevel(Level.ERROR)
    Logger.getLogger("Datastore").setLevel(Level.ERROR)

    // Restore noisy logs in spark/snappystore
    //Logger.getLogger("org").setLevel(Level.INFO)
    //Logger.getLogger("akka").setLevel(Level.INFO)
    //Logger.getLogger("snappystore").setLevel(Level.INFO)
    //Logger.getLogger("Datastore").setLevel(Level.INFO)

  }

}
