package io.snappydata.benchmark.snappy

import com.typesafe.config.Config
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.sql.SnappySQLJob

/**
  * Created by kishor on 28/1/16.
  */
object TPCH_Snappy_Query extends SnappySQLJob{

   var sqlSparkProperties: Array[String] = _
   var queryPlan : Boolean = false
   var queries:Array[String] = _
    var useIndex: Boolean = _
  //  var avgFileStream: FileOutputStream = _
//  var avgPrintStream:PrintStream = _

   override def runJob(snc: C, jobConfig: Config): Any = {
     val isResultCollection = false
     val isSnappy = true

//     avgFileStream = new FileOutputStream(new File(s"Average.out"))
//     avgPrintStream = new PrintStream(avgFileStream)

     val usingOptionString = s"""
           USING row
           OPTIONS ()"""


     for(prop <- sqlSparkProperties) {
       snc.sql(s"set $prop")
     }

     if (queryPlan) {
       TPCH_Snappy.queryPlan(snc, isSnappy, useIndex)
     }

     println(s"****************queries : $queries")

     for(i <- 1 to 1) {
       for(query <- queries)
         query match {
           case "1" =>   TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i, useIndex)
           case "2" =>   TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy, i, useIndex)//taking hours to execute in Snappy
           case "3"=>   TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i, useIndex)
           case "4" =>   TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i, useIndex)
           case "5" =>   TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i, useIndex)
           case "6" =>   TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i, useIndex)
           case "7" =>   TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i, useIndex)
           case "8" =>   TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i, useIndex)
           case "9" =>   TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i, useIndex) //taking hours to execute in Snappy
           case "10" =>   TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i, useIndex)
           case "11" =>   TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i, useIndex)
           case "12" =>   TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i, useIndex)
           case "13" =>   TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i, useIndex)
           case "14" =>   TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i, useIndex)
           case "15" =>   TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i, useIndex)
           case "16" =>   TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i, useIndex)
           case "17" =>   TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i, useIndex)
           case "18" =>   TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i, useIndex)
           case "19" =>   TPCH_Snappy.execute("q19", snc,isResultCollection, isSnappy, i, useIndex) //not working in local mode hence not executing it for cluster mode too
           case "20" =>   TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i, useIndex)
           case "21" =>   TPCH_Snappy.execute("q21", snc,isResultCollection, isSnappy, i, useIndex) //not working in local mode hence not executing it for cluster mode too
           case "22" =>   TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i, useIndex)
             println("---------------------------------------------------------------------------------")
         }

 //      TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy, i)//taking hours to execute in Snappy
 //      TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i) //taking hours to execute in Snappy
 //      TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q19", snc,isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
 //      TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i)
 //      TPCH_Snappy.execute("q21", snc,isResultCollection, isSnappy, i) //not working in local mode hence not executing it for cluster mode too
 //      TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i)
 //      println("---------------------------------------------------------------------------------")
     }

     TPCH_Snappy.close()
   }

   override def validate(sc: C, config: Config): SparkJobValidation = {

     var sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
       config.getString("sparkSqlProps")
     }
     else " "

     sqlSparkProperties = sqlSparkProps.split(" ")

     queryPlan = if (config.hasPath("queryPlan")) {
       config.getBoolean("queryPlan")
     }else false

     var tempqueries = if (config.hasPath("queries")) {
       config.getString("queries")
     } else {
       return new SparkJobInvalid("Specify Query number to be executed")
     }

     useIndex = if (config.hasPath("useIndex")) {
       config.getBoolean("useIndex")
     } else {
       return new SparkJobInvalid("Specify whether to use Index")
     }

     println(s"tempqueries : $tempqueries")

     queries = tempqueries.split(",")

     SparkJobValid
   }
 }
