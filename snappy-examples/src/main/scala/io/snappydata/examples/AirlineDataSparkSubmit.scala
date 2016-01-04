package io.snappydata.examples

import java.net.InetAddress

import org.apache.spark.sql.{SnappyContext, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.snappy._

/**
 * Created by supriya on 18/12/15.
 */
object AirlineDataSparkSubmit {
  def main(args:Array[String])  {
    val hostName = InetAddress.getLocalHost.getHostName
    println("HostName is ",hostName)
    val locatorPort=10334
    val conf = new SparkConf().
        setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("snappydata.store.locators", s"localhost:$locatorPort")
    val sc = new SparkContext(conf)
    val snContext =SnappyContext(sc)
    val sqlContext = new SQLContext(sc)
    val airlineDF = snContext.table("airline")


    //Create a sample table
//    val samples = snContext.sql("select * from airline").count
   val samples = airlineDF.stratifiedSample(Map("qcs" -> "UniqueCarrier", "fraction" -> 0.01))
//    snContext.createExternalTable("sampledColTable", "column", airlineDF.schema,
  //            Map.empty[String, String])

 samples.write.mode(SaveMode.Append).format("column").options(Map[String, String]()).saveAsTable("sampledColTable")

    //val result=snContext.sql("select AVG(ArrDelay),UniqueCarrier,YearI,MonthI from sampledColTable group by UniqueCarrier,YearI,MonthI")
   println("Query result is ", samples)
  }
}
