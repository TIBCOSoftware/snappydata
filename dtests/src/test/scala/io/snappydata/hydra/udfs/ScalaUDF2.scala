package com.snappy.scala.poc.udf

import java.sql.Timestamp

import org.apache.spark.sql.api.java.UDF2

class ScalaUDF2 extends UDF2[Timestamp,Timestamp,String] {
  override def call(t1: Timestamp, t2: Timestamp): String = {
    val difference : Long = t2.getTime - t1.getTime
    val secs : Int = (difference / 1000).toInt
    val hours : Int = secs/3600
    val mintutes : Int = (secs%3600) / 60
    val seconds : Int = (secs%3600) % 60
    return  "Difference is : \n" + "Hours : " + hours.toString +  "\nMinutes : " + mintutes.toString + "\nSeconds : " + seconds.toString
  }
}
