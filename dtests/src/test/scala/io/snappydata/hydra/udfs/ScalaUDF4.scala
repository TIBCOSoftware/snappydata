package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF4

class ScalaUDF4 extends  UDF4[Int,Int,Int,Int,Double] {
  override def call(t1: Int, t2: Int, t3: Int, t4: Int): Double = {
    return (Math.sqrt(1024) + Math.sqrt(2048) + Math.sqrt(4096) + Math.sqrt(8192))
  }
}
