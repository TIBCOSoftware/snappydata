package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF3

class ScalaUDF3 extends  UDF3[Double,Double,Double,String] {
  override def call(t1: Double, t2: Double, t3: Double): String = {
    val sin : Double = Math.sin(t1)
    val cos : Double = Math.cos(t2)
    val tan : Double = Math.tan(t3)
    return "Sine :" + sin.toString + "\nCosine : " + cos.toString + "\n Tangent : " + tan.toString
  }
}
