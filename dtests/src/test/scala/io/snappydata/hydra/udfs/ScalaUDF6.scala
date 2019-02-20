package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF6


class ScalaUDF6 extends UDF6[Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Double]{
  override def call(t1: Map[String, Double], t2: Map[String, Double], t3: Map[String, Double], t4: Map[String, Double], t5: Map[String, Double], t6: Map[String, Double]): Double = {
    val v1 : Option[Double] = t1.get("Maths")
    val v2 : Option[Double] = t2.get("Science")
    val v3 : Option[Double]= t3.get("English")
    val v4 : Option[Double]  = t4.get("Social Studies")
    val v5 : Option[Double] = t5.get("Computer")
    val v6 : Option[Double] = t6.get("Music")

    return v1.get + v2.get + v3.get + v4.get + v5.get + v6.get
  }
}
