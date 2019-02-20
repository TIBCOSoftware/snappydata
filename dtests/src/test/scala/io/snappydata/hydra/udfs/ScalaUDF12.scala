package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF12


class ScalaUDF12 extends UDF12[String,Int,Int,String,Long,Long,String,Float,Float,String,Short,Short,String] {
  override def call(t1: String, t2: Int, t3: Int, t4: String, t5: Long, t6: Long, t7: String, t8: Float, t9: Float, t10: String, t11: Short, t12: Short): String = {
    var result : String = ""

    if(t1 == "+") {
      val add : String = (t2 + t3).toString
      result = "Addition -> " + add
    }

    if(t4 == "-") {
      val minus : String = (t5 - t6).toString
      result = result + " , Substraction -> " + minus
    }

    if(t7 == "*") {
      val multiple : String = (t8 *  t9).toString
      result = result + " , Multiplication -> " + multiple
    }

    if(t10 == "/") {
      val division : String = (t11 / t12).toString
      result = result  + " , Division -> " + division
    }

    return result
  }
}
