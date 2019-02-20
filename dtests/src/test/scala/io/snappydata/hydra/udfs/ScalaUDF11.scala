package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF11

class ScalaUDF11 extends UDF11[Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float] {
  override def call(t1: Float, t2: Float, t3: Float, t4: Float, t5: Float, t6: Float, t7: Float, t8: Float, t9: Float, t10: Float, t11: Float): Float = {
    return t1 * t2 * t3 * t4 * t5 * t6 * t7 * t8 * t9 * t10 * t11;
  }
}
