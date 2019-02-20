package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF19

class ScalaUDF19 extends UDF19[Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double]{
  override def call(t1: Double, t2: Double, t3: Double, t4: Double, t5: Double, t6: Double, t7: Double, t8: Double, t9: Double, t10: Double, t11: Double, t12: Double, t13: Double, t14: Double, t15: Double, t16: Double, t17: Double, t18: Double, t19: Double): Double = {
    return t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16 + t17 + t18 + t19;
  }
}
