package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF22

class ScalaUDF22 extends UDF22[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]{
  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Int, t6: Int, t7: Int, t8: Int, t9: Int, t10: Int, t11: Int, t12: Int, t13: Int, t14: Int, t15: Int, t16: Int, t17: Int, t18: Int, t19: Int, t20: Int, t21: Int, t22: Int): Int = {
    return t1 * t2 * t3 * t4 * t5 * t6 * t7 * t8 * t9 * t10 * t11 * t12 * t13 * t14 * t15 * t16 * t17 * t18 * t19 * t20 * t21 * t22;
  }
}
