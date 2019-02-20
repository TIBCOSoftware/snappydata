package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF13
import scala.math.BigDecimal
//import java.math.BigDecimal

class BadCase_ScalaUDF13 extends UDF13[BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal]{
  override def call(t1: BigDecimal, t2: BigDecimal, t3: BigDecimal, t4: BigDecimal, t5: BigDecimal, t6: BigDecimal, t7: BigDecimal, t8: BigDecimal, t9: BigDecimal, t10: BigDecimal, t11: BigDecimal, t12: BigDecimal, t13: BigDecimal): BigDecimal = {
//    val bigDecimal : BigDecimal = new BigDecimal(145.23)
    val bigDecimal : BigDecimal = 145.23
    return bigDecimal
  }
}
