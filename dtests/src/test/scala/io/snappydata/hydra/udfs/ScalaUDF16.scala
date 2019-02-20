package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF16

class ScalaUDF16 extends UDF16[Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Byte,Boolean] {
  override def call(t1: Byte, t2: Byte, t3: Byte, t4: Byte, t5: Byte, t6: Byte, t7: Byte, t8: Byte, t9: Byte, t10: Byte, t11: Byte, t12: Byte, t13: Byte, t14: Byte, t15: Byte, t16: Byte): Boolean = {
    val result : Int = t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16
    if(result == 136)
      return  true
    else
      return false
  }
}
