package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF17

class ScalaUDF17 extends UDF17[Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long] {
  override def call(t1: Long, t2: Long, t3: Long, t4: Long, t5: Long, t6: Long, t7: Long, t8: Long, t9: Long, t10: Long, t11: Long, t12: Long, t13: Long, t14: Long, t15: Long, t16: Long, t17: Long): Long = {
    return t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16 + t17
  }
}
