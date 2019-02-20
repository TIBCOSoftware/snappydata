package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF9

class ScalaUDF9 extends  UDF9[Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,String] {
  override def call(t1: Boolean, t2: Boolean, t3: Boolean, t4: Boolean, t5: Boolean, t6: Boolean, t7: Boolean, t8: Boolean, t9: Boolean): String = {
    return (
      "false && false -> " + (t1 && t2) +
      "\nfalse && true -> "  + (t3 && t4) +
      "\ntrue && false -> " + (t5 && t6) +
      "\ntrue && true -> " + (t7 && t8) +
      "\nfalse || false -> " + (t1 || t2) +
      "\nfalse || true -> "  + (t3 || t4) +
      "\ntrue || false -> " + (t5 || t6) +
      "\ntrue || true -> " + (t7 || t8) +
      "\n!(true) Or  -> " + !t9
      )
  }
}
