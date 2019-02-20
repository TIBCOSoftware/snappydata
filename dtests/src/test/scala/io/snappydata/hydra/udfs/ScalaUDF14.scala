package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF14

class ScalaUDF14 extends UDF14[String,String,String,String,String,String,String,String,String,String,String,String,String,String,String] {
  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String, t9: String, t10: String, t11: String, t12: String, t13: String, t14: String): String = {
    var str : String = t1.toUpperCase + t2 + t3 + t4 + t5 + t6 + t7.toUpperCase + t8 + t9 + t10 + " " +  t11.toUpperCase + t12 +
               " Spark " + t13 + t14;
    return str;
  }
}
