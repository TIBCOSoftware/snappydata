package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF20

class ScalaUDF20 extends UDF20[String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String] {
  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String, t9: String, t10: String, t11: String, t12: String, t13: String, t14: String, t15: String, t16: String, t17: String, t18: String, t19: String, t20: String): String = {
    var result : String = ""
    result = t1.replaceAll("\\s","") + ","
    result = result + t2.replaceAll("\\s","") + ","
    result = result + t3.replaceAll("\\s","") + ","
    result = result + t4.replaceAll("\\s", "") + ","
    result = result + t5.replaceAll("\\s","") + ","
    result = result + t6.replaceAll("\\s","") + ","
    result = result + t7.replaceAll("\\s","") + ","
    result = result + t8.replaceAll("\\s","") + ","
    result = result + t9.replaceAll("\\s","") + ","
    result = result + t10.replaceAll("\\s","") + ","
    result = result + t11.replaceAll("\\s","") + ","
    result = result + t12.replaceAll("\\s","") + ","
    result = result + t13.replaceAll("\\s","") + ","
    result = result + t14.replaceAll("\\s","") + ","
    result = result + t15.replaceAll("\\s","") + ","
    result = result + t16.replaceAll("\\s","") + ","
    result = result + t17.replaceAll("\\s","") + ","
    result = result + t18.replaceAll("\\s","") + ","
    result = result + t19.replaceAll("\\s","")
    result = result + t20.replaceAll("\\s","")
    return result;
  }
}
