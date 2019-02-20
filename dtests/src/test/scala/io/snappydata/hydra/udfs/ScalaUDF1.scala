package com.snappy.scala.poc.udf

import java.io.{FileOutputStream, PrintWriter}
import java.sql.Timestamp
import java.io.File
import org.apache.spark.sql.api.java.UDF1

class ScalaUDF1 extends UDF1[String,Timestamp]{
  override def call(t1: String): Timestamp = {
    val str = t1.toUpperCase;
    //val path = System.getProperty("user.dir")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File("/home/cbhatt/udf1.txt"),false))
    //pw.write(path)
    pw.write(str);
    pw.flush();
    pw.close();
    val ts : Timestamp = new Timestamp(System.currentTimeMillis());
    return ts;
  }
}
