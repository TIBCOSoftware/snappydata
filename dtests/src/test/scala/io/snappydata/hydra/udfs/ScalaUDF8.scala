package com.snappy.scala.poc.udf

import java.util
import java.util.HashMap

import org.apache.spark.sql.api.java.UDF8

class ScalaUDF8 extends UDF8[String,String,String,String,String,String,String,String,String] {
  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String): String = {

    val hashMap : HashMap[String,String] = new util.HashMap[String,String]()
    hashMap.put(t1, "Computation Engine")
    hashMap.put(t2,"In-memory database")
    hashMap.put(t3,"Storage layer")
    hashMap.put(t4,"Container Platform")
    hashMap.put(t5,"Cloud Platform")
    hashMap.put(t6,"Programming Language")
    hashMap.put(t7,"Bug/Task tracking tool")
    hashMap.put(t8,"Version control tool")

    val v1 : String = hashMap.get(t1)
    val v2 : String = hashMap.get(t2)
    val v3 : String = hashMap.get(t3)
    val v4 : String = hashMap.get(t4)
    val v5 : String = hashMap.get(t5)
    val v6 : String = hashMap.get(t6)
    val v7 : String = hashMap.get(t7)
    val v8 : String = hashMap.get(t8)

    val size : String = hashMap.size().toString

    return "[HashMap Size -> " + size + "\n"  + t1 + "->" + v1 + "\n"  + t2 + "->" + v2 + "\n"  + t3 + "->" + v3 +
    "\n"  + t4 + "->" + v4 + "\n"  + t5 + "->" + v5 + "\n"  + t6 + "->" + v6 + "\n"  + t7 + "->" + v7 +
    "\n"  + t8 + "->" + v8 + "]"
  }
}
