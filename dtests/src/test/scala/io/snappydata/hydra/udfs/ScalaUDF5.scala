package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF5
import org.apache.spark.sql.Row

class ScalaUDF5 extends UDF5[Int,Int,Int,Int,Row,String] {
  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Row): String = {
    val sum : Int = t1 + t2 + t3 + t4
    val i : Int = t5.getInt(0)
    val d : Double = t5.getDouble(1)
    val s : String = t5.getString(2)
    val b : Boolean = t5.isNullAt(3)
    return  "Sum of four integer : " + sum.toString + ", Struct data type element : (" + i.toString  + "," + d.toString + "," +  s + "," + b.toString +")"
  }
}
