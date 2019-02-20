package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF10

class ScalaUDF10 extends UDF10[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,String] {
  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Int, t6: Int, t7: Int, t8: Int, t9: Int, t10: Int): String = {
      val f1 : Long = findFactorial(t1);
      val f2 : Long = findFactorial(t2);
      val f3 : Long  = findFactorial(t3);
      val f4 : Long  = findFactorial(t4);
      val f5 : Long  = findFactorial(t5);
      val f6 : Long  = findFactorial(t6);
      val f7 : Long  = findFactorial(t7);
      val f8 : Long  = findFactorial(t8);
      val f9 : Long  = findFactorial(t9);
      val f10 : Long  = findFactorial(t10);

      val result : String = "(" + f1.toString + ","  + f2.toString + ","  + f3.toString + "," + f4.toString + "," + f5.toString + ","  +
                                               f6.toString + ","  + f7.toString + ","  + f8.toString + "," + f9.toString + "," + f10.toString + ")"

      return result

  }

  def findFactorial(number : Int) : Long = {
    var r : Long = 1;
    for(i <-1 to number) {
      r = r * i
    }
    return  r;
  }
}
