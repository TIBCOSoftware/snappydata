package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF15


class ScalaUDF15 extends UDF15[Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Boolean,Int]{
  override def call(t1: Boolean, t2: Boolean, t3: Boolean, t4: Boolean, t5: Boolean, t6: Boolean, t7: Boolean, t8: Boolean, t9: Boolean, t10: Boolean, t11: Boolean, t12: Boolean, t13: Boolean, t14: Boolean, t15: Boolean): Int = {
    var  num : Int = 0
    if(t1 == true)
      num = 1 * Math.pow(2.0,14.0).toInt
    else
      num = 0 * Math.pow(2.0,14.0).toInt

    if(t2 == true)
      num = num + (1 * Math.pow(2.0,13.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,13.0).toInt)

    if(t3 == true)
      num = num + (1 * Math.pow(2.0,12.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,12.0).toInt)

    if(t4 == true)
      num = num + (1 * Math.pow(2.0,11.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,11.0).toInt)

    if(t5 == true)
      num = num + (1 * Math.pow(2.0,10.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,10.0).toInt)

    if(t6 == true)
      num = num + (1 * Math.pow(2.0,9.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,9.0).toInt)

    if(t7 == true)
      num = num + (1 * Math.pow(2.0,8.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,8.0).toInt)

    if(t8 == true)
      num = num + (1 * Math.pow(2.0,7.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,7.0).toInt)

    if(t9 == true)
      num = num + (1 * Math.pow(2.0,6.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,6.0).toInt)

    if(t10 == true)
      num = num + (1 * Math.pow(2.0,5.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,5.0).toInt)

    if(t11 == true)
      num = num + (1 * Math.pow(2.0,4.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,4.0).toInt)

    if(t12 == true)
      num = num + (1 * Math.pow(2.0,3.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,3.0).toInt)

    if(t13 == true)
      num = num + (1 * Math.pow(2.0,2.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,2.0).toInt)

    if(t14 == true)
      num = num + (1 * Math.pow(2.0,1.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,1.0).toInt)

    if(t15 == true)
      num = num + (1 * Math.pow(2.0,0.0).toInt)
    else
      num = num + (0 * Math.pow(2.0,0.0).toInt)

    return num;
  }
}
