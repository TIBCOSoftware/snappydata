package com.snappy.scala.poc.udf

import java.io.{FileOutputStream, PrintWriter, File}
import org.apache.spark.sql.api.java.UDF18
import java.sql.Date

class ScalaUDF18 extends  UDF18[Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Long,Date]{
  override def call(t1: Long, t2: Long, t3: Long, t4: Long, t5: Long, t6: Long, t7: Long, t8: Long, t9: Long, t10: Long, t11: Long, t12: Long, t13: Long, t14: Long, t15: Long, t16: Long, t17: Long, t18: Long): Date = {
    val result : Long = t1 + t2 + t3 + t4 +  t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16 + t17 + t18;
    //val path : String = System.getProperty("user.dir")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File("/home/cbhatt/udf18.txt"), false))
    //pw.write(path)
    pw.write("Sum of 18 Long numbers are : " + result.toString)
    pw.flush()
    pw.close()
    val date : Date = new Date(System.currentTimeMillis());
    return  date
  }
}
