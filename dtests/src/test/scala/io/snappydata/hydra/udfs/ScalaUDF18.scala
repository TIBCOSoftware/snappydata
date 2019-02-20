/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra.udfs

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.Date
import org.apache.spark.sql.api.java.UDF18
// scalastyle:off
class ScalaUDF18 extends  UDF18[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long,Date]{
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
