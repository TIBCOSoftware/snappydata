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

import org.apache.spark.sql.api.java.UDF12
// scalastyle:off
class ScalaUDF12 extends UDF12[String, Int, Int, String, Long, Long, String,
                                                          Float, Float, String, Short, Short, String] {
  override def call(t1: String, t2: Int, t3: Int, t4: String, t5: Long, t6: Long,
                    t7: String, t8: Float, t9: Float, t10: String, t11: Short, t12: Short): String = {

    var result : String = ""

    if(t1 == "+") {
      val add : String = (t2 + t3).toString
      result = "Addition -> " + add
    }

    if(t4 == "-") {
      val minus : String = (t5 - t6).toString
      result = result + " , Substraction -> " + minus
    }

    if(t7 == "*") {
      val multiple : String = (t8 *  t9).toString
      result = result + " , Multiplication -> " + multiple
    }

    if(t10 == "/") {
      val division : String = (t11 / t12).toString
      result = result  + " , Division -> " + division
    }

    return result
  }
}
