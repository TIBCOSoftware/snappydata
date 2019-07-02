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

import org.apache.spark.sql.api.java.UDF21
// scalastyle:off
class ScalaUDF21 extends  UDF21[String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Float]{

  //  Purpose is to test the Float as output.
  //  Below function returns the average of 21 numbers.

  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String, t9: String, t10: String, t11: String, t12: String, t13: String, t14: String, t15: String, t16: String, t17: String, t18: String, t19: String, t20: String, t21: String): Float = {
    val i1 : Int = t1.toInt
    val i2 : Int = t2.toInt
    val i3 : Int = t3.toInt
    val i4 : Int = t4.toInt
    val i5 : Int = t5.toInt
    val i6 : Int = t6.toInt
    val i7 : Int = t7.toInt
    val i8 : Int = t8.toInt
    val i9 : Int = t9.toInt
    val i10 : Int = t10.toInt
    val i11 : Int = t11.toInt
    val i12 : Int = t12.toInt
    val i13 : Int = t13.toInt
    val i14 : Int = t14.toInt
    val i15 : Int = t15.toInt
    val i16 : Int = t16.toInt
    val i17 : Int = t17.toInt
    val i18 : Int = t18.toInt
    val i19 : Int = t19.toInt
    val i20 : Int = t20.toInt
    val i21 : Int = t21.toInt
    val avg : Float = ((i1 + i2 +i3 + i4 + i5 +i6 + i7 + i8 +i9 + i10 + i11 +i12 + i13 + i14 +i15 + i16 + i17 +i18 + i19 + i20 +i21) / 21.0f)
    return avg
  }
}
