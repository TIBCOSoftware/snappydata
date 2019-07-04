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

import org.apache.spark.sql.api.java.UDF15
// scalastyle:off
class ScalaUDF15 extends UDF15[Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Int]{

   // Purspose is to test the Boolean as input type.
  //  Below function peform the Math.pow function based on true or false value.
  //  We will add these numbers and returns the result.


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
