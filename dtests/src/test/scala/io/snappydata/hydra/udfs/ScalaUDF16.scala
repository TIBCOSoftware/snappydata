/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import org.apache.spark.sql.api.java.UDF16
// scalastyle:off
class ScalaUDF16 extends UDF16[Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Byte, Boolean] {

  //  Pupose is to test the TINYINT.
  //  Add all the numbers, check the result and return Boolean.

  override def call(t1: Byte, t2: Byte, t3: Byte, t4: Byte, t5: Byte, t6: Byte, t7: Byte, t8: Byte, t9: Byte, t10: Byte, t11: Byte, t12: Byte, t13: Byte, t14: Byte, t15: Byte, t16: Byte): Boolean = {
    val result : Int = t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16
    if(result == 136)
      return  true
    else
      return false
  }
}
