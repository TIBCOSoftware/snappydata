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

import org.apache.spark.sql.api.java.UDF22
// scalastyle:off
class ScalaUDF22 extends UDF22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]{
  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Int, t6: Int, t7: Int, t8: Int, t9: Int, t10: Int, t11: Int, t12: Int, t13: Int, t14: Int, t15: Int, t16: Int, t17: Int, t18: Int, t19: Int, t20: Int, t21: Int, t22: Int): Int = {
    return t1 * t2 * t3 * t4 * t5 * t6 * t7 * t8 * t9 * t10 * t11 * t12 * t13 * t14 * t15 * t16 * t17 * t18 * t19 * t20 * t21 * t22;
  }
}
