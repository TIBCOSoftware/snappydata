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

import org.apache.spark.sql.api.java.UDF19
// scalastyle:off
class ScalaUDF19 extends UDF19[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]{

  //  Add all the 19 Double numbers and return Double.
  //  Purpose is to test Double as input and output.

  override def call(t1: Double, t2: Double, t3: Double, t4: Double, t5: Double, t6: Double, t7: Double, t8: Double, t9: Double, t10: Double, t11: Double, t12: Double, t13: Double, t14: Double, t15: Double, t16: Double, t17: Double, t18: Double, t19: Double): Double = {
    return t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16 + t17 + t18 + t19;
  }
}
