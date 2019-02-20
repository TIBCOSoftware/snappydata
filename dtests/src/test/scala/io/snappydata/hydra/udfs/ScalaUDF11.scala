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

import org.apache.spark.sql.api.java.UDF11
// scalastyle:off
class ScalaUDF11 extends UDF11[Float, Float, Float, Float, Float, Float,
                                                          Float, Float, Float, Float, Float, Float] {
  override def call(t1: Float, t2: Float, t3: Float, t4: Float, t5: Float,
                    t6: Float, t7: Float, t8: Float, t9: Float, t10: Float, t11: Float): Float = {
    return t1 * t2 * t3 * t4 * t5 * t6 * t7 * t8 * t9 * t10 * t11;
  }
}
