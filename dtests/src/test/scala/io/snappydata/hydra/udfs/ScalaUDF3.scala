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

import org.apache.spark.sql.api.java.UDF3

class ScalaUDF3 extends  UDF3[Double, Double, Double, String] {
  override def call(t1: Double, t2: Double, t3: Double): String = {
    val sin : Double = Math.sin(t1)
    val cos : Double = Math.cos(t2)
    val tan : Double = Math.tan(t3)
    return "Sine :" + sin.toString + "\nCosine : " + cos.toString + "\n Tangent : " + tan.toString
  }
}
