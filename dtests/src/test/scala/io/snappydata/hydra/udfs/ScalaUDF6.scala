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

import org.apache.spark.sql.api.java.UDF6


class ScalaUDF6 extends UDF6[Map[String, Double], Map[String, Double], Map[String, Double],
  Map[String, Double], Map[String, Double], Map[String, Double], Double] {

  //  Purpose is to test the Map Type as input.
  //  Get the marks of subject from Map and returns the sum of all marks.

  override def call(t1: Map[String, Double], t2: Map[String, Double], t3: Map[String, Double],
                    t4: Map[String, Double], t5: Map[String, Double],
                    t6: Map[String, Double]): Double = {
    val v1 : Option[Double] = t1.get("Maths")
    val v2 : Option[Double] = t2.get("Science")
    val v3 : Option[Double] = t3.get("English")
    val v4 : Option[Double] = t4.get("Social Studies")
    val v5 : Option[Double] = t5.get("Computer")
    val v6 : Option[Double] = t6.get("Music")

    return v1.get + v2.get + v3.get + v4.get + v5.get + v6.get
  }
}
