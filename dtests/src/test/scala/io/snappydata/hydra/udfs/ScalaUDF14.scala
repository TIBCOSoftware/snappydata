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

import org.apache.spark.sql.api.java.UDF14

// scalastyle:off
class ScalaUDF14 extends UDF14[String, String, String, String, String, String, String, String, String, String, String, String, String, String, String] {
  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String, t9: String, t10: String, t11: String, t12: String, t13: String, t14: String): String = {
    var str : String = t1.toUpperCase + t2 + t3 + t4 + t5 + t6 + t7.toUpperCase + t8 + t9 + t10 + " " +  t11.toUpperCase + t12 +
               " Spark " + t13 + t14
    return str
  }
}
