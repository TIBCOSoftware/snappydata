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

import org.apache.spark.sql.api.java.UDF20
// scalastyle:off
class ScalaUDF20 extends UDF20[String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String] {

  //  User provides 20 Strings and remove the leading and trailing spaces from given String and returns the String without space.

  override def call(t1: String, t2: String, t3: String, t4: String, t5: String, t6: String, t7: String, t8: String, t9: String, t10: String, t11: String, t12: String, t13: String, t14: String, t15: String, t16: String, t17: String, t18: String, t19: String, t20: String): String = {
    var result : String = ""
    result = t1.replaceAll("\\s","") + ","
    result = result + t2.replaceAll("\\s","") + ","
    result = result + t3.replaceAll("\\s","") + ","
    result = result + t4.replaceAll("\\s", "") + ","
    result = result + t5.replaceAll("\\s","") + ","
    result = result + t6.replaceAll("\\s","") + ","
    result = result + t7.replaceAll("\\s","") + ","
    result = result + t8.replaceAll("\\s","") + ","
    result = result + t9.replaceAll("\\s","") + ","
    result = result + t10.replaceAll("\\s","") + ","
    result = result + t11.replaceAll("\\s","") + ","
    result = result + t12.replaceAll("\\s","") + ","
    result = result + t13.replaceAll("\\s","") + ","
    result = result + t14.replaceAll("\\s","") + ","
    result = result + t15.replaceAll("\\s","") + ","
    result = result + t16.replaceAll("\\s","") + ","
    result = result + t17.replaceAll("\\s","") + ","
    result = result + t18.replaceAll("\\s","") + ","
    result = result + t19.replaceAll("\\s","")
    result = result + t20.replaceAll("\\s","")
    return result;
  }
}
