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

import java.sql.Timestamp
import org.apache.spark.sql.api.java.UDF2

class ScalaUDF2 extends UDF2[Timestamp, Timestamp, String] {

  //  Purpose is to test the TimeStamp as an Input type.
  //  Below function, states the difference between two time stamps.

  override def call(t1: Timestamp, t2: Timestamp): String = {
    val difference : Long = t2.getTime - t1.getTime
    val secs : Int = (difference / 1000).toInt
    val hours : Int = secs/3600
    val mintutes : Int = (secs%3600) / 60
    val seconds : Int = (secs%3600) % 60
    return  "Difference is : \n" + "Hours : " + hours.toString +
      "\nMinutes : " + mintutes.toString +
      "\nSeconds : " + seconds.toString
  }
}
