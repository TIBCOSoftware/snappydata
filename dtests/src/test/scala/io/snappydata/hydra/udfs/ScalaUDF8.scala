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

import java.util
import java.util.HashMap
import org.apache.spark.sql.api.java.UDF8

class ScalaUDF8 extends UDF8[String, String, String, String,
  String, String, String, String, String] {

  //  Below function make the key-value pair from given input String.
  //  Calculate the size of Map, returns the size of the Map and all the key - value pair.

  override def call(t1: String, t2: String, t3: String, t4: String,
                    t5: String, t6: String, t7: String, t8: String): String = {
    val hashMap : HashMap[String, String] = new util.HashMap[String, String]()
    hashMap.put(t1, "Computation Engine")
    hashMap.put(t2, "In-memory database")
    hashMap.put(t3, "Storage layer")
    hashMap.put(t4, "Container Platform")
    hashMap.put(t5, "Cloud Platform")
    hashMap.put(t6, "Programming Language")
    hashMap.put(t7, "Bug/Task tracking tool")
    hashMap.put(t8, "Version control tool")

    val v1 : String = hashMap.get(t1)
    val v2 : String = hashMap.get(t2)
    val v3 : String = hashMap.get(t3)
    val v4 : String = hashMap.get(t4)
    val v5 : String = hashMap.get(t5)
    val v6 : String = hashMap.get(t6)
    val v7 : String = hashMap.get(t7)
    val v8 : String = hashMap.get(t8)

    val size : String = hashMap.size().toString

    return "[HashMap Size -> " +
    size + "\n"  + t1 + "->" + v1 + "\n"  + t2 + "->" + v2 + "\n"  +
     t3 + "->" + v3 + "\n"  + t4 + "->" + v4 + "\n"  + t5 + "->" + v5 + "\n"  +
     t6 + "->" + v6 + "\n"  + t7 + "->" + v7 + "\n"  + t8 + "->" + v8 + "]"
  }
}
