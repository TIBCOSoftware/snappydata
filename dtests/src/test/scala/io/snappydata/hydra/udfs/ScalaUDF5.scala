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

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF5

class ScalaUDF5 extends UDF5[Int, Int, Int, Int, Row, String] {

  //  Purpose is to test the Struct data type as input type.
  //  Return the sum all four integer numbers, individual element of Struct data type as String.

  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Row): String = {
    val sum : Int = t1 + t2 + t3 + t4
    val i : Int = t5.getInt(0)
    val d : Double = t5.getDouble(1)
    val s : String = t5.getString(2)
    val b : Boolean = t5.isNullAt(3)
    return  "Sum of four integer : " + sum.toString +
      ", Struct data type element : (" +
      i.toString  + "," + d.toString + "," +  s + "," + b.toString + ")"
  }
}
