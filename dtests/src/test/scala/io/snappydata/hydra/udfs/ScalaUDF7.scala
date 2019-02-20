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

import org.apache.spark.sql.api.java.UDF7
import scala.collection.mutable
import scala.collection.mutable.WrappedArray
//  import scala.collection.mutable.ArrayBuffer

class ScalaUDF7 extends UDF7[WrappedArray[Double], WrappedArray[Double], WrappedArray[Double],
  WrappedArray[Double], WrappedArray[Double], WrappedArray[Double], WrappedArray[Double], String] {
  override def call(t1: mutable.WrappedArray[Double], t2: mutable.WrappedArray[Double],
                    t3: mutable.WrappedArray[Double], t4: mutable.WrappedArray[Double],
                    t5: mutable.WrappedArray[Double], t6: mutable.WrappedArray[Double],
                    t7: mutable.WrappedArray[Double]): String = {
    var v1 : Double = 0.0
    var v2 : Double = 0.0
    var v3 : Double = 0.0
    var v4 : Double = 0.0
    var v5 : Double = 0.0
    var v6 : Double = 0.0
    var v7 : Double = 0.0

    for(i <- 0 to t1.length-1) {
        v1 = v1 + t1(i)
    }
    v1 = (v1 / t1.length)

    for(i <- 0 to t2.length-1) {
      v2 = v2 + t2(i)
    }
    v2 = (v2 / t2.length)

    for(i <- 0 to t3.length-1) {
      v3 = v3 + t3(i)
    }
    v3 = (v3 / t3.length)

    for(i <- 0 to t4.length-1) {
      v4 = v4 + t4(i)
    }
    v4 = (v4 / t4.length)

    for(i <- 0 to t5.length-1) {
      v5 = v5 + t5(i)
    }
    v5 = (v5 / t5.length)

    for(i <- 0 to t6.length-1) {
      v6 = v6 + t6(i)
    }
    v6 = (v6 / t6.length)

    for(i <- 0 to t7.length-1) {
      v7 = v7 + t7(i)
    }
    v7 = (v7 / t7.length)

    return "V1 : " + v1.toString +
    "\n V2 : " + v2.toString +
    "\n V2 : " + v3.toString +
    "\n V2 : " + v4.toString +
    "\n V2 : " + v5.toString +
    "\n V2 : " + v6.toString +
    "\n V2 : " + v7.toString

  }
}


//  class ScalaUDF7 extends UDF7[ArrayBuffer[Double], ArrayBuffer[Double], ArrayBuffer[Double],
// ArrayBuffer[Double],  ArrayBuffer[Double], ArrayBuffer[Double], ArrayBuffer[Double], String] {
//  override def call(t1: ArrayBuffer[Double], t2: ArrayBuffer[Double], t3: ArrayBuffer[Double],
//  t4: ArrayBuffer[Double],  t5: ArrayBuffer[Double], t6: ArrayBuffer[Double],
//  t7: ArrayBuffer[Double]): String = {
//    var v1 : Double = 0.0
//    for(i <- 0 to t1.length) {
//      v1 = v1 + t1(i)
//    }
//    v1 = v1 / t1.length
//
//    return  "V1 : " + v1
//    return "Success"
//  }
// }
