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

import org.apache.spark.sql.api.java.UDF10

class ScalaUDF10 extends UDF10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, String] {

  //  Below function find the factorial of given number,
  //  return the factorial of all given number as String.

  override def call(t1: Int, t2: Int, t3: Int, t4: Int, t5: Int,
                    t6: Int, t7: Int, t8: Int, t9: Int, t10: Int): String = {
      val f1 : Long = findFactorial(t1)
      val f2 : Long = findFactorial(t2)
      val f3 : Long = findFactorial(t3)
      val f4 : Long = findFactorial(t4)
      val f5 : Long = findFactorial(t5)
      val f6 : Long = findFactorial(t6)
      val f7 : Long = findFactorial(t7)
      val f8 : Long = findFactorial(t8)
      val f9 : Long = findFactorial(t9)
      val f10 : Long = findFactorial(t10)

      val result : String = "(" + f1.toString + ","  + f2.toString + ","  + f3.toString + "," +
        f4.toString + "," + f5.toString + ","  + f6.toString + ","  + f7.toString + ","  +
        f8.toString + "," + f9.toString + "," + f10.toString + ")"
      return result
  }

  def findFactorial(number : Int) : Long = {
    var r : Long = 1;
    for (i <- 1 to number) {
      r = r * i
    }
    return  r;
  }
}
