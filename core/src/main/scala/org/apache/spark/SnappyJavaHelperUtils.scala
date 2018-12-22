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
package org.apache.spark


object SnappyJavaHelperUtils {

  type JDouble = java.lang.Double

  def toJDouble(implicit v: Tuple1[Double]): Tuple1[JDouble] =
    Tuple1(java.lang.Double.valueOf(v._1))

  def toJDouble(implicit v: Tuple2[Double, Double]): Tuple2[JDouble, JDouble] =
    Tuple2(java.lang.Double.valueOf(v._1), java.lang.Double.valueOf(v._2))


  def toJDouble(implicit v: Tuple3[Double, Double, Double]): Tuple3[JDouble, JDouble, JDouble] =
    Tuple3(java.lang.Double.valueOf(v._1), java.lang.Double.valueOf(v._2),
      java.lang.Double.valueOf(v._3))


  def toJDouble(implicit v: Tuple4[Double, Double, Double, Double]): Tuple4[JDouble, JDouble, JDouble, JDouble] =
    Tuple4(java.lang.Double.valueOf(v._1), java.lang.Double.valueOf(v._2),
      java.lang.Double.valueOf(v._3), java.lang.Double.valueOf(v._4))
}
