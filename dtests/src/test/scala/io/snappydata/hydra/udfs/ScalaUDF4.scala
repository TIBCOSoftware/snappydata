/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import org.apache.spark.sql.api.java.UDF4

class ScalaUDF4 extends  UDF4[Int, Int, Int, Int, Double] {

  //  Below function perform the sqrt operation of 1024, 2048, 4096, 8192.
  //  After performing sqrt operations, add all the numbers and return it.

  override def call(t1: Int, t2: Int, t3: Int, t4: Int): Double = {
    return (Math.sqrt(1024) + Math.sqrt(2048) + Math.sqrt(4096) + Math.sqrt(8192))
  }
}
