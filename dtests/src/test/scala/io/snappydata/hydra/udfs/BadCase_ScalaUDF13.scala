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

import org.apache.spark.sql.api.java.UDF13
import scala.math.BigDecimal
//  import java.math.BigDecimal

class BadCase_ScalaUDF13 extends UDF13 [BigDecimal, BigDecimal, BigDecimal, BigDecimal,
  BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal,
  BigDecimal, BigDecimal, BigDecimal, BigDecimal] {
  // scalastyle:off

  //  For input/output types:
  //  The framework always returns the Java types to the UDFs.
  //  So, if you are writing scala.math.BigDecimal as an input type or output type, an exception is reported.
  //  You can use java.math.BigDecimal in the SCALA code.
  //  So verify the above statements using scala.math.BigDecimal.


  override def call (t1: BigDecimal, t2: BigDecimal, t3: BigDecimal, t4: BigDecimal, t5: BigDecimal,
                    t6: BigDecimal, t7: BigDecimal, t8: BigDecimal, t9: BigDecimal, t10: BigDecimal,
                     t11: BigDecimal, t12: BigDecimal, t13: BigDecimal): BigDecimal = {
//    val bigDecimal : BigDecimal = new BigDecimal(145.23)
    val bigDecimal : BigDecimal = 145.23
    return bigDecimal
  }
}
