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

import org.apache.spark.sql.api.java.UDF17
// scalastyle:off
class ScalaUDF17 extends UDF17[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long] {

  //  Add all the 17 Long numbers and return the Long number.
  //  Purpose is to test the Long as input and output.

  override def call(t1: Long, t2: Long, t3: Long, t4: Long, t5: Long, t6: Long, t7: Long, t8: Long, t9: Long, t10: Long, t11: Long, t12: Long, t13: Long, t14: Long, t15: Long, t16: Long, t17: Long): Long = {
    return t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 + t14 + t15 + t16 + t17
  }
}
