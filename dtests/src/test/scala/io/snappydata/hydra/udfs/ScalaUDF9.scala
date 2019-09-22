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

import org.apache.spark.sql.api.java.UDF9

class ScalaUDF9 extends  UDF9[Boolean, Boolean, Boolean, Boolean,
  Boolean, Boolean, Boolean, Boolean, Boolean, String] {

  //  Purpose is to test the Boolean type as input type.
  //  Below function perform the logical AND, logical OR and logical NOT operation
  //  return the result as String.


  override def call(t1: Boolean, t2: Boolean, t3: Boolean, t4: Boolean,
                    t5: Boolean, t6: Boolean, t7: Boolean, t8: Boolean, t9: Boolean): String = {
    return (
      "false && false -> " + (t1 && t2) +
      "\nfalse && true -> "  + (t3 && t4) +
      "\ntrue && false -> " + (t5 && t6) +
      "\ntrue && true -> " + (t7 && t8) +
      "\nfalse || false -> " + (t1 || t2) +
      "\nfalse || true -> "  + (t3 || t4) +
      "\ntrue || false -> " + (t5 || t6) +
      "\ntrue || true -> " + (t7 || t8) +
      "\n!(true) Or  -> " + !t9
      )
  }
}
