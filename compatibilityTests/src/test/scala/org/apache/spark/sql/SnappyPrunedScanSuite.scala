/* 
* Licensed to the Apache Software Foundation (ASF) under one or more 
* contributor license agreements.  See the NOTICE file distributed with 
* this work for additional information regarding copyright ownership. 
* The ASF licenses this file to You under the Apache License, Version 2.0 
* (the "License"); you may not use this file except in compliance with 
* the License.  You may obtain a copy of the License at 
* 
*    http://www.apache.org/licenses/LICENSE-2.0 
* 
* Unless required by applicable law or agreed to in writing, software 
* distributed under the License is distributed on an "AS IS" BASIS, 
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
* See the License for the specific language governing permissions and 
* limitations under the License. 
*/
package org.apache.spark.sql

import org.apache.spark.sql.sources.PrunedScanSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyPrunedScanSuite extends PrunedScanSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] = Seq(
    "Columns output b,a: SELECT b, a FROM oneToTenPruned",
    "Columns output a: SELECT a FROM oneToTenPruned",
    "Columns output b: SELECT b FROM oneToTenPruned"
  )
}
