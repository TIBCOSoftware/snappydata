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
package io.snappydata.hydra.concurrency

import java.io.PrintWriter

import io.snappydata.hydra.SnappyTestUtils
import io.snappydata.hydra.northwind.{NWPLQueries, NWQueries}
import org.apache.spark.sql.{SQLContext, SnappyContext}

object ConcTestUtils {
  def validateAnalyticalQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q37" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q37, "Q37",
          tableType, pw, sqlContext)
        case "Q55" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q55, "Q55",
          tableType, pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q36, "Q36",
          tableType, pw, sqlContext)
        case "Q56" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q56, "Q56",
          tableType, pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertJoinFullResultSet(snc, NWQueries.Q38, "Q38",
          tableType, pw, sqlContext)
        // scalastyle:off println
        case _ => println("OK")
        // scalastyle:on println
      }
    }
  }

  def validatePointLookUPQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    for (q <- NWPLQueries.queries) {
      q._1 match {
        case "Q1" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q1, "Q1",
          tableType, pw, sqlContext)
        case "Q2" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q2, "Q2",
          tableType, pw, sqlContext)
        case "Q3" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q3, "Q3",
          tableType, pw, sqlContext)
        case "Q4" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q4, "Q4",
          tableType, pw, sqlContext)
        case "Q5" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q5, "Q5",
          tableType, pw, sqlContext)
        case "Q6" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q6, "Q6",
          tableType, pw, sqlContext)
        case "Q7" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q7, "Q7",
          tableType, pw, sqlContext)
        case "Q8" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q8, "Q8",
          tableType, pw, sqlContext)
        case "Q9" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q9, "Q9",
          tableType, pw, sqlContext)
        case "Q10" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q5, "Q10",
          tableType, pw, sqlContext)
        case "Q11" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q11, "Q11",
          tableType, pw, sqlContext)
        case "Q12" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q12, "Q12",
          tableType, pw, sqlContext)
        case "Q13" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q13, "Q13",
          tableType, pw, sqlContext)
        case "Q14" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q14, "Q14",
          tableType, pw, sqlContext)
        case "Q15" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q15, "Q15",
          tableType, pw, sqlContext)
        case "Q16" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q16, "Q16",
          tableType, pw, sqlContext)
        case "Q17" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q17, "Q17",
          tableType, pw, sqlContext)
        case "Q18" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q18, "Q18",
          tableType, pw, sqlContext)
        case "Q19" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q19, "Q19",
          tableType, pw, sqlContext)
        case "Q20" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q20, "Q20",
          tableType, pw, sqlContext)
        case "Q21" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q21, "Q21",
          tableType, pw, sqlContext)
        case "Q22" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q22, "Q22",
          tableType, pw, sqlContext)
        case "Q23" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q23, "Q23",
          tableType, pw, sqlContext)
        case "Q24" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q24, "Q24",
          tableType, pw, sqlContext)
        case "Q25" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q25, "Q25",
          tableType, pw, sqlContext)
        case "Q26" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q26, "Q28",
          tableType, pw, sqlContext)
        case "Q27" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q27, "Q27",
          tableType, pw, sqlContext)
        case "Q28" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q28, "Q28",
          tableType, pw, sqlContext)
        case "Q29" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q29, "Q29",
          tableType, pw, sqlContext)
        case "Q30" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q30, "Q30",
          tableType, pw, sqlContext)
        case "Q31" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q31, "Q31",
          tableType, pw, sqlContext)
        case "Q32" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q32, "Q32",
          tableType, pw, sqlContext)
        case "Q33" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q33, "Q33",
          tableType, pw, sqlContext)
        case "Q34" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q34, "Q34",
          tableType, pw, sqlContext)
        case "Q35" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q35, "Q35",
          tableType, pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q36, "Q36",
          tableType, pw, sqlContext)
        case "Q37" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q37, "Q37",
          tableType, pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q38, "Q38",
          tableType, pw, sqlContext)
        case "Q39" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q39, "Q39",
          tableType, pw, sqlContext)
        case "Q40" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q40, "Q40",
          tableType, pw, sqlContext)
        case "Q41" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q41, "Q41",
          tableType, pw, sqlContext)
        case "Q42" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q42, "Q42",
          tableType, pw, sqlContext)
        case "Q43" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q43, "Q43",
          tableType, pw, sqlContext)
        case "Q44" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q44, "Q44",
          tableType, pw, sqlContext)
        case "Q45" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q45, "Q45",
          tableType, pw, sqlContext)
        case "Q46" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q46, "Q46",
          tableType, pw, sqlContext)
        case "Q47" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q47, "Q47",
          tableType, pw, sqlContext)
        case "Q48" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q48, "Q48",
          tableType, pw, sqlContext)
        case "Q49" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q49, "Q49",
          tableType, pw, sqlContext)
        case "Q50" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q50, "Q50",
          tableType, pw, sqlContext)
        case "Q51" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q51, "Q51",
          tableType, pw, sqlContext)
        /* case "Q52" => SnappyTestUtils.assertQueryFullResultSet(snc, NWPLQueries.Q52, "Q52",
          tableType, pw, sqlContext) */
        // scalastyle:off println
        case _ => println("OK")
        // scalastyle:on println
      }
    }
  }

}
