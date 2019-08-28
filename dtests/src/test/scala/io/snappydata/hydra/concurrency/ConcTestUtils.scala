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
package io.snappydata.hydra.concurrency

import java.io.PrintWriter

import io.snappydata.hydra.SnappyTestUtils
import io.snappydata.hydra.northwind.{NWPLQueries, NWQueries}
import org.apache.spark.sql.{SQLContext, SnappyContext}

object ConcTestUtils {
  def validateAnalyticalQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    SnappyTestUtils.validateFullResultSet = true;
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q37" => SnappyTestUtils.assertJoin(snc, NWQueries.Q37, "Q37",
          pw, sqlContext)
        case "Q55" => SnappyTestUtils.assertJoin(snc, NWQueries.Q55, "Q55",
          pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertJoin(snc, NWQueries.Q36, "Q36",
          pw, sqlContext)
        case "Q56" => SnappyTestUtils.assertJoin(snc, NWQueries.Q56, "Q56",
          pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertJoin(snc, NWQueries.Q38, "Q38",
          pw, sqlContext)
        // scalastyle:off println
        case _ => println("OK")
        // scalastyle:on println
      }
    }
  }

  def validatePointLookUPQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    SnappyTestUtils.validateFullResultSet = true;
    for (q <- NWPLQueries.queries) {
      q._1 match {
        case "Q1" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q1, "Q1",
          pw, sqlContext)
        case "Q2" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q2, "Q2",
          pw, sqlContext)
        case "Q3" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q3, "Q3",
          pw, sqlContext)
        case "Q4" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q4, "Q4",
          pw, sqlContext)
        case "Q5" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q5, "Q5",
          pw, sqlContext)
        case "Q6" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q6, "Q6",
          pw, sqlContext)
        case "Q7" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q7, "Q7",
          pw, sqlContext)
        case "Q8" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q8, "Q8",
          pw, sqlContext)
        case "Q9" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q9, "Q9",
          pw, sqlContext)
        case "Q10" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q5, "Q10",
          pw, sqlContext)
        case "Q11" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q11, "Q11",
          pw, sqlContext)
        case "Q12" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q12, "Q12",
          pw, sqlContext)
        case "Q13" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q13, "Q13",
          pw, sqlContext)
        case "Q14" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q14, "Q14",
          pw, sqlContext)
        case "Q15" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q15, "Q15",
          pw, sqlContext)
        case "Q16" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q16, "Q16",
          pw, sqlContext)
        case "Q17" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q17, "Q17",
          pw, sqlContext)
        case "Q18" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q18, "Q18",
          pw, sqlContext)
        case "Q19" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q19, "Q19",
          pw, sqlContext)
        case "Q20" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q20, "Q20",
          pw, sqlContext)
        case "Q21" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q21, "Q21",
          pw, sqlContext)
        case "Q22" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q22, "Q22",
          pw, sqlContext)
        case "Q23" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q23, "Q23",
          pw, sqlContext)
        case "Q24" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q24, "Q24",
          pw, sqlContext)
        case "Q25" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q25, "Q25",
          pw, sqlContext)
        case "Q26" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q26, "Q28",
          pw, sqlContext)
        case "Q27" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q27, "Q27",
          pw, sqlContext)
        case "Q28" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q28, "Q28",
          pw, sqlContext)
        case "Q29" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q29, "Q29",
          pw, sqlContext)
        case "Q30" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q30, "Q30",
          pw, sqlContext)
        case "Q31" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q31, "Q31",
          pw, sqlContext)
        case "Q32" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q32, "Q32",
          pw, sqlContext)
        case "Q33" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q33, "Q33",
          pw, sqlContext)
        case "Q34" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q34, "Q34",
          pw, sqlContext)
        case "Q35" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q35, "Q35",
          pw, sqlContext)
        case "Q36" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q36, "Q36",
          pw, sqlContext)
        case "Q37" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q37, "Q37",
          pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q38, "Q38",
          pw, sqlContext)
        case "Q39" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q39, "Q39",
          pw, sqlContext)
        case "Q40" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q40, "Q40",
          pw, sqlContext)
        case "Q41" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q41, "Q41",
          pw, sqlContext)
        case "Q42" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q42, "Q42",
          pw, sqlContext)
        case "Q43" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q43, "Q43",
          pw, sqlContext)
        case "Q44" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q44, "Q44",
          pw, sqlContext)
        case "Q45" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q45, "Q45",
          pw, sqlContext)
        case "Q46" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q46, "Q46",
          pw, sqlContext)
        case "Q47" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q47, "Q47",
          pw, sqlContext)
        case "Q48" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q48, "Q48",
          pw, sqlContext)
        case "Q49" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q49, "Q49",
          pw, sqlContext)
        case "Q50" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q50, "Q50",
          pw, sqlContext)
        case "Q51" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q51, "Q51",
          pw, sqlContext)
        /* case "Q52" => SnappyTestUtils.assertQuery(snc, NWPLQueries.Q52, "Q52",
          pw, sqlContext) */
        // scalastyle:off println
        case _ => pw.println(s"Query ${q._1} has not been executed.")
        // scalastyle:on println
      }
    }
  }

}
