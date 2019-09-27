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

package io.snappydata.hydra.spva


import java.io.PrintWriter

import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql.{SQLContext, SnappyContext}


object SPVATestUtil {

  def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table)

    snc.sql(SPVAQueries.encounters_table)

    snc.sql(SPVAQueries.allergies_table)

    snc.sql(SPVAQueries.careplans_table)

    snc.sql(SPVAQueries.conditions_table)

    snc.sql(SPVAQueries.imaging_studies_table)

    snc.sql(SPVAQueries.immunizations_table)

    snc.sql(SPVAQueries.medications_table)

    snc.sql(SPVAQueries.observations_table)

    snc.sql(SPVAQueries.procedures_table)

    loadTables(snc)
  }

  def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table + " using row options(PARTITION_BY 'ID', buckets '12', " +
        " redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.encounters_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.allergies_table + " using row options(PARTITION_BY 'PATIENT'," +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.careplans_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.conditions_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.imaging_studies_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.immunizations_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.medications_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.observations_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.procedures_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")

    loadTables(snc)
  }

  def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table + " using column options(PARTITION_BY 'ID', buckets '12', " +
        " redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.encounters_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.allergies_table + " using column options(PARTITION_BY 'PATIENT'," +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.careplans_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.conditions_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.imaging_studies_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.immunizations_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.medications_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.observations_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.procedures_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    loadTables(snc)
  }

  def loadTables(snc: SnappyContext): Unit = {
    SPVAQueries.patients(snc).write.insertInto("patients")
    SPVAQueries.encounters(snc).write.insertInto("encounters")
    SPVAQueries.allergies(snc).write.insertInto("allergies")
    SPVAQueries.careplans(snc).write.insertInto("careplans")
    SPVAQueries.conditions(snc).write.insertInto("conditions")
    SPVAQueries.imaging_studies(snc).write.insertInto("imaging_studies")
    SPVAQueries.immunizations(snc).write.insertInto("immunizations")
    SPVAQueries.medications(snc).write.insertInto("medications")
    SPVAQueries.observations(snc).write.insertInto("observations")
    SPVAQueries.procedures(snc).write.insertInto("procedures")
  }

  def validateQueriesFullResultSet(snc: SnappyContext, tableType: String, pw: PrintWriter,
                                   sqlContext: SQLContext): String = {
    var failedQueries = ""
    SnappyTestUtils.tableType = tableType
    for (q <- SPVAQueries.queries) {
      var queryExecuted = true;
      var validationFailed = false;
      if (SnappyTestUtils.validateFullResultSet) {
        // scalastyle:off println
        pw.println(s"createAndLoadSparkTables started ...")
        val startTime = System.currentTimeMillis
        createAndLoadSparkTables(sqlContext)
        val finishTime = System.currentTimeMillis()
        pw.println(s"createAndLoadSparkTables completed successfully in : " + ((finishTime -
            startTime)/1000) + " seconds")
      }
      q._1 match {
        case "Q1_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_1,
          "Q1_1", pw, sqlContext)
        case "Q1_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_2, "Q1_2",
          pw, sqlContext)
        case "Q1_3" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_3, "Q1_3",
          pw, sqlContext)
        case "Q1_4" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_4, "Q1_4",
          pw, sqlContext)
        case "Q1_5" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_5, "Q1_5",
          pw, sqlContext)
        case "Q1_6" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_6, "Q1_6",
          pw, sqlContext)
        case "Q1_7" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_7, "Q1_7",
          pw, sqlContext)
        case "Q1_8" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_8, "Q1_8",
          pw, sqlContext)
        case "Q1_9" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q1_9, "Q1_9",
          pw, sqlContext)
        case "Q2_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_1, "Q2_1",
          pw, sqlContext)
        case "Q2_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_2, "Q2_2",
          pw, sqlContext)
        case "Q2_3" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_3, "Q2_3",
          pw, sqlContext)
        case "Q2_4" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_4, "Q2_4",
          pw, sqlContext)
        case "Q2_5" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_5, "Q2_5",
          pw, sqlContext)
        case "Q2_6" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_6, "Q2_6",
          pw, sqlContext)
        case "Q2_7" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_7, "Q2_7",
          pw, sqlContext)
        case "Q2_8" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_8, "Q2_8",
          pw, sqlContext)
        case "Q2_9" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_9, "Q2_9",
          pw, sqlContext)
        case "Q2_10" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_10,
          "Q2_10", pw, sqlContext)
        case "Q2_11" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_11,
          "Q2_11", pw, sqlContext)
        case "Q2_12" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_12,
          "Q2_12", pw, sqlContext)
        case "Q2_13" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q2_13,
          "Q2_13", pw, sqlContext)
        case "Q3_1_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_1_2,
          "Q3_1_2", pw, sqlContext)
        case "Q3_2_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_2_2,
          "Q3_2_2", pw, sqlContext)
        case "Q3_3" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_3,
          "Q3_3", pw, sqlContext)
        case "Q3_4_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_4_1,
          "Q3_4_1", pw, sqlContext)
        case "Q3_4_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_4_2,
          "Q3_4_2", pw, sqlContext)
        case "Q3_4_3" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_4_3,
          "Q3_4_3", pw, sqlContext)
        case "Q3_5" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_5, "Q3_5",
          pw, sqlContext)
        case "Q3_6" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_6, "Q3_6",
          pw, sqlContext)
        case "Q3_7_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_7_1,
          "Q3_7_1", pw, sqlContext)
        case "Q3_7_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_7_2,
          "Q3_7_2", pw, sqlContext)
        case "Q3_7_3" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_7_3,
          "Q3_7_3", pw, sqlContext)
        case "Q3_7_4" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_7_4,
          "Q3_7_4", pw, sqlContext)
        case "Q3_7_5" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_7_5,
          "Q3_7_5", pw, sqlContext)
        case "Q3_8" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_8, "Q3_8",
          pw, sqlContext)
        case "Q3_9" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_9, "Q3_9",
          pw, sqlContext)
        case "Q3_10" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_10,
          "Q3_10", pw, sqlContext)
        case "Q3_11" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_11,
          "Q3_11", pw, sqlContext)
        case "Q3_12" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_12,
          "Q3_12", pw, sqlContext)
        case "Q3_13" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_13,
          "Q3_13", pw, sqlContext)
        case "Q3_14" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q3_14,
          "Q3_14", pw, sqlContext)
        case "Q4_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q4_1, "Q4_1",
          pw, sqlContext)
        case "Q4_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q4_2, "Q4_2",
          pw, sqlContext)
        case "Q5_1_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q5_1_1,
          "Q5_1_1", pw, sqlContext)
        case "Q5_1_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q5_1_2,
          "Q5_1_2", pw, sqlContext)
        case "Q5_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q5_2, "Q5_2",
          pw, sqlContext)
        case "Q6_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q6_1, "Q6_1",
          pw, sqlContext)
        case "Q6_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q6_2, "Q6_2",
          pw, sqlContext)
        case "Q7_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q7_1, "Q7_1",
          pw, sqlContext)
        case "Q7_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q7_2, "Q7_2",
          pw, sqlContext)
        case "Q8_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q8_1, "Q8_1",
          pw, sqlContext)
        case "Q8_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q8_2, "Q8_2",
          pw, sqlContext)
        case "Q9_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q9_1, "Q9_1",
          pw, sqlContext)
        case "Q9_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q9_2, "Q9_2",
          pw, sqlContext)
        case "Q10" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q10, "Q10",
          pw, sqlContext)
        case "Q11_1" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q11_1,
          "Q11_1", pw, sqlContext)
        case "Q11_2" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q11_2,
          "Q11_2", pw, sqlContext)
        case "Q12" => validationFailed = SnappyTestUtils.assertQuery(snc, SPVAQueries.Q12, "Q12",
          pw, sqlContext)
        // scalastyle:off println
        case _ =>   // scalastyle:off println
          pw.println(s"Query ${q._1} will not  be executed.")
          queryExecuted = false
      }
      if (queryExecuted) {
        pw.println(s"Execution completed for query ${q._1}")
      }
      if (validationFailed) {
        failedQueries = SnappyTestUtils.addToFailedQueryList(failedQueries, q._1)
      }
    }
    return failedQueries;
  }

  def dropTables(snc: SnappyContext): Unit = {
    // scalastyle:off println
    snc.sql("drop table if exists patients")
    println("patients table dropped successfully.")
    snc.sql("drop table if exists encounters")
    println("encounters table dropped successfully.")
    snc.sql("drop table if exists allergies")
    println("allergies table dropped successfully.")
    snc.sql("drop table if exists careplans")
    println("careplans table dropped successfully.")
    snc.sql("drop table if exists conditions")
    println("conditions table dropped successfully.")
    snc.sql("drop table if exists imaging_studies")
    println("imaging_studies table dropped successfully.")
    snc.sql("drop table if exists immunizations")
    println("immunizations table dropped successfully.")
    snc.sql("drop table if exists medications")
    println("medications table dropped successfully.")
    snc.sql("drop table if exists observations")
    println("observations table dropped successfully.")
    snc.sql("drop table if exists procedures")
    println("procedures table dropped successfully.")
    // scalastyle:on println
  }

  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    // scalastyle:off println
   // recordDF.write().mode("overwrite").saveAsTable("schemaName.tableName");
    SPVAQueries.patients(sqlContext).registerTempTable("patients")
    println(s"regions Table created successfully in spark")
    SPVAQueries.encounters(sqlContext).registerTempTable("encounters")
    println(s"categories Table created successfully in spark")
    SPVAQueries.allergies(sqlContext).registerTempTable("allergies")
    println(s"shippers Table created successfully in spark")
    SPVAQueries.careplans(sqlContext).registerTempTable("careplans")
    println(s"employees Table created successfully in spark")
    SPVAQueries.conditions(sqlContext).registerTempTable("conditions")
    println(s"customers Table created successfully in spark")
    SPVAQueries.imaging_studies(sqlContext).registerTempTable("imaging_studies")
    println(s"orders Table created successfully in spark")
    SPVAQueries.immunizations(sqlContext).registerTempTable("immunizations")
    println(s"order_details Table created successfully in spark")
    SPVAQueries.medications(sqlContext).registerTempTable("medications")
    println(s"products Table created successfully in spark")
    SPVAQueries.observations(sqlContext).registerTempTable("observations")
    println(s"suppliers Table created successfully in spark")
    SPVAQueries.procedures(sqlContext).registerTempTable("procedures")
    println(s"territories Table created successfully in spark")
    // scalastyle:on println
  }

}
