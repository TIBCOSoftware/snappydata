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

package io.snappydata.hydra.spva


import java.io.PrintWriter

import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql.{SQLContext, SnappyContext}

object SPVATestUtil {

  def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table)
    SPVAQueries.patients(snc).write.insertInto("spd.patient")

    snc.sql(SPVAQueries.encounters_table)
    SPVAQueries.encounters(snc).write.insertInto("spd.encounters")

    snc.sql(SPVAQueries.allergies_table)
    SPVAQueries.allergies(snc).write.insertInto("spd.allergies")

    snc.sql(SPVAQueries.careplans_table)
    SPVAQueries.careplans(snc).write.insertInto("spd.careplans")

    snc.sql(SPVAQueries.conditions_table)
    SPVAQueries.conditions(snc).write.insertInto("spd.conditions")

    snc.sql(SPVAQueries.imaging_studies_table)
    SPVAQueries.imaging_studies(snc).write.insertInto("spd.imaging_studies")

    snc.sql(SPVAQueries.immunizations_table)
    SPVAQueries.immunizations(snc).write.insertInto("spd.immunizations")

    snc.sql(SPVAQueries.medications_table)
    SPVAQueries.medications(snc).write.insertInto("spd.medications")

    snc.sql(SPVAQueries.observations_table)
    SPVAQueries.observations(snc).write.insertInto("spd.observations")

    snc.sql(SPVAQueries.procedures_table)
    SPVAQueries.procedures(snc).write.insertInto("spd.procedures")
  }

  def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table + " using row options(PARTITION_BY 'ID', buckets '12', " +
        " redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.encounters_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.allergies_table + " using row options(PARTITION_BY 'PATIENT'," +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.careplans_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.conditions_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.imaging_studies_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.immunizations_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.medications_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.observations_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.procedures_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")

    SPVAQueries.patients(snc).write.insertInto("spd.patients")
    SPVAQueries.encounters(snc).write.insertInto("spd.encounters")
    SPVAQueries.allergies(snc).write.insertInto("spd.allergies")
    SPVAQueries.careplans(snc).write.insertInto("spd.careplans")
    SPVAQueries.conditions(snc).write.insertInto("spd.conditions")
    SPVAQueries.imaging_studies(snc).write.insertInto("spd.imaging_studies")
    SPVAQueries.immunizations(snc).write.insertInto("spd.immunizations")
    SPVAQueries.medications(snc).write.insertInto("spd.medications")
    SPVAQueries.observations(snc).write.insertInto("spd.observations")
    SPVAQueries.procedures(snc).write.insertInto("spd.procedures")
  }

  def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    snc.sql(SPVAQueries.patients_table + " using column options(PARTITION_BY 'ID', buckets '12', " +
        " redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.encounters_table + " using row options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.allergies_table + " using column options(PARTITION_BY 'PATIENT'," +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.careplans_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.conditions_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.imaging_studies_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync'," +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.immunizations_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.medications_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.observations_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")
    snc.sql(SPVAQueries.procedures_table + " using column options(PARTITION_BY 'PATIENT', " +
        " colocate_with 'SPD.PATIENTS', buckets '12', redundancy '1', PERSISTENT 'sync', " +
        " EVICTION_BY 'LRUHEAPPERCENT')")

    SPVAQueries.patients(snc).write.insertInto("spd.patients")
    SPVAQueries.encounters(snc).write.insertInto("spd.encounters")
    SPVAQueries.allergies(snc).write.insertInto("spd.allergies")
    SPVAQueries.careplans(snc).write.insertInto("spd.careplans")
    SPVAQueries.conditions(snc).write.insertInto("spd.conditions")
    SPVAQueries.imaging_studies(snc).write.insertInto("spd.imaging_studies")
    SPVAQueries.immunizations(snc).write.insertInto("spd.immunizations")
    SPVAQueries.medications(snc).write.insertInto("spd.medications")
    SPVAQueries.observations(snc).write.insertInto("spd.observations")
    SPVAQueries.procedures(snc).write.insertInto("spd.procedures")
  }

  def validateQueriesFullResultSet(snc: SnappyContext, tableType: String, pw: PrintWriter,
                                   sqlContext: SQLContext): Unit = {
    for (q <- SPVAQueries.queries) {
      q._1 match {
        case "Q1_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_1, "Q1_1", tableType,
          pw, sqlContext)
        case "Q1_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_2, "Q1_2", tableType,
          pw, sqlContext)
        case "Q1_3" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_3, "Q1_3", tableType,
          pw, sqlContext)
        case "Q1_4" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_4, "Q1_4", tableType,
          pw, sqlContext)
        case "Q1_5" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_5, "Q1_5", tableType,
          pw, sqlContext)
        case "Q1_6" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_6, "Q1_6", tableType,
          pw, sqlContext)
        case "Q1_7" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_7, "Q1_7", tableType,
          pw, sqlContext)
        case "Q1_8" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_8, "Q1_8", tableType,
          pw, sqlContext)
        case "Q1_9" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q1_9, "Q1_9", tableType,
          pw, sqlContext)
        case "Q2_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_1, "Q2_1", tableType,
          pw, sqlContext)
        case "Q2_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_2, "Q2_2", tableType,
          pw, sqlContext)
        case "Q2_3" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_3, "Q2_3", tableType,
          pw, sqlContext)
        case "Q2_4" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_4, "Q2_4", tableType,
          pw, sqlContext)
        case "Q2_5" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_5, "Q2_5", tableType,
          pw, sqlContext)
        case "Q2_6" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_6, "Q2_6", tableType,
          pw, sqlContext)
        case "Q2_7" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_7, "Q2_7", tableType,
          pw, sqlContext)
        case "Q2_8" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_8, "Q2_8", tableType,
          pw, sqlContext)
        case "Q2_9" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_9, "Q2_9", tableType,
          pw, sqlContext)
        case "Q2_10" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_10, "Q2_10", tableType,
          pw, sqlContext)
        case "Q2_11" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_11, "Q2_11", tableType,
          pw, sqlContext)
        case "Q2_12" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_12, "Q2_12", tableType,
          pw, sqlContext)
        case "Q2_13" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q2_13, "Q2_13", tableType,
          pw, sqlContext)
        case "Q3_1_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_1_2, "Q3_1_2", tableType,
          pw, sqlContext)
        case "Q3_2_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_2_2, "Q3_2_2", tableType,
          pw, sqlContext)
        case "Q3_3" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_3, "Q3_3", tableType,
          pw, sqlContext)
        case "Q3_4_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_4_1, "Q3_4_1", tableType,
          pw, sqlContext)
        case "Q3_4_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_4_2, "Q3_4_2", tableType,
          pw, sqlContext)
        case "Q3_4_3" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_4_3, "Q3_4_3", tableType,
          pw, sqlContext)
        case "Q3_5" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_5, "Q3_5", tableType,
          pw, sqlContext)
        case "Q3_6" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_6, "Q3_6", tableType,
          pw, sqlContext)
        case "Q3_7_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_7_1, "Q3_7_1", tableType,
          pw, sqlContext)
        case "Q3_7_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_7_2, "Q3_7_2", tableType,
          pw, sqlContext)
        case "Q3_7_3" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_7_3, "Q3_7_3", tableType,
          pw, sqlContext)
        case "Q3_7_4" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_7_4, "Q3_7_4", tableType,
          pw, sqlContext)
        case "Q3_7_5" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_7_5, "Q3_7_5", tableType,
          pw, sqlContext)
        case "Q3_8" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_8, "Q3_8", tableType,
          pw, sqlContext)
        case "Q3_9" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_9, "Q3_9", tableType,
          pw, sqlContext)
        case "Q3_10" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_10, "Q3_10", tableType,
          pw, sqlContext)
        case "Q3_11" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_11, "Q3_11", tableType,
          pw, sqlContext)
        case "Q3_12" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_12, "Q3_12", tableType,
          pw, sqlContext)
        case "Q3_13" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_13, "Q3_13", tableType,
          pw, sqlContext)
        case "Q3_14" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q3_14, "Q3_14", tableType,
          pw, sqlContext)
        case "Q4_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q4_1, "Q4_1", tableType,
          pw, sqlContext)
        case "Q4_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q4_2, "Q4_2", tableType,
          pw, sqlContext)
        case "Q5_1_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q5_1_1, "Q5_1_1", tableType,
          pw, sqlContext)
        case "Q5_1_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q5_1_2, "Q5_1_2", tableType,
          pw, sqlContext)
        case "Q5_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q5_2, "Q5_2", tableType,
          pw, sqlContext)
        case "Q6_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q6_1, "Q6_1", tableType,
          pw, sqlContext)
        case "Q6_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q6_2, "Q6_2", tableType,
          pw, sqlContext)
        case "Q7_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q7_1, "Q7_1", tableType,
          pw, sqlContext)
        case "Q7_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q7_2, "Q7_2", tableType,
          pw, sqlContext)
        case "Q8_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q8_1, "Q8_1", tableType,
          pw, sqlContext)
        case "Q8_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q8_2, "Q8_2", tableType,
          pw, sqlContext)
        case "Q9_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q9_1, "Q9_1", tableType,
          pw, sqlContext)
        case "Q9_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q9_2, "Q9_2", tableType,
          pw, sqlContext)
        case "Q10" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q10, "Q10",
          tableType, pw, sqlContext)
        case "Q11_1" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q11_1, "Q11_1",
          tableType, pw, sqlContext)
        case "Q11_2" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q11_2, "Q11_2", tableType,
          pw, sqlContext)
        case "Q12" => SnappyTestUtils.assertQueryFullResultSet(snc, SPVAQueries.Q12, "Q12",
          tableType, pw, sqlContext)
        // scalastyle:off println
        case _ => println("OK")
      }
    }
  }

  def dropTables(snc: SnappyContext): Unit = {
    // scalastyle:off println
    snc.sql("drop table if exists spd.patients")
    println("spd.patients table dropped successfully.");
    snc.sql("drop table if exists spd.encounters")
    println("spd.encounters table dropped successfully.");
    snc.sql("drop table if exists spd.allergies")
    println("spd.allergies table dropped successfully.");
    snc.sql("drop table if exists spd.careplans")
    println("spd.careplans table dropped successfully.");
    snc.sql("drop table if exists spd.conditions")
    println("spd.conditions table dropped successfully.");
    snc.sql("drop table if exists spd.imaging_studies")
    println("spd.imaging_studies table dropped successfully.");
    snc.sql("drop table if exists spd.immunizations")
    println("spd.immunizations table dropped successfully.");
    snc.sql("drop table if exists spd.medications")
    println("spd.medications table dropped successfully.");
    snc.sql("drop table if exists spd.observations")
    println("spd.observations table dropped successfully.");
    snc.sql("drop table if exists spd.procedures")
    println("spd.procedures table dropped successfully.");
    // scalastyle:on println
  }

  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    // scalastyle:off println
    SPVAQueries.patients(sqlContext).registerTempTable("spd.patients")
    println(s"regions Table created successfully in spark")
    SPVAQueries.encounters(sqlContext).registerTempTable("spd.encounters")
    println(s"categories Table created successfully in spark")
    SPVAQueries.allergies(sqlContext).registerTempTable("spd.allergies")
    println(s"shippers Table created successfully in spark")
    SPVAQueries.careplans(sqlContext).registerTempTable("spd.careplans")
    println(s"employees Table created successfully in spark")
    SPVAQueries.conditions(sqlContext).registerTempTable("spd.conditions")
    println(s"customers Table created successfully in spark")
    SPVAQueries.imaging_studies(sqlContext).registerTempTable("spd.imaging_studies")
    println(s"orders Table created successfully in spark")
    SPVAQueries.immunizations(sqlContext).registerTempTable("spd.immunizations")
    println(s"order_details Table created successfully in spark")
    SPVAQueries.medications(sqlContext).registerTempTable("spd.medications")
    println(s"products Table created successfully in spark")
    SPVAQueries.observations(sqlContext).registerTempTable("spd.observations")
    println(s"suppliers Table created successfully in spark")
    SPVAQueries.procedures(sqlContext).registerTempTable("pd.procedures")
    println(s"territories Table created successfully in spark")
    // scalastyle:on println
  }

}
