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

package io.snappydata.examples;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * NOTE :
 * CURRENTLY THERE IS NO SUPPORT FOR QUERIES ON SAMPLING TABLE IN SPLIT MODE.(AQP-257)
 * NOT SUPPORTED FOR NOW.
 *
 * Creates and loads Airline data from parquet files in row and column
 * tables. Also samples the data and stores it in a column table.
 *
 *
 * Run this on your local machine:
 * <p/>
 * Start snappy cluster
 *
 * `$ sbin/snappy-start-all.sh`
 * <p/>
 * Start spark cluster
 *
 * `$ sbin/start-all.sh`
 * <p/>
 * `$./bin/spark-submit --class io.snappydata.examples.JavaAirlineDataJob \
 * --master spark://<hostname>:7077 --conf snappydata.connection=localhost:10334 \
 * $SNAPPY_HOME/examples/jars/quickstart.jar`
 *
 */

public class JavaAirlineDataJob {

  private static String airlinefilePath = "quickstart/data/airlineParquetData";
  private static String airlinereftablefilePath = "quickstart/data/airportcodeParquetData";
  private static final String colTable = "AIRLINE";
  private static final String rowTable = "AIRLINEREF";
  private static final String sampleTable = "AIRLINE_SAMPLE";
  private static final String stagingAirline = "STAGING_AIRLINE";

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    SnappyContext snc = SnappyContext.apply(jsc);

    // Drop tables if already exists
    snc.dropTable(colTable, true);
    snc.dropTable(rowTable, true);
    snc.dropTable(sampleTable, true);
    snc.dropTable(stagingAirline, true);

    System.out.println("****** CreateAndLoadAirlineDataJob ******");

    // Create a DF from the parquet data file and make it a table

    Map<String, String> options = new HashMap<>();
    options.put("path", airlinefilePath);
    Dataset<Row> airlineDF = snc.createExternalTable(stagingAirline, "parquet", options);

    StructType updatedSchema = replaceReservedWords(airlineDF.schema());

    // Create a table in snappy store
    options.clear();
    options.put("buckets", "16");
    snc.createTable(colTable, "column", updatedSchema, options, false);

    // Populate the table in snappy store
    airlineDF.write().mode(SaveMode.Append).saveAsTable(colTable);
    System.out.println("Created and imported data in " + colTable + " table.");

    // Create a DF from the airline ref data file
    Dataset<Row> airlinerefDF = snc.read().load(airlinereftablefilePath);

    // Create a table in snappy store
    snc.createTable(rowTable, "row",
        airlinerefDF.schema(), Collections.<String, String>emptyMap(), false);

    // Populate the table in snappy store
    airlinerefDF.write().mode(SaveMode.Append).saveAsTable(rowTable);

    System.out.println(String.format("Created and imported data in %s table", rowTable));

    // Create a sample table sampling parameters.
    options.clear();
    options.put("buckets", "8");
    options.put("qcs", "UniqueCarrier, Year_, Month_");
    options.put("fraction", "0.03");
    options.put("strataReservoirSize", "50");

    snc.createSampleTable(sampleTable, "Airline", options, false);


    // Initiate the sampling from base table to sample table.
    snc.table(colTable).write().insertInto(sampleTable);

    System.out.println(String.format("Created and imported data in %s table.", sampleTable));

    System.out.println("****** Job finished ******");
  }

  private static StructType replaceReservedWords(StructType airlineSchema) {
    StructField[] fields = airlineSchema.fields();
    StructField[] newFields = new StructField[fields.length];
    for (StructField s : fields) {
      StructField newField = null;
      if (s.name().equals("Year")) {
        newField = new StructField("Year_", s.dataType(), s.nullable(), s.metadata());
      } else if (s.name().equals("Month")) {
        newField = new StructField("Month_", s.dataType(), s.nullable(), s.metadata());
      } else {
        newField = s;
      }
      newFields[airlineSchema.indexOf(s)] = newField;
    }
    return new StructType(newFields);
  }
}
