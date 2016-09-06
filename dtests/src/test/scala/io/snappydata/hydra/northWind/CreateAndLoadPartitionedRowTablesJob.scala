/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.northWind

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northWind
import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

import scala.util.{Failure, Success, Try}

/**
 * Created by swati on 2/9/16.
 */
object CreateAndLoadPartitionedRowTablesJob extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadPartitionedRowTablesJob.out"), true));
    Try {
      snc.sql("set spark.sql.shuffle.partitions=6")
      northWind.NWQueries.snc = snc
      NWTestSparkApp.dropTables(snc)
      println("Create and load Partitioned Row tables Test started")
      NWTestSparkApp.createAndLoadPartitionedTables(snc)
      NWTestSparkApp.validateQueries(snc, "Partitioned Row Table", pw)
      println("Create and load Partitioned Row tables Test completed successfully")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/CreateAndLoadPartitionedRowTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}

