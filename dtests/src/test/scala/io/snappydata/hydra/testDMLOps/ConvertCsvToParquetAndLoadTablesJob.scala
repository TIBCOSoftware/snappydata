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

package io.snappydata.hydra.testDMLOps

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class ConvertCsvToParquetAndLoadTablesJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream
      (new File("LoadNWTablesUsingParquetDataJob.out"), true));
    val currDir = new java.io.File(".").getCanonicalPath
    Try {
      val snc = snSession.sqlContext
      SnappyDMLTestUtil.snc = snc
      snc.sql("set spark.sql.shuffle.partitions=23")
      // scalastyle:off println
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      pw.println(s"dataFilesLocation is : ${dataFilesLocation}")
      snc.setConf("dataFilesLocation", dataFilesLocation)

      val parquetFileLocation = currDir + File.separator + ".." + File.separator + ".." +
          File.separator + "parquetFiles"
      pw.println(s"Parquet file location is : ${parquetFileLocation}")
      val parquetFileDir : File = new File(parquetFileLocation)
      if(!parquetFileDir.exists()) {
        parquetFileDir.mkdir()
      }

      SnappyDMLTestUtil.createParquetData(snc, parquetFileLocation, pw)
      snc.setConf("parquetFileLocation", parquetFileLocation)
      snc.sql("set schema gemfire1")
      pw.println("Now loading data to the snappy tables...")
      pw.println("Loading data into orders...");  pw.flush()
      SnappyDMLTestUtil.orders_par(snc).write.insertInto("orders")
      pw.println("Loading data into order_details...");  pw.flush()
      SnappyDMLTestUtil.order_details_par(snc).write.insertInto("order_details")
      pw.println("Loading data into orders...");  pw.flush()
      SnappyDMLTestUtil.regions_par(snc).write.insertInto("regions")
      pw.println("Loading data into categories...");  pw.flush()
      SnappyDMLTestUtil.categories_par(snc).write.insertInto("categories")
      pw.println("Loading data into shippers...");  pw.flush()
      SnappyDMLTestUtil.shippers_par(snc).write.insertInto("shippers")
      pw.println("Loading data into employees...");  pw.flush()
      SnappyDMLTestUtil.employees_par(snc).write.insertInto("employees")
      pw.println("Loading data into customers...");  pw.flush()
      SnappyDMLTestUtil.customers_par(snc).write.insertInto("customers")
      pw.println("Loading data into products...");  pw.flush()
      SnappyDMLTestUtil.products_par(snc).write.insertInto("products")
      pw.println("Loading data into suppliers...");  pw.flush()
      SnappyDMLTestUtil.suppliers_par(snc).write.insertInto("suppliers")
      pw.println("Loading data into territories...");  pw.flush()
      SnappyDMLTestUtil.territories_par(snc).write.insertInto("territories")
      pw.println("Loading data into employee_territories...");  pw.flush()
      SnappyDMLTestUtil.employee_territories_par(snc).write.insertInto("employee_territories")
      pw.println("Done loading data into snappy tables.")
    }
    match {
      case Success(v) => pw.close()
        s"See ${currDir}/LoadNWTablesUsingParquetDataJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

