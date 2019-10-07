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

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext}

object SnappyDMLTestUtil {

  var snc: SnappyContext = _

  def regions(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/regions.csv")

  def categories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/categories.csv")

  def shippers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/shippers.csv")

  def employees(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/employees.csv")

  def customers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/customers.csv")

  def orders(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/orders.csv")

  def order_details(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("maxCharsPerColumn", "4096")
        .csv(s"${snc.getConf("dataFilesLocation")}/order_details.csv")

  def products(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/products.csv")

  def suppliers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/suppliers.csv")

  def territories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/territories.csv")

  def employee_territories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("maxCharsPerColumn", "4096")
        .option("nullValue", "")
        .csv(s"${snc.getConf("dataFilesLocation")}/employee_territories.csv")


  def regions_par(sqlContext: SQLContext): DataFrame =
      sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/regions")

  def categories_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/categories")

  def shippers_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/shippers")

  def employees_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/employees")

  def customers_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/customers")

  def orders_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/orders")

  def order_details_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/order_details")

  def products_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/products")

  def suppliers_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/suppliers")

  def territories_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/territories")

  def employee_territories_par(sqlContext: SQLContext): DataFrame =
    sqlContext.read.load(s"${snc.getConf("parquetFileLocation")}/employee_territories")

  val tableList = Map(
    0 -> "orders",
    1 -> "order_details",
    2 -> "regions",
    3 -> "categories",
    4 -> "shippers",
    5 -> "employees",
    6 -> "customers",
    7 -> "products",
    8 -> "suppliers",
    9 -> "territories",
    10 -> "employee_territories")

  def checkDir(parquetFileLocation: String, tableName: String): Boolean = {
    val tableDir : File = new File(s"${parquetFileLocation}/${tableName}")
    if (!tableDir.exists()) {
      return true;
    }
    return false;
  }

  def createParquetData(snc: SnappyContext, parquetFileLocation: String, pw: PrintWriter): Any = {
    for (q <- tableList) {
      val tableName = tableList.get(q._1).get
      // scalastyle:off println
      pw.println(s"Creating parquet data for table ${tableName}")
      if (checkDir(parquetFileLocation, tableName)) {
        q._1 match {
          case 0 =>
            orders(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 1 =>
            order_details(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 2 =>
            regions(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 3 =>
            categories(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 4 =>
            shippers(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 5 =>
            employees(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 6 =>
            customers(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 7 =>
            products(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 8 =>
            suppliers(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 9 =>
            territories(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
          case 10 =>
            employee_territories(snc).write.parquet(s"${parquetFileLocation}/${tableName}")
        }
        pw.println(s"Created parquet data for table ${tableName}")
      }
      else {
        pw.println(s"Parquet data already exists for ${tableName}")
      }
    }
  }

}
