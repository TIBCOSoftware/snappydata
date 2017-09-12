/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

}
