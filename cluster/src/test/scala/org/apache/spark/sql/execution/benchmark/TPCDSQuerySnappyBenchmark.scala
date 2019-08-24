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

package org.apache.spark.sql.execution.benchmark

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Benchmark
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object TPCDSQuerySnappyBenchmark {

  var spark: SparkSession = _
  var snappy: SnappySession = _
  var ds: DataFrame = _

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  var snappyRS: FileOutputStream = new FileOutputStream(
    new File(s"Snappy_Results.out"))
  var sparkRS : FileOutputStream = new FileOutputStream(
    new File(s"Spark_Results.out"))

  var snappyPS: PrintStream = new PrintStream(snappyRS)
  var sparkPS: PrintStream = new PrintStream(sparkRS)

  def setupTables(dataLocation: String, isSnappy: Boolean): Map[String, Long] = {
    val props = Map("BUCKETS" -> "7")

    tables.map { tableName =>
      if (isSnappy) {

        val df = snappy.read.parquet(s"$dataLocation/$tableName")
        snappy.createTable(tableName, "column",
          new StructType(df.schema.map(_.copy(nullable = false)).toArray), props)
        df.write.insertInto(tableName)

        // scalastyle:off println
        println("Table Created..."+ tableName)
        tableName -> snappy.table(tableName).count()
      }
      else {
        spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
        spark.sqlContext.cacheTable(tableName)
        tableName -> spark.table(tableName).count()
      }
    }.toMap
  }

  def execute(dataLocation: String, queries: Seq[String], isSnappy: Boolean = false,
              queryPath: String = ""): Unit = {

    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation, isSnappy)

    queries.foreach { name =>

      val path: String = s"$queryPath/$name.sql"
      val queryString = fileToString(new File(path))

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan. Note that this
      // currently doesn't take WITH subqueries into account which might lead to fairly inaccurate
      // per-row processing time for those cases.
      try {
        val queryRelations = scala.collection.mutable.HashSet[String]()

        if (isSnappy) {
          ds = snappy.sqlContext.sql(queryString)
          //println("Plan..."+ ds.queryExecution.executedPlan)
        }
        else
          ds = spark.sql(queryString)

        ds.queryExecution.logical.map {
          case ur@UnresolvedRelation(t: TableIdentifier, _) =>
            queryRelations.add(t.table)
          case lp: LogicalPlan =>
            lp.expressions.foreach {
              _ foreach {
                case subquery: SubqueryExpression =>
                  subquery.plan.foreach {
                    case ur@UnresolvedRelation(t: TableIdentifier, _) =>
                      queryRelations.add(t.table)
                    case _ =>
                  }
                case _ =>
              }
            }
          case _ =>
        }

        val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
        val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 5)

        benchmark.addCase(name) { i =>

            if (isSnappy) {
              val rs = snappy.sqlContext.sql(queryString).collect()
             // snappyPS = new PrintStream(new FileOutputStream(new File(s"Snappy_$name.out")))
             // normalizeRows(rs, snappyPS)
            }
            else {
            val rs = spark.sql(queryString).collect()
            //sparkPS = new PrintStream(new FileOutputStream(new File(s"Spark_$name.out")))
            //normalizeRows(rs, sparkPS)
            }
        }
        benchmark.run()

      } catch {
        case e: Exception => println(s"Failed $name  " + e.printStackTrace())
      }
    }
  }

  private def normalizeRows(resultSet: Array[Row], printStream: PrintStream): Unit = {
    for (row <- resultSet) {
      printStream.println(row.toSeq.map {
        // case d: Double => "%18.4f".format(d).trim()
        case v => v
      }.mkString("|"))
    }
  }
}
