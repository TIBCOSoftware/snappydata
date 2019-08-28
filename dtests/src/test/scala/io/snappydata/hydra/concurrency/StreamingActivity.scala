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

import java.util.Properties
import java.io.FileInputStream

import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SnappyJobValid, SnappyJobValidation}
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{SnappyStreamingContext}

object StreamingActivity extends SnappyStreamingJob {
  override def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation =
    SnappyJobValid()

  def typeCast(col: String, field: String): Any = {
    val fType = field.split(sSep)(1)
    fType.trim.toLowerCase() match {
      case "integer" => {
        if (col == null || col.equals("")) {
          0
        } else {
          col.toInt
        }
      }
      case "string" => {
        if (col == null || col.equals("")) {
          ""
        } else {
          col
        }
      }
      case _ => col
    }
  }

  // seperator for field details
  val fSep = ";"
  // seperator for schema details
  val sSep = "#"

  override def runSnappyJob(sns: SnappyStreamingContext, jobConfig: Config): Any = {
    import sns.snappySession.implicits._
    val propertiesPath = jobConfig.getString("prop_path")
    val inPath = jobConfig.getString("input_path")
    val props: Properties = new Properties()
    props.load(new FileInputStream(propertiesPath))

    val tableName = props.getProperty("table_name")
    val enablePutInto = props.getProperty("enable_putinto").trim.toLowerCase().equals("true")
    val schemaStr = props.getProperty("schema_string")
    // val millisToRun = props.getProperty("run_millis").toInt

    val tsColIndx = 0
    // val endTime = System.currentTimeMillis() + millisToRun

    var tableSchema = new StructType()
    val fieldsInfo = schemaStr.split(fSep)
    fieldsInfo.foreach(fieldStr => {
      val colName = fieldStr.split(sSep)(0)
      val colType = fieldStr.split(sSep)(1)
      tableSchema = tableSchema.add(colName, colType)
    })

    val inp = sns.textFileStream(s"""file://$inPath""")
    inp.foreachRDD(rdd => {
      val rddNoSchema = rdd.map(line => {
        val cols = line.split(",")
        val ts = String.valueOf(System.nanoTime())
        val numOfFields = line.count(_ == ',') + 1
        val arr = new Array[Any](numOfFields)
        var i = 0
        cols.foreach(col => {
          if (i == tsColIndx) {
            arr(i) = col + ts
          } else {
            val f = fieldsInfo(i)
            arr(i) = typeCast(col, f)
          }
          i += 1
        })
        Row.fromSeq(arr.toSeq)
      })

      val df = sns.snappySession.createDataFrame(rddNoSchema, tableSchema)
      if (enablePutInto) {
        df.cache()
        // inserts
        df.write.insertInto(tableName)
        import org.apache.spark.sql.snappy._
        df.write.putInto(tableName)
        df.unpersist()
      } else {
        // inserts
        df.write.insertInto(tableName)
      }
    })

    sns.start()

//    while (System.currentTimeMillis() < endTime) {
//      Thread.sleep(2000)
//    }
//    sns.stop(true, true)
     sns.awaitTermination()
  }
}
