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
package org.apache.spark.sql.streaming.jdbc

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class JdbcStreamProvider extends StreamSourceProvider with DataSourceRegister {
  @Override
  def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (parameters("dbtable"), JDBCSource(sqlContext, parameters).schema)
  }

  @Override
  def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    JDBCSource(sqlContext, parameters)
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "jdbcStream"
}

case class LSN(lsn: Any) extends Offset

case class JDBCSource(@transient sqlContext: SQLContext,
    parameters: Map[String, String]) extends Source {

  private var lastOffset: Option[LSN] = {
    Some(LSN("0x000005ad0001cda000cd"))
    //None
  }

  private val connProps = {
    val props = new Properties()
    parameters.foreach { case (k, v) =>
      props.setProperty(k, v)
    }
    props
  }

  private val partitioner = {
    parameters("partition.1")
  }

  private val dbtable = parameters("dbtable")

  private val offsetCol = parameters("offsetColumn")

  private val offsetToStrFunc = parameters("offsetToStrFunc")

  private val strToOffsetFunc = parameters("strToOffsetFunc")

  private val offsetIncFunc = parameters("offsetIncFunc")

  private val getNextOffset = parameters.getOrElse("getNextOffset", null)

  private def reader =
    parameters.foldLeft(sqlContext.sparkSession.read)({
      case (r, (k, v)) => r.option(k, v)
    })

  private def execute(query: String) = reader.jdbc(parameters("url"), query, new Properties())


  private def getNextOffsetQuery(l: LSN): String = {
    getNextOffset match {
      case null =>
        s"(select $offsetToStrFunc(min($offsetCol)) nxtOffset from $dbtable " +
            s" where $offsetCol > $offsetIncFunc($strToOffsetFunc('${l.lsn}'))) getNextOffset "
      case query => val replaceQ = query
            .replaceAll("\\$table", dbtable)
            .replaceAll("\\$currentOffset", l.lsn.toString)
        s"( $replaceQ ) getNextOffset"
    }
  }

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  override def getOffset: Option[Offset] = {
    val nextBatch = execute(
      lastOffset.fold(s"(select $offsetToStrFunc(min($offsetCol)) newOffset from $dbtable) " +
          s"getFirstOffset ")(getNextOffsetQuery))
    nextBatch.collect()(0).get(0) match {
      case null =>
      case max =>
        lastOffset = Some(LSN(max))
    }

    lastOffset
  }

  @Override
  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    def lsn(o: Offset) = o.asInstanceOf[LSN].lsn

    start match {
      case None =>
        execute(s"(select * from $dbtable " +
            s" where $offsetCol <= $strToOffsetFunc('${lsn(end)}') ) getBatch ")
      case Some(begin) =>
        execute(s"(select * from $dbtable " +
            s" where $offsetCol > $strToOffsetFunc('${lsn(begin)}') " +
            s" and $offsetCol <= $strToOffsetFunc('${lsn(end)}') ) getBatch ")
    }
  }

  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    execute(dbtable).schema
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {}
}

