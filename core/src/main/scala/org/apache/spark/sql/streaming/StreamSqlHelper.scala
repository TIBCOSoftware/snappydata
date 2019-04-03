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
package org.apache.spark.sql.streaming

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.{InternalRow, JavaTypeInference}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SparkSupport}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

object StreamSqlHelper extends SparkSupport {

  def clearStreams(): Unit = {
    StreamBaseRelation.clearStreams()
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  def getSchemaDStream(ssc: SnappyStreamingContext, tableName: String): SchemaDStream = {
    val catalog = ssc.snappySession.sessionState.catalog
    catalog.resolveRelation(ssc.snappySession.tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[StreamPlan] =>
        val sr = lr.relation.asInstanceOf[StreamPlan]
        new SchemaDStream(ssc, internals.newLogicalDStreamPlan(
          sr.schema.toAttributes, sr.rowStream, ssc))
      case _ =>
        throw new AnalysisException(s"Table $tableName not a stream table")
    }
  }

  /**
   * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
   */
  def createSchemaDStream[A <: Product : TypeTag](ssc: SnappyStreamingContext,
      stream: DStream[A]): SchemaDStream = {
    val encoder = ExpressionEncoder[A]()
    val schema = encoder.schema
    val logicalPlan = internals.newLogicalDStreamPlan(schema.toAttributes,
      stream.map(encoder.toRow(_).copy()), ssc)
    new SchemaDStream(ssc, logicalPlan)
  }

  def createSchemaDStream(ssc: SnappyStreamingContext, rowStream: DStream[Row],
      schema: StructType): SchemaDStream = {
    val encoder = RowEncoder(schema)
    val logicalPlan = internals.newLogicalDStreamPlan(schema.toAttributes,
      rowStream.map(encoder.toRow(_).copy()), ssc)
    new SchemaDStream(ssc, logicalPlan)
  }

  def createSchemaDStream(ssc: SnappyStreamingContext,
      rowStream: JavaDStream[_], beanClass: Class[_]): SchemaDStream = {
    val encoder = ExpressionEncoder.javaBean(beanClass.asInstanceOf[Class[Any]])
    val schema = encoder.schema
    val logicalPlan = internals.newLogicalDStreamPlan(schema.toAttributes,
      rowStream.dstream.map(encoder.toRow(_).copy()), ssc)
    new SchemaDStream(ssc, logicalPlan)
  }
}


trait StreamPlan {
  def rowStream: DStream[InternalRow]

  def schema: StructType
}

trait StreamPlanProvider extends SchemaRelationProvider
