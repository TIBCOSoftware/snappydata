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
package org.apache.spark.sql.streaming

import java.beans.{BeanInfo, Introspector}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, AttributeReference}
import org.apache.spark.sql.catalyst.{JavaTypeInference, CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

object StreamSqlHelper {

  def registerRelationDestroy(): Unit ={
     SnappyStoreHiveCatalog.registerRelationDestroy()
  }

  def clearStreams(): Unit ={
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

  /**
   * Converts an iterator of Java Beans to InternalRow using the provided
   * bean info & schema. This is not related to the singleton, but is a static
   * method for internal use.
   */
  private def beansToRows(data: Iterator[_], beanInfo: BeanInfo, attrs: Seq[AttributeReference]):
  Iterator[InternalRow] = {
    val extractors =
      beanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)
    val methodsToConverts = extractors.zip(attrs).map { case (e, attr) =>
      (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
    }
    data.map{ element =>
      new GenericInternalRow(
        methodsToConverts.map { case (e, convert) => convert(e.invoke(element)) }.toArray[Any]
      ): InternalRow
    }
  }


  def getSchemaDStream(ssc: SnappyStreamingContext, tableName: String): SchemaDStream = {
    val catalog = ssc.snappyContext.catalog
    catalog.lookupRelation(catalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(sr: StreamPlan, _) => new SchemaDStream(ssc,
        LogicalDStreamPlan(sr.schema.toAttributes, sr.rowStream)(ssc))
      case _ =>
        throw new AnalysisException(s"Table $tableName not a stream table")
    }
  }

  /**
   * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
   */
  def createSchemaDStream[A <: Product : TypeTag](ssc: SnappyStreamingContext, stream: DStream[A]): SchemaDStream = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd
    (rdd, schema.map(_.dataType)))
    val logicalPlan = LogicalDStreamPlan(schema.toAttributes, rowStream)(ssc)
    new SchemaDStream(ssc, logicalPlan)
  }

  def createSchemaDStream(ssc: SnappyStreamingContext, rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    val logicalPlan = LogicalDStreamPlan(schema.toAttributes,
      rowStream.map(converter(_).asInstanceOf[InternalRow]))(ssc)
    new SchemaDStream(ssc, logicalPlan)
  }


  def createSchemaDStream(ssc: SnappyStreamingContext, rowStream: JavaDStream[_], beanClass: Class[_]): SchemaDStream = {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val internalRowStream = rowStream.dstream.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(Utils.classForName(className))
      beansToRows(iter, localBeanInfo, attributeSeq)
    }

    val logicalPlan = LogicalDStreamPlan(attributeSeq,
      internalRowStream)(ssc)
    new SchemaDStream(ssc, logicalPlan)
  }
}


trait StreamPlan {
  def rowStream: DStream[InternalRow]

  def schema: StructType
}

trait StreamPlanProvider extends SchemaRelationProvider
