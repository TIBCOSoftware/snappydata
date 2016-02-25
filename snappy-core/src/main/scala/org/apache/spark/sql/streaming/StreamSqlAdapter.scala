package org.apache.spark.sql.streaming

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by rishim on 23/2/16.
 */
object StreamSqlAdapter {

  def registerRelationDestroy(): Unit ={
     SnappyStoreHiveCatalog.registerRelationDestroy()
  }

  def clearStreams(): Unit ={
    StreamBaseRelation.clearStreams()
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

}


trait StreamPlan {
  def rowStream: DStream[InternalRow]

  def schema: StructType
}

trait StreamPlanProvider extends SchemaRelationProvider
