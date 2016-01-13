package org.apache.spark.sql.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

final class DefaultSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {
    new SchemaDStreamRelation(sqlContext, options, schema)
  }
}

case class SchemaDStreamRelation(@transient val sqlContext: SQLContext,
    options: Map[String, String],
    override val schema: StructType)
    extends BaseRelation with TableScan {

  override def buildScan(): RDD[Row] = {
    /*    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    if (rowStream.generatedRDDs.isEmpty) {
      new EmptyRDD[Row](sqlContext.sparkContext)
    } else {
      rowStream.generatedRDDs.maxBy(_._1)._2.map(converter(_)).asInstanceOf[RDD[Row]]
    } */
    null
  }
}
