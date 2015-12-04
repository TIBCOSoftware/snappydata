package org.apache.spark.sql.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

/**
 * Created by ymahajan on 25/09/15.
 */

final class SocketStreamSource extends SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String], schema: StructType) = {
    new SocketStreamRelation(sqlContext, options, schema)
  }
}