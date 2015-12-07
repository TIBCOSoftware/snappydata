package org.apache.spark.sql.streaming

import org.apache.spark.sql.sources.{BaseRelation, DeletableRelation, DestroyRelation}

/**
 * Created by ymahajan on 7/12/15.
 */
abstract class StreamBaseRelation extends BaseRelation with StreamPlan with DeletableRelation
with DestroyRelation with Serializable {
  override def destroy(ifExists: Boolean): Unit = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  override def delete(filterExpr: String): Int = {
    throw new IllegalAccessException("Stream tables cannot be dropped")
  }

  def truncate(): Unit = {
    throw new IllegalAccessException("Stream tables cannot be truncated")
  }
}