package org.apache.spark.sql

import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}

object SnappyUIUtils {
  def withNewExecutionId[T](ctx: SQLContext, queryExecution: QueryExecution)(body: => T): T = {
    SQLExecution.withNewExecutionId(ctx, queryExecution)(body)
  }
}
