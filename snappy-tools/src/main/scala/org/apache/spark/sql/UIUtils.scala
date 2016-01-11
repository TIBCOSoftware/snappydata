package org.apache.spark.sql

import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}

object UIUtils {
   def withNewExecutionId[T](ctx:SnappyContext , queryExecution:QueryExecution) (body: => T): T = {
    SQLExecution.withNewExecutionId(ctx,queryExecution)(body)
  }
}
