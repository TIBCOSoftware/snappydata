package org.apache.spark.sql.execution.row

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JdbcDialect

@DeveloperApi
trait UpdatableRelation {
  def insert(row: Row): Int

  def update(updatedColumns: Row, setColumns: Seq[String],
      filterExpr: String): Int

  def delete(filterExpr: String): Int

  def destroy(): Unit
}

abstract class JdbcExtendedDialect extends JdbcDialect {
  def truncateTable(tableName: String): String = s"TRUNCATE TABLE $tableName"
}
