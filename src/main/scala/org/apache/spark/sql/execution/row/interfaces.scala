package org.apache.spark.sql.execution.row

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JdbcDialect

@DeveloperApi
trait UpdatableRelation {
  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
   *
   * @return number of rows inserted
   */
  def insert(rows: Seq[Row]): Int

  /**
   * Update a set of rows matching given criteria.
   *
   * @param filterExpr SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues updated values for the columns being changed;
   *                        must match `updateColumns`
   * @param updateColumns the columns to be updated; must match `updatedColumns`
   *
   * @return number of rows affected
   */
  def update(filterExpr: String, newColumnValues: Row,
      updateColumns: Seq[String]): Int

  /**
   * Delete a set of row matching given criteria.
   *
   * @param filterExpr SQL WHERE criteria to select rows that will be deleted
   *
   * @return number of rows deleted
   */
  def delete(filterExpr: String): Int

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  def destroy(): Unit
}

/**
 * Some extensions to `JdbcDialect` used by Snappy implementation.
 */
abstract class JdbcExtendedDialect extends JdbcDialect {
  /** DDL to truncate a table, or null/empty if truncate is not supported */
  def truncateTable(tableName: String): String = s"TRUNCATE TABLE $tableName"
}
