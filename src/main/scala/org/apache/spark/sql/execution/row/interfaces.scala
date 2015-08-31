package org.apache.spark.sql.execution.row

import java.sql.Connection
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.{Row, SQLContext}

@DeveloperApi
trait RowInsertableRelation {
  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
   *
   * @return number of rows inserted
   */
  def insert(rows: Seq[Row]): Int
}

@DeveloperApi
trait UpdatableRelation {
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
}

@DeveloperApi
trait DeletableRelation {
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
  def destroy(ifExists: Boolean): Unit
}

/**
 * Some extensions to `JdbcDialect` used by Snappy implementation.
 */
abstract class JdbcExtendedDialect extends JdbcDialect {

  /** Query string to check for existence of a table */
  def tableExists(tableName: String, conn: Connection,
      context: SQLContext): Boolean

  /** DDL to truncate a table, or null/empty if truncate is not supported */
  def truncateTable(tableName: String): String = s"TRUNCATE TABLE $tableName"

  def dropTable(tableName: String, conn: Connection, context: SQLContext,
      ifExists: Boolean): Unit

  def initializeTable(tableName: String, conn: Connection): Unit = {}

  def extraCreateTableProperties(isLoner: Boolean): Properties = new Properties()
}

object JdbcExtendedUtils {

  val DBTABLE_PROPERTY = "dbtable"
  val SCHEMA_PROPERTY = "tableschema"
  val ALLOW_EXISTING_PROPERTY = "allowexisting"

  def executeUpdate(sql: String, conn: Connection): Unit = {
    val stmt = conn.createStatement()
    try {
      stmt.executeUpdate(sql)
    } finally {
      stmt.close()
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String, dialect: JdbcDialect,
      context: SQLContext): Boolean = {
    dialect match {
      case d: JdbcExtendedDialect => d.tableExists(table, conn, context)

      case _ =>
        try {
          val stmt = conn.createStatement()
          val rs = stmt.executeQuery(s"SELECT 1 FROM $table LIMIT 1")
          rs.next()
          rs.close()
          stmt.close()
          true
        } catch {
          case NonFatal(e) => false
        }
    }
  }

  def dropTable(conn: Connection, tableName: String, dialect: JdbcDialect,
      context: SQLContext, ifExists: Boolean): Unit = {
    dialect match {
      case d: JdbcExtendedDialect =>
        d.dropTable(tableName, conn, context, ifExists)
      case _ =>
        if (!ifExists || tableExists(conn, tableName, dialect, context)) {
          JdbcExtendedUtils.executeUpdate(s"DROP TABLE $tableName", conn)
        }
    }
  }
}
