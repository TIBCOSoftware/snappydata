/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.sources

import java.sql.Connection

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.LocalRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.{RelationInfo, SnappyExternalCatalog}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortDirection}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aqp.SampleInsertExec
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.internal.SnappySessionCatalog
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.types.{StructField, StructType}

@DeveloperApi
trait RowInsertableRelation extends SingleRowInsertableRelation {

  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
   *
   * @return number of rows inserted
   */
  def insert(rows: Seq[Row]): Int
}

trait PlanInsertableRelation extends DestroyRelation with InsertableRelation {

  /**
   * Get a spark plan for insert. The result of SparkPlan execution should
   * be a count of number of inserted rows.
   */
  def getInsertPlan(relation: LogicalRelation, child: SparkPlan): SparkPlan = {
    val baseTableInsert = getBasicInsertPlan(relation, child);
    val catalog = child.sqlContext.sessionState.catalog.asInstanceOf[SnappySessionCatalog]

    val sampleRelations = catalog.getSampleRelations(TableIdentifier(resolvedName))
    if (sampleRelations.isEmpty) {
      baseTableInsert
    } else {
      SampleInsertExec (baseTableInsert, child, resolvedName, relation.schema)
    }
  }

  def getBasicInsertPlan(relation: LogicalRelation, child: SparkPlan): SparkPlan
  def resolvedName: String
}

trait RowPutRelation extends DestroyRelation {

  /**
   * If the row is already present, it gets updated otherwise it gets
   * inserted into the table represented by this relation
   *
   * @param rows the rows to be upserted
   *
   * @return number of rows upserted
   */
  def put(rows: Seq[Row]): Int

  /**
   * Get a spark plan for puts. If the row is already present, it gets updated
   * otherwise it gets inserted into the table represented by this relation.
   * The result of SparkPlan execution should be a count of number of rows put.
   */
  def getPutPlan(relation: LogicalRelation, child: SparkPlan): SparkPlan
}

trait BulkPutRelation extends DestroyRelation {

  def table: String

  def getPutKeys(session: SnappySession): Option[Seq[String]]

  /**
    * Get a spark plan for puts. If the row is already present, it gets updated
    * otherwise it gets inserted into the table represented by this relation.
    * The result of SparkPlan execution should be a count of number of rows put.
    */
  def getPutPlan(insertPlan: SparkPlan, updatePlan: SparkPlan): SparkPlan
}

@DeveloperApi
trait SingleRowInsertableRelation {
  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  def executeUpdate(sql: String, defaultSchema: String): Int
}

/**
 * ::DeveloperApi
 *
 * API for updates and deletes to a relation.
 */
@DeveloperApi
trait MutableRelation extends DestroyRelation {

  /** Name of this mutable table as stored in catalog. */
  def table: String

  /**
   * Get the "key" columns for the table that need to be projected out by
   * UPDATE and DELETE operations for affecting the selected rows.
   */
  def getKeyColumns: Seq[String]

  /**
    * Get the "primary key" of the row table and "key columns" of the  column table
  */
  def getPrimaryKeyColumns(session: SnappySession): Seq[String]

  /** Get the partitioning columns for the table, if any. */
  def partitionColumns: Seq[String]

  /**
   * If required inject the key columns in the original relation.
   */
  def withKeyColumns(relation: LogicalRelation,
      keyColumns: Seq[String]): LogicalRelation = relation

  /**
   * Get a spark plan to update rows in the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  def getUpdatePlan(relation: LogicalRelation, child: SparkPlan,
      updateColumns: Seq[Attribute], updateExpressions: Seq[Expression],
      keyColumns: Seq[Attribute]): SparkPlan

  /**
   * Get a spark plan to delete rows the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  def getDeletePlan(relation: LogicalRelation, child: SparkPlan,
      keyColumns: Seq[Attribute]): SparkPlan
}

/**
 * ::DeveloperApi
 *
 * An extension to <code>InsertableRelation</code> that allows for data to be
 * inserted (possibily having different schema) into the target relation after
 * comparing against the result of <code>insertSchema</code>.
 */
@DeveloperApi
trait SchemaInsertableRelation extends InsertableRelation {

  /**
   * Return the actual relation to be used for insertion into the relation
   * or None if <code>sourceSchema</code> cannot be inserted.
   */
  def insertableRelation(
      sourceSchema: Seq[Attribute]): Option[InsertableRelation]

  /**
   * Append a given RDD or rows into the relation.
   */
  def append(rows: RDD[Row], time: Long = -1): Unit
}

@DeveloperApi
trait SamplingRelation extends BaseRelation with SchemaInsertableRelation {

  /**
   * Options set for this sampling relation.
   */
  def samplingOptions: Map[String, Any]

  /**
   * The QCS columns for the sample.
   */
  def qcs: Array[String]

  /** Base table of this relation. */
  def baseTable: Option[String]

  /**
   * The underlying column table used to store data.
   */
  def baseRelation: BaseColumnFormatRelation

  /**
    * If underlying sample table is partitioned
    * @return
    */
  def isPartitioned: Boolean

  /**
   * True if underlying sample table is using a row table as reservoir store.
   */
  def isReservoirAsRegion: Boolean

  def canBeOnBuildSide: Boolean
}

@DeveloperApi
trait UpdatableRelation extends MutableRelation with SingleRowInsertableRelation {

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
trait DeletableRelation extends MutableRelation {

  /**
   * Delete a set of row matching given criteria.
   *
   * @param filterExpr SQL WHERE criteria to select rows that will be deleted
   *
   * @return number of rows deleted
   */
  def delete(filterExpr: String): Int
}

@DeveloperApi
trait DestroyRelation extends BaseRelation {

  /**
   * Truncate the table represented by this relation.
   */
  def truncate(): Unit

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  def destroy(ifExists: Boolean): Unit
}

@DeveloperApi
trait IndexableRelation {
  /**
    * Create an index on a table.
    * @param indexIdent Index Identifier which goes in the catalog
    * @param tableIdent Table identifier on which the index is created.
    * @param indexColumns Columns on which the index has to be created with the
    *                     direction of sorting. Direction can be specified as None.
    * @param options Options for indexes. For e.g.
    *                column table index - ("COLOCATE_WITH"->"CUSTOMER").
    *                row table index - ("INDEX_TYPE"->"GLOBAL HASH") or
    *                ("INDEX_TYPE"->"UNIQUE")
    */
  def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit

  /**
    * Drops an index on this table
    * @param indexIdent Index identifier
    * @param tableIdent Table identifier
    * @param ifExists Drop if exists
    */
  def dropIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      ifExists: Boolean): Unit

}

@DeveloperApi
trait AlterableRelation {

  /**
   * Alter's table schema by adding or dropping a provided column.
   * The schema of this instance must reflect the updated one after alter.
   *
   * @param tableIdent  Table identifier
   * @param isAddColumn True if column is to be added else it is to be dropped
   * @param column      Column to be added or dropped
   * @param extensions  Any additional clauses accepted by underlying table storage
   *                    like DEFAULT value or column constraints
   */
  def alterTable(tableIdent: TableIdentifier,
      isAddColumn: Boolean, column: StructField, extensions: String): Unit
}

trait RowLevelSecurityRelation {
  def isRowLevelSecurityEnabled: Boolean
  def schemaName: String
  def tableName: String
  def resolvedName: String
  def enableOrDisableRowLevelSecurity(tableIdent: TableIdentifier,
      enableRowLevelSecurity: Boolean)
}

@DeveloperApi
trait NativeTableRowLevelSecurityRelation extends DestroyRelation with RowLevelSecurityRelation {

  protected val connFactory: () => Connection

  protected def dialect: JdbcDialect

  def connProperties: ConnectionProperties

  protected def isRowTable: Boolean

  val sqlContext: SQLContext
  val table: String

  override def enableOrDisableRowLevelSecurity(tableIdent: TableIdentifier,
      enableRowLevelSecurity: Boolean): Unit = {
    val conn = connFactory()
    try {
      val tableExists = JdbcExtendedUtils.tableExists(tableIdent.unquotedString,
        conn, dialect, sqlContext)
      val sql = if (enableRowLevelSecurity) {
        s"""alter table ${quotedName(table)} enable row level security"""
      } else {
        s"""alter table ${quotedName(table)} disable row level security"""
      }
      if (tableExists) {
        JdbcExtendedUtils.executeUpdate(sql, conn)
      } else {
        throw new AnalysisException(s"table $table does not exist.")
      }
    } catch {
      case se: java.sql.SQLException =>
        if (se.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${se.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw se
        }
    } finally {
      conn.commit()
      conn.close()
    }
  }

  def isRowLevelSecurityEnabled: Boolean = {
    val conn = connFactory()
    try {
      JdbcExtendedUtils.isRowLevelSecurityEnabled(resolvedName,
        conn, dialect, sqlContext)
    } catch {
      case se: java.sql.SQLException =>
        if (se.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${se.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw se
        }
    } finally {
      conn.commit()
      conn.close()
    }
  }

  protected[this] var _schema: StructType = _
  @transient protected[this] var _relationInfoAndRegion: (RelationInfo, Option[LocalRegion]) = _

  protected def refreshTableSchema(invalidateCached: Boolean, fetchFromStore: Boolean): Unit = {
    // Schema here must match the RelationInfo obtained from catalog.
    // If the schema has changed (in smart connector) then execution should throw an exception
    // leading to a retry or a CatalogStaleException to fail the operation.
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    if (invalidateCached) session.externalCatalog.invalidate(schemaName -> tableName)
    _relationInfoAndRegion = null
    if (fetchFromStore) {
      _schema = JdbcExtendedUtils.normalizeSchema(JDBCRDD.resolveTable(new JDBCOptions(
        connProperties.url, table, connProperties.connProps.asScala.toMap)))
    } else {
      session.externalCatalog.getTableOption(schemaName, tableName) match {
        case None => _schema = JdbcExtendedUtils.EMPTY_SCHEMA
        case Some(t) => _schema = t.schema; assert(relationInfoAndRegion ne null)
      }
    }
  }

  override def schema: StructType = _schema

  protected def relationInfoAndRegion: (RelationInfo, Option[LocalRegion]) = {
    if (((_relationInfoAndRegion eq null) || _relationInfoAndRegion._1.invalid) &&
        (sqlContext ne null)) {
      val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
      _relationInfoAndRegion = session.externalCatalog.getRelationInfo(
        schemaName, tableName, isRowTable)
    }
    _relationInfoAndRegion
  }

  protected def withoutUserSchema: Boolean

  def relationInfo: RelationInfo = relationInfoAndRegion._1

  @transient lazy val region: LocalRegion = {
    val relationInfoAndRegion = this.relationInfoAndRegion
    var lr = if (relationInfoAndRegion ne null) relationInfoAndRegion._2 match {
      case None => null
      case Some(r) => r
    } else null
    if (lr eq null) {
      lr = Misc.getRegionForTable(resolvedName, true).asInstanceOf[LocalRegion]
    }
    lr
  }

  protected def createActualTables(connection: Connection): Unit

  def createTable(mode: SaveMode): Unit = {
    var tableExists = true
    var conn: Connection = null
    try {
      // if no user-schema has been provided then expect the table to exist
      if (withoutUserSchema) {
        if (schema.isEmpty) {
          // refresh schema and try again
          refreshTableSchema(invalidateCached = true, fetchFromStore = false)
          if (schema.isEmpty) throw new TableNotFoundException(schemaName, tableName)
        }
      } else {
        conn = connFactory()
        tableExists = JdbcExtendedUtils.tableExists(resolvedName, conn, dialect, sqlContext)
      }
      if (tableExists) mode match {
        case SaveMode.ErrorIfExists => throw new AnalysisException(
          s"Table '$resolvedName' already exists. SaveMode: ErrorIfExists.")
        case SaveMode.Overwrite =>
          // truncate the table and return
          truncate()
        case _ =>
      } else createActualTables(conn)
    } catch {
      case se: java.sql.SQLException =>
        if (se.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${se.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and the driver jars " +
              "available (deploy jars in SnappyData cluster or --jars option in spark-submit).")
        } else throw se
    } finally {
      if ((conn ne null) && !conn.isClosed) {
        conn.commit()
        conn.close()
      }
    }
  }
}

/**
 * ::DeveloperApi::
 * Marker interface for data sources that allow for extended schema specification
 * in CREATE TABLE (like constraints in RDBMS databases). The schema string is passed
 * as [[SnappyExternalCatalog.SCHEMADDL_PROPERTY]] in the relation provider parameters.
 */
@DeveloperApi
trait ExternalSchemaRelationProvider extends RelationProvider {

  def getSchemaString(options: Map[String, String]): Option[String] =
    JdbcExtendedUtils.readSplitProperty(SnappyExternalCatalog.SCHEMADDL_PROPERTY, options)
}

/**
  * ::DeveloperApi::
  * A BaseRelation that can eliminate unneeded columns and filter using selected
  * predicates before producing an RDD containing all matching tuples as Unsafe Row objects.
  *
  * The actual filter should be the conjunction of all `filters`,
  * i.e. they should be "and" together.
  *
  * The pushed down filters are currently purely an optimization as they will all be evaluated
  * again.  This means it is safe to use them with methods that produce false positives such
  * as filtering partitions based on a bloom filter.
  *
  * @since 1.3.0
  */
@DeveloperApi
trait PrunedUnsafeFilteredScan {

  /**
   * Returns the list of [[Expression]]s that this datasource may not be able to handle.
   * By default, this function will return all filters, as it is always safe to
   * double evaluate an [[Expression]].
   */
  def unhandledFilters(filters: Seq[Expression]): Seq[Expression]

  def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]])
}
