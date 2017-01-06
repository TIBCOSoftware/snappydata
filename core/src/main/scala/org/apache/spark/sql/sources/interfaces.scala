/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortDirection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

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

trait RowPutRelation extends SingleRowInsertableRelation {
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
   * If the row is already present, it gets updated otherwise it gets
   * inserted into the table represented by this relation
   *
   * @param df the <code>DataFrame</code> to be upserted
   *
   */
  def put(df: DataFrame): Unit
}

@DeveloperApi
trait SingleRowInsertableRelation {
  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  def executeUpdate(sql: String): Int
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

/**
 * A relation having a parent-child relationship with a base relation.
 */
@DeveloperApi
trait DependentRelation extends BaseRelation {

  /** Base table of this relation. */
  def baseTable: Option[String]

  /** Name of this relation in the catalog. */
  def name: String
}

/**
 * A relation having a parent-child relationship with one or more
 * <code>DependentRelation</code>s as children.
 */
@DeveloperApi
trait ParentRelation extends BaseRelation {

  /** Used by <code>DependentRelation</code>s to register with parent */
  def addDependent(dependent: DependentRelation,
      catalog: SnappyStoreHiveCatalog): Boolean

  /** Used by <code>DependentRelation</code>s to unregister with parent */
  def removeDependent(dependent: DependentRelation,
      catalog: SnappyStoreHiveCatalog): Boolean

  /** Get the dependent child. */
  def getDependents(catalog: SnappyStoreHiveCatalog): Seq[String]

  /**
   * Recover/Re-create the dependent child relations. This callback
   * is to recreate Dependent relations when the ParentRelation is
   * being created.
   */
  def recoverDependentRelations(properties: Map[String, String]): Unit
}

@DeveloperApi
trait SamplingRelation extends DependentRelation with SchemaInsertableRelation {

  /**
   * Options set for this sampling relation.
   */
  def samplingOptions: Map[String, Any]

  /**
   * The QCS columns for the sample.
   */
  def qcs: Array[String]

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
}

@DeveloperApi
trait UpdatableRelation extends SingleRowInsertableRelation {

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
}

@DeveloperApi
trait DestroyRelation {

  /**
   * Return true if table already existed when the relation object was created.
   */
  def tableExists: Boolean

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
  def createIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit

  /**
    * Drops an index on this table
    * @param indexIdent Index identifier
    * @param tableIdent Table identifier
    * @param ifExists Drop if exists
    */
  def dropIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit

  /**
   * Return an [[IndexScan]] if there is an index that can be used
   * for repeated lookups on given key columns.
   */
  def getIndexScan(indexColumns: Seq[String]): Option[IndexScan] = None
}

/** A trait to perform efficient index lookups for a set of indexed keys. */
@DeveloperApi
trait IndexScan extends Serializable {

  /** Returns true if every key has a unique mapping. */
  def keyIsUnique: Boolean

  /** Initialize the scan for given base table columns (0 based). */
  def initialize(projectionColumns: Seq[String]): Unit

  /**
   * Returns the set of projected rows mapped to given key columns
   * and null for no match.
   */
  def get(key: InternalRow): Iterator[InternalRow]

  /**
   * Returns the matched single row when [[keyIsUnique]] is true
   * or null if no match.
   */
  def getValue(key: InternalRow): InternalRow

  /** Close the scan. */
  def close(): Unit
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data
 * source with a given schema.  When Spark SQL is given a DDL operation with
 * a USING clause specified (to specify the implemented SchemaRelationProvider)
 * and a user defined schema, this interface is used to pass in the parameters
 * specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.
 * When that class is not found Spark SQL will append the class name
 * `DefaultSource` to the path, allowing for less verbose invocation.
 * For example, 'org.apache.spark.sql.json' would resolve to the data source
 * 'org.apache.spark.sql.json.DefaultSource'.
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[SchemaRelationProvider]] and an
 * [[ExternalSchemaRelationProvider]] is that latter accepts schema and other
 * clauses in DDL string and passes over to the backend as is, while the schema
 * specified for former is parsed by Spark SQL.
 * A relation provider can inherit both [[SchemaRelationProvider]] and
 * [[ExternalSchemaRelationProvider]] if it can support both Spark SQL schema
 * and backend-specific schema.
 */
@DeveloperApi
trait ExternalSchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined
   * schema (and possibly other backend-specific clauses).
   * Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      schema: String,
      data: Option[LogicalPlan]): BaseRelation
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

  def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], Seq[RDD[InternalRow]])
}
