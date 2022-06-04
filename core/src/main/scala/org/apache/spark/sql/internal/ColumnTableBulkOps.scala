/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import scala.collection.mutable.ArrayBuffer

import io.snappydata.Property
import it.unimi.dsi.fastutil.Hash.Strategy
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, BindReferences, EmptyRow, EqualTo, Expression, JoinedRow, Murmur3Hash, UnsafeProjection}
import org.apache.spark.sql.catalyst.optimizer.{CollapseProject, RemoveRedundantProject, SimplifyCasts}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LocalRelation, LogicalPlan, OneRowRelation, OverwriteOptions, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.{AnalysisException, CachedDataFrame, Dataset, JoinStrategy, Row, SnappySession, SparkSession}

/**
 * Helper object for PutInto operations for column tables.
 * This class takes the logical plans from SnappyParser
 * and converts it into another plan.
 */
object ColumnTableBulkOps {

  def transformPutPlan(session: SnappySession, originalPlan: PutIntoTable): LogicalPlan = {
    validateOp(originalPlan)
    val table = originalPlan.table
    var subQuery = originalPlan.child
    val subQueryOutput = subQuery.output
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case LogicalRelation(mutable: BulkPutRelation, _, _) =>
        val putKeys = mutable.getPutKeys(session) match {
          case None => throw new AnalysisException(
            s"PutInto in a column table requires key column(s) but got empty string")
          case Some(k) => k
        }
        val condition = prepareCondition(session, table, subQuery, putKeys)

        val keyColumns = getKeyColumns(table)
        var updateSubQuery: LogicalPlan = Join(table, subQuery, Inner, condition)
        val updateColumns = table.output.filterNot(a => keyColumns.contains(a.name))
        val updateExpressions = subQueryOutput.filterNot(a => keyColumns.contains(a.name))
        if (updateExpressions.isEmpty) {
          throw new AnalysisException(
            s"PutInto is attempted without any column which can be updated." +
                s" Provide some columns apart from key column(s)")
        }

        val sqlConf = session.sqlContext.conf
        val cacheSize = ExternalStoreUtils.sizeAsBytes(
          Property.PutIntoInnerJoinCacheSize.get(sqlConf),
          Property.PutIntoInnerJoinCacheSize.name, -1, Long.MaxValue)
        val subquerySize = subQuery.statistics.sizeInBytes
        val localCache = if (session.conf.contains(Property.PutIntoInnerJoinLocalCache.name)) {
          Property.PutIntoInnerJoinLocalCache.get(sqlConf)
        } else if (subQuery.find(l => l.isInstanceOf[LocalRelation]
            || l == OneRowRelation).isDefined) {
          // simplify projections and then check against the cases used for automatic local caching
          val maxIterations = sqlConf.optimizerMaxIterations
          var i = 0
          var samePlan = false
          while (!samePlan && i < maxIterations) {
            val newSubQuery = RemoveRedundantProject(SimplifyCasts(CollapseProject(subQuery)))
            samePlan = newSubQuery.fastEquals(subQuery)
            subQuery = newSubQuery
            i += 1
          }
          // check if incoming data is from a simple LocalRelation/OneRowRelation
          subQuery match {
            case _: LocalRelation | Project(_, _: LocalRelation | OneRowRelation) =>
              subquerySize <= JoinStrategy.getMaxHashJoinSize(sqlConf)
            case _ => false
          }
        } else false

        val updatePlan = Update(table, updateSubQuery, Nil,
          updateColumns, updateExpressions)
        val updateDS = new Dataset(session, updatePlan, RowEncoder(updatePlan.schema))
        val analyzedUpdate = updateDS.queryExecution.analyzed.asInstanceOf[Update]
        var updateOrSkip: LogicalPlan = analyzedUpdate
        updateSubQuery = analyzedUpdate.child

        // explicitly project out only the updated expression references and key columns
        // from the sub-query to minimize cache (if it is selected to be done)
        val analyzer = session.sessionState.analyzer
        val updateReferences = AttributeSet(updateExpressions.flatMap(_.references))
        updateSubQuery = Project(updateSubQuery.output.filter(a =>
          updateReferences.contains(a) || keyColumns.contains(a.name) ||
              putKeys.exists(k => analyzer.resolver(a.name, k))), updateSubQuery)

        // higher level CodegenSparkFallback/CachedDataFrame will never retry DML operations on
        // stale catalog, so explicitly retry just the caching if it fails
        var success = false
        val cachedChild = try {
          val result = session.cachePutInto(localCache,
            cacheSize < 0 || subquerySize <= cacheSize, updateSubQuery, mutable.table)
          success = true
          result
        } catch {
          case t: Throwable if CachedDataFrame.isConnectorCatalogStaleException(t, session) =>
            session.sessionCatalog.invalidateAll()
            SnappySession.clearAllCache()
            // throw failure immediately to keep it consistent with insert/update/delete
            throw CachedDataFrame.catalogStaleFailure(t, session)
        } finally {
          if (!success) session.clearPutInto()
        }
        val insertChild = cachedChild match {
          case None =>
            // no update is required
            updateOrSkip = LocalRelation(analyzedUpdate.output)
            subQuery
          case Some(newUpdateSubQuery) =>
            // project on just the join columns so that its broadcast/exchange size can be reduced
            if (updateSubQuery ne newUpdateSubQuery) {
              updateOrSkip = analyzedUpdate.copy(child = newUpdateSubQuery)
              // the original condition is commutative so it still good to use as is even though
              // the "subQuery" is now on the left side of the anti-join
              newUpdateSubQuery match {
                case joinData: LocalRelation =>
                  // optimize to do a local anti-join if the data being put is also LocalRelation
                  if (condition.isEmpty) joinData
                  else subQuery match {
                    case LocalRelation(_, putData) =>
                      localAntiJoin(session, joinData, putData.iterator,
                        subQueryOutput, condition.get, putKeys)
                    case Project(exprs, lr: LocalRelation) =>
                      // if projection is a simple one-to-one alias, then pass data as is
                      if (exprs.length == lr.output.length &&
                          exprs.zip(lr.output).forall(p => p._1.exprId == p._2.exprId ||
                              (p._1.isInstanceOf[Alias] &&
                                  p._1.asInstanceOf[Alias].child.isInstanceOf[Attribute] &&
                                  p._1.asInstanceOf[Alias].child.asInstanceOf[Attribute].exprId ==
                                      p._2.exprId))) {
                        localAntiJoin(session, joinData, lr.data.iterator,
                          subQueryOutput, condition.get, putKeys)
                      } else {
                        val proj = UnsafeProjection.create(exprs, lr.output)
                        localAntiJoin(session, joinData, lr.data.iterator.map(proj),
                          subQueryOutput, condition.get, putKeys)
                      }
                    case Project(exprs, OneRowRelation) =>
                      val proj = UnsafeProjection.create(exprs)
                      localAntiJoin(session, joinData, Iterator(proj(EmptyRow)),
                        subQueryOutput, condition.get, putKeys)
                    case _ =>
                      // create a copy for anti-join so that original is used for the UPDATE
                      // while the condition has to be re-evaluated since ExprIds will change
                      val newData = joinData.newInstance()
                      Join(subQuery, projectJoinKeys(analyzer, newData, putKeys), LeftAnti,
                        prepareCondition(session, subQuery, newData, putKeys))
                  }
                case _ =>
                  Join(subQuery, projectJoinKeys(analyzer, newUpdateSubQuery, putKeys), LeftAnti,
                    condition)
              }
            } else {
              Join(subQuery, projectJoinKeys(analyzer, updateSubQuery, putKeys), LeftAnti,
                condition)
            }
        }
        val insertPlan = insertChild match {
          case lr: LocalRelation if lr.data.isEmpty =>
            LocalRelation(AttributeReference("count", LongType)() :: Nil)
          case _ => new Insert(table, Map.empty[String, Option[String]], Project(subQueryOutput,
            insertChild), OverwriteOptions(enabled = false), ifNotExists = false)
        }

        transFormedPlan = PutIntoColumnTable(table, insertPlan, updateOrSkip)
      case _ => // Do nothing, original putInto plan is enough
    }
    transFormedPlan
  }

  private def localAntiJoin(sparkSession: SparkSession, joinData: LocalRelation,
      putData: Iterator[InternalRow], putAttrs: Seq[Attribute], condition: Expression,
      putKeys: Seq[String]): LogicalPlan = {
    if (joinData.data.isEmpty) return LocalRelation(putAttrs, putData.map(_.copy()).toSeq)

    val joinOut = joinData.output
    val joinKeys = findJoinKeys(sparkSession.sessionState.analyzer, joinData, putKeys, "JOIN").map(
      BindReferences.bindReference[Expression](_, joinOut))
    // when populating the table, joinData will be compared against itself so the position of the
    // columns is adjusted in putAttr as per the size of joinData
    val dummyCol = AttributeReference("", LongType)()
    var checkCondition = BindReferences.bindReference(condition,
      putAttrs ++ Seq.fill(joinOut.length - putAttrs.length)(dummyCol) ++ joinOut)
    // the original condition is commutative so it still good to use as is even though
    // the "subQuery" is now on the left side of the anti-join; moreover the joinData
    // *should* be on the right side since it also contains the same columns as putAttrs
    // at the end, hence bindReference will resolve to all its columns if its on the left
    // (and then the condition.eval will obviously be always true)
    val resolvedCondition = BindReferences.bindReference(condition, putAttrs ++ joinOut)
    val hashingStrategy = new Strategy[InternalRow] {

      private[this] val hashFunction = Murmur3Hash(joinKeys, 42)
      private[this] val joinedRow = new JoinedRow()

      override def hashCode(row: InternalRow): Int = {
        hashFunction.eval(row).asInstanceOf[Int]
      }

      override def equals(putRow: InternalRow, joinRow: InternalRow): Boolean = {
        // ObjectOpenCustomHashSet explicitly checks incoming key against null
        if (joinRow eq null) putRow eq null
        else checkCondition.eval(joinedRow(putRow, joinRow)) == Boolean.box(true)
      }
    }
    val map = new ObjectOpenCustomHashSet[InternalRow](
      math.min(128, joinData.data.length), hashingStrategy)
    joinData.data.foreach(map.add)
    // use comparison of joinRow to putRow for the anti-join
    // (no rehash is possible so identity comparisons are not required)
    checkCondition = resolvedCondition
    val insertData = new ArrayBuffer[InternalRow]()
    putData.foreach(row => if (!map.contains(row)) insertData += row.copy())
    LocalRelation(putAttrs, insertData)
  }

  def validateOp(originalPlan: PutIntoTable) {
    originalPlan match {
      case PutIntoTable(LogicalRelation(t: BulkPutRelation, _, _), query) =>
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }
      case _ => // OK
    }
  }

  private def projectJoinKeys(analyzer: Analyzer, plan: LogicalPlan,
      columnNames: Seq[String]): LogicalPlan = {
    Project(findJoinKeys(analyzer, plan, columnNames, "JOIN"), plan)
  }

  private def findJoinKeys(analyzer: Analyzer, plan: LogicalPlan,
      columnNames: Seq[String], side: String): Seq[Attribute] = {
    columnNames.map { keyName =>
      plan.output.find(attr => analyzer.resolver(attr.name, keyName)) match {
        case Some(attr) => attr
        case _ =>
          throw new AnalysisException(s"PUT column `$keyName` cannot be resolved on " +
              s"the $side side of the operation. The $side-side columns: " +
              s"[${plan.output.map(_.name).mkString(", ")}]")
      }
    }
  }

  private def prepareCondition(sparkSession: SparkSession,
      table: LogicalPlan,
      child: LogicalPlan,
      columnNames: Seq[String]): Option[Expression] = {
    val analyzer = sparkSession.sessionState.analyzer
    val leftKeys = findJoinKeys(analyzer, table, columnNames, "left")
    val rightKeys = findJoinKeys(analyzer, child, columnNames, "right")
    val joinPairs = leftKeys.zip(rightKeys)
    val newCondition = joinPairs.map(EqualTo.tupled).reduceOption(And)
    newCondition
  }

  def getKeyColumns(table: LogicalPlan): Set[String] = {
    table.collectFirst {
      case LogicalRelation(mutable: MutableRelation, _, _) => mutable.getKeyColumns.toSet
    } match {
      case None => throw new AnalysisException(
        s"Update/Delete requires a MutableRelation but got $table")
      case Some(k) => k
    }
  }

  def transformDeletePlan(session: SnappySession,
      originalPlan: DeleteFromTable): LogicalPlan = {
    val table = originalPlan.table
    val subQuery = originalPlan.child
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case LogicalRelation(mutable: MutableRelation, _, _) =>
        val ks = mutable.getPrimaryKeyColumns(session)
        if (ks.isEmpty) {
          throw new AnalysisException(
            s"DeleteFrom operation requires key columns(s) or primary key defined on table.")
        }
        val condition = prepareCondition(session, table, subQuery, ks)
        val exists = Join(subQuery, table, Inner, condition)
        val deletePlan = Delete(table, exists, Nil)
        val deleteDs = new Dataset(session, deletePlan, RowEncoder(deletePlan.schema))
        transFormedPlan = deleteDs.queryExecution.analyzed.asInstanceOf[Delete]
    }
    transFormedPlan
  }

  def bulkInsertOrPut(rows: Seq[Row], sparkSession: SparkSession,
      schema: StructType, resolvedName: String, putInto: Boolean): Int = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val tableIdent = session.tableIdentifier(resolvedName)
    val ds = session.createDataFrame(rows, schema)
    val plan = if (putInto) {
      PutIntoTable(
        table = UnresolvedRelation(tableIdent),
        child = ds.logicalPlan)
    } else {
      new Insert(
        table = UnresolvedRelation(tableIdent),
        partition = Map.empty[String, Option[String]],
        child = ds.logicalPlan,
        overwrite = OverwriteOptions(enabled = false),
        ifNotExists = false)
    }
    session.sessionState.executePlan(plan).executedPlan.executeCollect()
        // always expect to create a TableInsertExec
        .foldLeft(0)(_ + _.getInt(0))
  }
}

case class PutIntoColumnTable(table: LogicalPlan,
    insert: LogicalPlan, update: LogicalPlan) extends BinaryNode {

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved &&
      update.output.zip(insert.output).forall {
        case (updateAttr, insertAttr) =>
          DataType.equalsIgnoreCompatibleNullability(updateAttr.dataType,
            insertAttr.dataType)
      }

  override def left: LogicalPlan = update

  override def right: LogicalPlan = insert
}
