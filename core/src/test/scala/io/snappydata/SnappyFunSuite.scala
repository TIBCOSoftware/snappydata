/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata

import java.io.File
import java.sql.Statement

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.shared.NativeCalls
import io.snappydata.core.{FileCleaner, LocalSparkConf}
import io.snappydata.test.dunit.DistributedTestBase
import io.snappydata.test.dunit.DistributedTestBase.{InitializeRun, WaitCriterion}
import io.snappydata.util.TestUtils
import org.scalatest.Assertions

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualNullSafe, EqualTo, Exists, ExprId, Expression, ListQuery, PredicateHelper, PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, OneRowRelation, Sample}
import org.apache.spark.sql.catalyst.util.{sideBySide, stackTraceToString}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.row.SnappyStoreDialect
import org.apache.spark.sql.types.{Metadata, StructField, StructType, TypeUtilities}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row, SnappySession}
// scalastyle:off
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome, Retries}
// scalastyle:on

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Base abstract class for all SnappyData tests similar to SparkFunSuite.
 */
abstract class SnappyFunSuite
    extends FunSuite // scalastyle:ignore
        with BeforeAndAfterAll
        with Serializable
        with Logging with Retries {

  InitializeRun.setUp()

  private val nativeCalls = NativeCalls.getInstance()
  nativeCalls.setEnvironment("gemfire.bind-address", "localhost")
  nativeCalls.setEnvironment("SPARK_LOCAL_IP", "localhost")
  nativeCalls.setEnvironment("SPARK_PUBLIC_DNS", "localhost")

  protected var testName: String = _
  protected val dirList: ArrayBuffer[String] = ArrayBuffer[String]()

  protected def sc: SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) {
      ctx
    } else {
      cachedContext = null
      new SparkContext(newSparkConf())
    }
  }

  protected def sc(addOn: SparkConf => SparkConf): SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) {
      ctx
    }
    else {
      cachedContext = null
      new SparkContext(newSparkConf(addOn))
    }
  }

  protected def scWithConf(addOn: SparkConf => SparkConf): SparkContext = {
    new SparkContext(newSparkConf(addOn))
  }

  @transient private var cachedContext: SnappyContext = _

  def getOrCreate(sc: SparkContext): SnappyContext = {
    val gnc = cachedContext
    if (gnc ne null) gnc
    else synchronized {
      val gnc = cachedContext
      if (gnc ne null) gnc
      else {
        cachedContext = SnappyContext(sc)
        cachedContext
      }
    }
  }

  protected def snc: SnappyContext = getOrCreate(sc)

  /**
   * Copied from SparkFunSuite.
   *
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("io.snappydata", "i.sd").
        replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      if (isRetryable(test)) withRetry {
        super.withFixture(test)
      } else super.withFixture(test)
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  def deleteDir(dir: String): Boolean = {
    FileCleaner.deletePath(dir)
  }

  protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf =
    LocalSparkConf.newConf(addOn)

  protected def dirCleanup(): Unit = {
    if (dirList.nonEmpty) {
      dirList.foreach(FileCleaner.deletePath)
      dirList.clear()
    }
  }

  protected def baseCleanup(clearStoreToBlockMap: Boolean = true): Unit = {
    try {
      val session = this.snc.snappySession
      TestUtils.dropAllSchemas(session)
    } finally {
      dirCleanup()
    }
  }

  override def beforeAll(): Unit = {
    log.info("Snappy Config:" + snc.sessionState.conf.getAllConfs.toString())

    baseCleanup()
  }

  override def afterAll(): Unit = {
    baseCleanup()
  }

  /**
   * Wait until given criterion is met
   *
   * @param check          Function criterion to wait on
   * @param ms             total time to wait, in milliseconds
   * @param interval       pause interval between waits
   * @param throwOnTimeout if false, don't generate an error
   */
  def waitForCriterion(check: => Boolean, desc: String, ms: Long,
      interval: Long, throwOnTimeout: Boolean): Unit = {
    val criterion = new WaitCriterion {

      override def done: Boolean = {
        check
      }

      override def description(): String = desc
    }
    DistributedTestBase.waitForCriterion(criterion, ms, interval,
      throwOnTimeout)
  }

  def stopAll(): Unit = {
    val sc = SnappyContext.globalSparkContext
    logInfo("Check stop required for spark context = " + sc)
    if (sc != null && !sc.isStopped) {
      logInfo("Stopping spark context = " + sc)
      sc.stop()
    }
    cachedContext = null
  }

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    dirList += fileName
    fileName
  }

  def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit =
    SnappyFunSuite.checkAnswer(df, expectedAnswer)
}

object SnappyFunSuite extends Assertions {
  def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
   */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

  private def withName(name: String, metadata: Metadata): Metadata =
    TypeUtilities.putMetadata("name", name, metadata)

  /**
   * Converts a JDBC ResultSet to a DataFrame.
   */
  def resultSetToDataset(session: SnappySession, stmt: Statement)
      (sql: String): Dataset[Row] = {
    if (stmt.execute(sql)) {
      val rs = stmt.getResultSet
      val schema = StructType(JdbcUtils.getSchema(rs, SnappyStoreDialect).map(f => StructField(
        f.name.toLowerCase, f.dataType, f.nullable, withName(f.name.toLowerCase, f.metadata))))
      val rows = Utils.resultSetToSparkInternalRows(rs, schema).map(_.copy()).toSeq
      session.internalCreateDataFrame(session.sparkContext.makeRDD(rows), schema)
    } else {
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(StructType(Nil))
      session.createDataset[Row](Nil)
    }
  }
}

/**
 * Provides helper methods for comparing plans.
 * <br>
 * <br><b>NOTE:</b></br> Copy of org.apache.spark.sql.catalyst.plans. PlanTest and converted to a
 * trait for inclusion to SnappyFunSuite. Strictly speaking this should have been a trait in Spark
 * itself but its an abstract class & parent to all spark tests. Later we can revisit how best
 * we can reuse the spark test code.
 */
trait PlanTest extends SnappyFunSuite with PredicateHelper {
  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case p: PredicateSubquery =>
        p.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      // case ae: AggregateExpression =>
      // ae.copy(resultId = ExprId(0))
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   */
  private def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(_.hashCode())
            .reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)(true)
      case Join(left, right, joinType, condition) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteEqual).sortBy(_.hashCode())
              .reduce(And)
        Join(left, right, joinType, Some(newCondition))
    }
  }

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case EqualTo(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation), Filter(e2, OneRowRelation))
  }
}
