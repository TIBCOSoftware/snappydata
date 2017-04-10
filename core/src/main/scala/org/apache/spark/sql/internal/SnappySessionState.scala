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

package org.apache.spark.sql.internal

import java.util.{Calendar, Properties}
import javassist.bytecode.stackmap.TypeData.NullType

import scala.collection.concurrent.TrieMap
import scala.reflect.{ClassTag, classTag}

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types._
import io.snappydata.Property

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, TypedConfigBuilder}
import org.apache.spark.sql._
import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.{CatalystConf, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryComparison, Cast, Expression, Like, Literal, ParamConstants, ParamLiteral, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, DynamicFoldableExpression, EmptyRow, Expression, Literal, ParamLiteral, PredicateHelper, UnaryExpression}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, ReorderJoin}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, HadoopFsRelation, LogicalRelation, ResolveDataSource, StoreDataSourceStrategy}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.hive.{SnappyConnectorCatalog, SnappyStoreHiveCatalog}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, WindowLogicalPlan}
import org.apache.spark.sql.types.{DataType, DecimalType}
import org.apache.spark.streaming.Duration
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkConf}


class SnappySessionState(snappySession: SnappySession)
    extends SessionState(snappySession) {

  self =>

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  protected lazy val sharedState: SnappySharedState =
    snappySession.sharedState.asInstanceOf[SnappySharedState]

  private[internal] lazy val metadataHive = sharedState.metadataHive.newSession()

  override lazy val sqlParser: SnappySqlParser =
    contextFunctions.newSQLParser(this.snappySession)

  object analyzerRules extends Analyzer(catalog, conf){
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new PreprocessTableInsertOrPut(conf) ::
          new FindDataSourceTable(snappySession) ::
          DataSourceAnalysis(conf) ::
          ResolveRelationsExtended ::
          AnalyzeChildQuery(snappySession) ::
          ResolveQueryHints(snappySession) ::
          (if (conf.runSQLonFile) new ResolveDataSource(snappySession) :: Nil else Nil)
  }

  /* * Important:
    * All Analyzer rules changes should be done in object analyzerRules
    * Do not override extendedResolutionRules here.
    * */
  override lazy val analyzer: Analyzer = new Analyzer(catalog, conf) {
    override lazy val batches: Seq[Batch] = analyzerRules.batches.map {
      case batch if batch.name.equalsIgnoreCase("Resolution") =>
        // ResolveParameters must come before TypeCoercion rules
        val splitIndex = batch.rules.indexWhere(_.ruleName.contains("TypeCoercion"))
        val (left, right) = batch.rules.splitAt(splitIndex)
        Batch(batch.name, batch.strategy.asInstanceOf[this.Strategy],
          left ++ Some(ResolveParameters) ++ right: _*)
      case otherBatch => Batch(otherBatch.name, otherBatch.strategy.asInstanceOf[this.Strategy],
        otherBatch.rules: _*)
    }

    override val extendedCheckRules = Seq(datasources.PreWriteCheck(conf, catalog), PrePutCheck)
  }

  override lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods) {
    override def batches: Seq[Batch] = {
      implicit val ss = snappySession
      var insertedSnappyOpts = 0
      val modified = super.batches.map {
        case batch if batch.name.equalsIgnoreCase("Operator Optimizations") =>
          insertedSnappyOpts += 1
          val (left, right) = batch.rules.splitAt(batch.rules.indexOf(ReorderJoin))
          Batch(batch.name, batch.strategy, left ++ Some(ResolveIndex()) ++ right
              : _*)
        case b => b
      }

      if (insertedSnappyOpts != 1) {
        throw new AnalysisException("Snappy Optimizations not applied")
      }

      modified :+
          Batch("Streaming SQL Optimizers", Once, PushDownWindowLogicalPlan) :+
          Batch("Link buckets to RDD partitions", Once, LinkPartitionsToBuckets) :+
          Batch("ParamLiteral Folding Optimization", Once, ParamLiteralFolding)
    }
  }

  // copy of ConstantFolding that will turn a constant up/down cast into
  // a static value.
  object ParamLiteralFolding extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case p: ParamLiteral => p.markFoldable(true)
        p
    } transform {
      case q: LogicalPlan => q transformExpressionsDown {
        // ignore leaf ParamLiteral & Literal
        case p: ParamLiteral => p
        case l: Literal => l
        // Wrap expressions that are foldable.
        case e if e.foldable =>
          // lets mark child params foldable false so that nested expression doesn't
          // attempt to wrap.
          e.foreach {
            case p: ParamLiteral => p.markFoldable(false)
            case _ =>
          }
          DynamicFoldableExpression(e)
      }
    }
  }

  object PushDownWindowLogicalPlan extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      var duration: Duration = null
      var slide: Option[Duration] = None
      var transformed: Boolean = false
      plan transformDown {
        case win@WindowLogicalPlan(d, s, child, false) =>
          child match {
            case LogicalRelation(_, _, _) |
                 LogicalDStreamPlan(_, _) => win
            case _ => duration = d
              slide = s
              transformed = true
              win.child
          }
        case c@(LogicalRelation(_, _, _) |
                LogicalDStreamPlan(_, _)) =>
          if (transformed) {
            transformed = false
            WindowLogicalPlan(duration, slide, c, transformed = true)
          } else c
      }
    }
  }

  /**
   * This rule sets the flag at query level to link the partitions to
   * be created for tables to be the same as number of buckets. This will avoid
   * exchange on one side of a non-collocated join in many cases.
   */
  object LinkPartitionsToBuckets extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.foreach {
        case j: Join if !JoinStrategy.isLocalJoin(j) =>
          // disable for the entire query for consistency
          snappySession.linkPartitionsToBuckets(flag = true)
        case _: InsertIntoTable | _: PutIntoTable =>
          // disable for inserts/puts to avoid exchanges
          snappySession.linkPartitionsToBuckets(flag = true)
        case PhysicalOperation(_, _, LogicalRelation(
        _: IndexColumnFormatRelation, _, _)) =>
          snappySession.linkPartitionsToBuckets(flag = true)
        case _ => // nothing for others
      }
      plan
    }
  }

  override lazy val conf: SnappyConf = new SnappyConf(snappySession)

  /**
   * The partition mapping selected for the lead partitioned region in
   * a collocated chain for current execution
   */
  private[spark] val leaderPartitions = new TrieMap[PartitionedRegion,
      Array[Partition]]()

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelationsExtended extends Rule[LogicalPlan] with PredicateHelper {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i@PutIntoTable(u: UnresolvedRelation, _) =>
        i.copy(table = EliminateSubqueryAliases(getTable(u)))
      case d@DMLExternalTable(_, u: UnresolvedRelation, _) =>
        d.copy(query = EliminateSubqueryAliases(getTable(u)))
    }
  }

  case class AnalyzeChildQuery(sparkSession: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case c: DMLExternalTable if !c.query.resolved =>
        c.copy(query = analyzeQuery(c.query))
    }

    private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
      val qe = sparkSession.sessionState.executePlan(query)
      qe.assertAnalyzed()
      qe.analyzed
    }
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    SnappyContext.getClusterMode(snappySession.sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
        new SnappyConnectorCatalog(
          sharedState.externalCatalog,
          snappySession,
          metadataHive,
          functionResourceLoader,
          functionRegistry,
          conf,
          newHadoopConf())
      case _ =>
        new SnappyStoreHiveCatalog(
          sharedState.externalCatalog,
          snappySession,
          metadataHive,
          functionResourceLoader,
          functionRegistry,
          conf,
          newHadoopConf())
    }
  }

  override def planner: SparkPlanner = new DefaultPlanner(snappySession, conf,
    experimentalMethods.extraStrategies)

  protected[sql] def queryPreparations: Seq[Rule[SparkPlan]] = Seq(
    python.ExtractPythonUDFs,
    PlanSubqueries(snappySession),
    EnsureRequirements(snappySession.sessionState.conf),
    CollapseCollocatedPlans(snappySession),
    CollapseCodegenStages(snappySession.sessionState.conf),
    InsertCachedPlanHelper(snappySession),
    ReuseExchange(snappySession.sessionState.conf))

  override def executePlan(plan: LogicalPlan): QueryExecution = {
    clearExecutionData()
    new QueryExecution(snappySession, plan) {
      override protected def preparations: Seq[Rule[SparkPlan]] =
        queryPreparations
    }
  }

  private[spark] def prepareExecution(plan: SparkPlan): SparkPlan = {
    clearExecutionData()
    queryPreparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  private[spark] def clearExecutionData(): Unit = {
    conf.refreshNumShufflePartitions()
    leaderPartitions.clear()
    snappySession.clearContext()
  }

  def getTablePartitions(region: PartitionedRegion): Array[Partition] = {
    val leaderRegion = ColocationHelper.getLeaderRegion(region)
    leaderPartitions.getOrElseUpdate(leaderRegion, {
      val linkPartitionsToBuckets = snappySession.hasLinkPartitionsToBuckets
      if (linkPartitionsToBuckets) {
        // also set the default shuffle partitions for this execution
        // to minimize exchange
        snappySession.sessionState.conf.setExecutionShufflePartitions(
          region.getTotalNumberOfBuckets)
      }
      StoreUtils.getPartitionsPartitionedTable(snappySession, leaderRegion,
        linkPartitionsToBuckets)
    })
  }

  def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
    StoreUtils.getPartitionsReplicatedTable(snappySession, region)

  var isPreparePhase: Boolean = false

  var pvs: ParameterValueSet = null

  def setPreparedQuery(preparePhase: Boolean, paramSet: ParameterValueSet): Unit = {
    isPreparePhase = preparePhase
    pvs = paramSet
  }

  object ResolveParameters extends Rule[LogicalPlan] {
    def getDataTypeResolvedPlan(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case b@BinaryComparison(left: AttributeReference, right: ParamConstants) =>
        b.makeCopy(Array(left, right.copy(paramType = left.dataType,
          nullableValue = left.nullable)))
      case b@BinaryComparison(left: ParamConstants, right: AttributeReference) =>
        b.makeCopy(Array(left.copy(paramType = right.dataType,
          nullableValue = right.nullable), right))
      case l@Like(left: AttributeReference, right: ParamConstants) =>
        l.makeCopy(Array(left, right.copy(paramType = left.dataType,
          nullableValue = left.nullable)))
      case l@Like(left: ParamConstants, right: AttributeReference) =>
        l.makeCopy(Array(left.copy(paramType = right.dataType,
          nullableValue = right.nullable), right))
      case i@org.apache.spark.sql.catalyst.expressions.In(value: Expression,
      list: Seq[Expression]) => i.makeCopy(Array(value, list.map {
        case pc: ParamConstants => pc.copy(paramType = value.dataType,
          nullableValue = value.nullable)
        case x => x
      }))
    }

    def assertAllDataTypeResolved(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case pc@ParamConstants(_, paramType, _) =>
        assert(paramType != org.apache.spark.sql.types.NullType)
        pc
    }

    def assertAllParametersResolved(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case pc@ParamConstants(_, _, _) =>
        assert(false, "Not all parameters value could be replaced")
        pc
    }

    def countLiterals(plan: LogicalPlan): (Int, Int) = {
      var countParams = 0
      var maxParamLiteral = 0
      plan transformAllExpressions {
        case pc: ParamConstants =>
          countParams = countParams + 1
          pc
        case pl: ParamLiteral =>
          if (pl.pos > maxParamLiteral) maxParamLiteral = pl.pos
          pl
      }
      (countParams, maxParamLiteral)
    }

    def setValue(dvd: DataValueDescriptor): Any = {
      dvd match {
        case i: SQLInteger => i.getInt
        case si: SQLSmallint => si.getShort
        case ti: SQLTinyint => ti.getByte
        case d: SQLDouble => d.getDouble
        case li: SQLLongint => li.getLong

        case bid: BigIntegerDecimal => bid.getDouble
        case de: SQLDecimal => de.getBigDecimal
        case r: SQLReal => r.getFloat

        case b: SQLBoolean => b.getBoolean

        case cl: SQLClob =>
          val charArray = cl.getCharArray()
          if (charArray != null) {
            val str = String.valueOf(charArray)
            UTF8String.fromString(str)
          } else null
        case lvc: SQLLongvarchar => UTF8String.fromString(lvc.getString)
        case vc: SQLVarchar => UTF8String.fromString(vc.getString)
        case c: SQLChar => UTF8String.fromString(c.getString)

        case ts: SQLTimestamp => ts.getTimestamp(null)
        case t: SQLTime => t.getTime(null)
        case d: SQLDate =>
          val c: Calendar = null
          d.getDate(c)

        case _ => dvd.getObject
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = if (isPreparePhase) {
      val preparedPlan = getDataTypeResolvedPlan(plan)
      assertAllDataTypeResolved(preparedPlan)
    } else if (pvs != null) {
      val (countParams, maxParamLiteral) = countLiterals(plan)
      if (countParams > 0) {
        assert(pvs.getParameterCount == countParams,
          s"Unequal param count: pvs-count=${pvs.getParameterCount}" +
              s" param-count=$countParams")
        val preparedPlan = getDataTypeResolvedPlan(plan)
        assertAllDataTypeResolved(preparedPlan)
        var paramLiteralCount = maxParamLiteral
        val parameterResolvedPlan = preparedPlan transformAllExpressions {
          case pc@ParamConstants(pos, paramType, _) =>
            val dvd = pvs.getParameter(pos - 1)
            paramLiteralCount = paramLiteralCount + 1
            val scalaTypeVal = setValue(dvd)
            val catalystTypeVal = CatalystTypeConverters.convertToCatalyst(scalaTypeVal)
            ParamLiteral(catalystTypeVal, paramType, paramLiteralCount, true)
        }
        assertAllParametersResolved(parameterResolvedPlan)
      } else plan // means already done
    } else plan
  }
}

class SnappyConf(@transient val session: SnappySession)
    extends SQLConf with Serializable with CatalystConf {

  /** If shuffle partitions is set by [[setExecutionShufflePartitions]]. */
  @volatile private[this] var executionShufflePartitions: Int = _

  /**
   * Records the number of shuffle partitions to be used determined on runtime
   * from available cores on the system. A value <= 0 indicates that it was set
   * explicitly by user and should not use a dynamic value.
   */
  @volatile private[this] var dynamicShufflePartitions: Int = _

  SQLConf.SHUFFLE_PARTITIONS.defaultValue match {
    case Some(d) if session != null && super.numShufflePartitions == d =>
      dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
    case None if session != null =>
      dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
    case _ =>
      executionShufflePartitions = -1
      dynamicShufflePartitions = -1
  }

  private def keyUpdateActions(key: String, doSet: Boolean): Unit = key match {
    // clear plan cache when some size related key that effects plans changes
    case SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key |
         Property.HashJoinSize.name => session.clearPlanCache()
    case SQLConf.SHUFFLE_PARTITIONS.key =>
      // stop dynamic determination of shuffle partitions
      if (doSet) {
        executionShufflePartitions = -1
        dynamicShufflePartitions = -1
      } else {
        dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
      }
    case _ => // ignore others
  }

  private[sql] def refreshNumShufflePartitions(): Unit = synchronized {
    if (session ne null) {
      if (executionShufflePartitions != -1) {
        executionShufflePartitions = 0
      }
      if (dynamicShufflePartitions != -1) {
        dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
      }
    }
  }

  private[sql] def setExecutionShufflePartitions(n: Int): Unit = synchronized {
    if (executionShufflePartitions != -1 && session != null) {
      executionShufflePartitions = math.max(n, executionShufflePartitions)
    }
  }

  override def numShufflePartitions: Int = {
    val partitions = this.executionShufflePartitions
    if (partitions > 0) partitions
    else {
      val partitions = this.dynamicShufflePartitions
      if (partitions > 0) partitions else super.numShufflePartitions
    }
  }

  override def setConfString(key: String, value: String): Unit = {
    keyUpdateActions(key, doSet = true)
    super.setConfString(key, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    keyUpdateActions(entry.key, doSet = true)
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    entry.defaultValue match {
      case Some(_) => super.setConf(entry, value)
      case None => super.setConf(entry.asInstanceOf[ConfigEntry[Option[T]]], Some(value))
    }
  }

  override def unsetConf(key: String): Unit = {
    keyUpdateActions(key, doSet = false)
    super.unsetConf(key)
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    keyUpdateActions(entry.key, doSet = false)
    super.unsetConf(entry)
  }
}

class SQLConfigEntry private(private[sql] val entry: ConfigEntry[_]) {

  def key: String = entry.key

  def doc: String = entry.doc

  def isPublic: Boolean = entry.isPublic

  def defaultValue[T]: Option[T] = entry.defaultValue.asInstanceOf[Option[T]]

  def defaultValueString: String = entry.defaultValueString

  def valueConverter[T]: String => T =
    entry.asInstanceOf[ConfigEntry[T]].valueConverter

  def stringConverter[T]: T => String =
    entry.asInstanceOf[ConfigEntry[T]].stringConverter

  override def toString: String = entry.toString
}

object SQLConfigEntry {

  private def handleDefault[T](entry: TypedConfigBuilder[T],
      defaultValue: Option[T]): SQLConfigEntry = defaultValue match {
    case Some(v) => new SQLConfigEntry(entry.createWithDefault(v))
    case None => new SQLConfigEntry(entry.createOptional)
  }

  def sparkConf[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](ConfigBuilder(key)
          .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](ConfigBuilder(key)
          .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](ConfigBuilder(key)
          .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](ConfigBuilder(key)
          .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](ConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }

  def apply[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](SQLConfigBuilder(key)
          .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](SQLConfigBuilder(key)
          .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](SQLConfigBuilder(key)
          .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](SQLConfigBuilder(key)
          .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](SQLConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }
}

trait AltName[T] {

  def name: String

  def altName: String

  def configEntry: SQLConfigEntry

  def defaultValue: Option[T] = configEntry.defaultValue[T]

  def getOption(conf: SparkConf): Option[String] = if (altName == null) {
    conf.getOption(name)
  } else {
    conf.getOption(name) match {
      case s: Some[String] => // check if altName also present and fail if so
        if (conf.contains(altName)) {
          throw new IllegalArgumentException(
            s"Both $name and $altName configured. Only one should be set.")
        } else s
      case None => conf.getOption(altName)
    }
  }

  def getProperty(properties: Properties): String = if (altName == null) {
    properties.getProperty(name)
  } else {
    val v = properties.getProperty(name)
    if (v != null) {
      // check if altName also present and fail if so
      if (properties.getProperty(altName) != null) {
        throw new IllegalArgumentException(
          s"Both $name and $altName specified. Only one should be set.")
      }
      v
    } else properties.getProperty(altName)
  }

  def unapply(key: String): Boolean = name.equals(key) ||
      (altName != null && altName.equals(key))
}

trait SQLAltName[T] extends AltName[T] {

  private def get(conf: SQLConf, entry: SQLConfigEntry): T = {
    entry.defaultValue match {
      case Some(_) => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[T]])
      case None => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[Option[T]]]).get
    }

  }

  private def get(conf: SQLConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](conf.getConfString(name, defaultValue)).get
    }

  }

  def get(conf: SQLConf): T = if (altName == null) {
    get(conf, configEntry)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, configEntry)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def getOption(conf: SQLConf): Option[T] = if (altName == null) {
    if (conf.contains(name)) Some(get(conf, name, "<undefined>"))
    else defaultValue
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) Some(get(conf, name, ""))
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else if (conf.contains(altName)) {
      Some(get(conf, altName, ""))
    } else defaultValue
  }

  def set(conf: SQLConf, value: T, useAltName: Boolean = false): Unit = {
    if (useAltName) {
      conf.setConfString(altName, configEntry.stringConverter(value))
    } else {
      conf.setConf[T](configEntry.entry.asInstanceOf[ConfigEntry[T]], value)
    }
  }

  def remove(conf: SQLConf, useAltName: Boolean = false): Unit = {
    conf.unsetConf(if (useAltName) altName else name)
  }
}

class DefaultPlanner(val snappySession: SnappySession, conf: SQLConf,
    extraStrategies: Seq[Strategy])
    extends SparkPlanner(snappySession.sparkContext, conf, extraStrategies)
        with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  private val storeOptimizedRules: Seq[Strategy] =
    Seq(StoreDataSourceStrategy, SnappyAggregation, LocalJoinStrategies)

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies,
      StoreStrategy, StreamQueryStrategy) ++
        storeOptimizedRules ++
        super.strategies
}

private[sql] final class PreprocessTableInsertOrPut(conf: SQLConf)
    extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) =>
          val br = ir.asInstanceOf[BaseRelation]
          val relation = LogicalRelation(br,
            l.expectedOutputAttributes, l.metastoreTableIdentifier)
          castAndRenameChildOutput(i.copy(table = relation),
            relation.output, br, null, child)
        case None =>
          throw new AnalysisException(s"$l requires that the query in the " +
              "SELECT clause of the INSERT INTO/OVERWRITE statement " +
              "generates the same number of columns as its schema.")
      }

    // Check for PUT
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(ir: RowInsertableRelation, _, _) =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          val expectedOutput = l.output
          if (expectedOutput.size != child.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutput(p, expectedOutput, ir, l, child)

        case _ => p
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, _, child, _, _)
      if table.resolved && child.resolved => table match {
      case relation: CatalogRelation =>
        val metadata = relation.catalogTable
        preProcess(i, relation = null, metadata.identifier.quotedString,
          metadata.partitionColumnNames)
      case LogicalRelation(h: HadoopFsRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preProcess(i, h, tblName, h.partitionSchema.map(_.name))
      case LogicalRelation(ir: InsertableRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preProcess(i, ir, tblName, Nil)
      case _ => i
    }
  }

  private def preProcess(
      insert: InsertIntoTable,
      relation: BaseRelation,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    val expectedColumns = insert.expectedColumns
    val child = insert.child
    if (expectedColumns.isDefined && expectedColumns.get.length != child.schema.length) {
      throw new AnalysisException(
        s"Cannot insert into table $tblName because the number of columns are different: " +
            s"need ${expectedColumns.get.length} columns, " +
            s"but query has ${child.schema.length} columns.")
    }

    if (insert.partition.nonEmpty) {
      // the query's partitioning must match the table's partitioning
      // this is set for queries like: insert into ... partition (one = "a", two = <expr>)
      val samePartitionColumns =
      if (conf.caseSensitiveAnalysis) {
        insert.partition.keySet == partColNames.toSet
      } else {
        insert.partition.keySet.map(_.toLowerCase) == partColNames.map(_.toLowerCase).toSet
      }
      if (!samePartitionColumns) {
        throw new AnalysisException(
          s"""
             |Requested partitioning does not match the table $tblName:
             |Requested partitions: ${insert.partition.keys.mkString(",")}
             |Table partitions: ${partColNames.mkString(",")}
           """.stripMargin)
      }
      expectedColumns.map(castAndRenameChildOutput(insert, _, relation, null,
        child)).getOrElse(insert)
    } else {
      // All partition columns are dynamic because because the InsertIntoTable
      // command does not explicitly specify partitioning columns.
      expectedColumns.map(castAndRenameChildOutput(insert, _, relation, null,
        child)).getOrElse(insert).copy(partition = partColNames
          .map(_ -> None).toMap)
    }
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  // TODO: do we really need to rename?
  def castAndRenameChildOutput[T <: LogicalPlan](
      plan: T,
      expectedOutput: Seq[Attribute],
      relation: BaseRelation,
      newRelation: LogicalRelation,
      child: LogicalPlan): T = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name) {
          actual
        } else {
          // avoid unnecessary copy+cast when inserting DECIMAL types
          // into column table
          actual.dataType match {
            case _: DecimalType
              if expected.dataType.isInstanceOf[DecimalType] &&
                  relation.isInstanceOf[PlanInsertableRelation] => actual
            case _ => Alias(Cast(actual, expected.dataType), expected.name)()
          }
        }
    }

    if (newChildOutput == child.output) {
      plan match {
        case p: PutIntoTable => p.copy(table = newRelation).asInstanceOf[T]
        case _: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case i: InsertIntoTable => i.copy(child = Project(newChildOutput,
        child)).asInstanceOf[T]
    }
  }
}

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(LogicalRelation(t: RowPutRelation, _, _), query) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }

      case PutIntoTable(table, _) =>
        throw Utils.analysisException(s"$table does not allow puts.")

      case _ => // OK
    }
  }
}
