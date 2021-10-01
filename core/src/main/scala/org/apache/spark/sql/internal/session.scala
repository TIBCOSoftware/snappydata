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

package org.apache.spark.sql.internal

import java.util.Properties
import java.util.function.BiConsumer

import scala.reflect.{ClassTag, classTag}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.{Attribute => GAttr}
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, TypedConfigBuilder}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Contains, EndsWith, EqualTo, Expression, Like, Literal, StartsWith, TokenLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, InsertIntoTable, Join, LocalRelation, LogicalPlan, OverwriteOptions, Project, UnaryNode, Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation, PreprocessTableInsertion}
import org.apache.spark.sql.execution.{SecurityUtils, datasources}
import org.apache.spark.sql.hive.SnappySessionState
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DecimalType, StringType}
import org.apache.spark.sql.{AnalysisException, SaveMode, SnappyContext, SnappyParser, SnappySession}
import org.apache.spark.unsafe.types.UTF8String

// Misc helper classes for session handling

class SnappyConf(@transient val session: SnappySession)
    extends SQLConf with Serializable {

  /** Pool to be used for the execution of queries from this session */
  @volatile private[this] var schedulerPool: String = Property.SchedulerPool.defaultValue.get

  /** If shuffle partitions is set by [[setExecutionShufflePartitions]]. */
  @volatile private[this] var executionShufflePartitions: Int = _

  /**
   * Records the number of shuffle partitions to be used determined on runtime
   * from available cores on the system. A value < 0 indicates that it was set
   * explicitly by user and should not use a dynamic value. A value of 0 indicates
   * that current plan execution did not touch it so reset need not touch it.
   */
  @volatile private[this] var dynamicShufflePartitions: Int = _

  /**
   * Set if a task implicitly sets the "spark.task.cpus" property based
   * on executorCores/physicalCores. This will be -1 if set explicitly
   * on the session so that it doesn't get reset at the end of task execution.
   */
  @volatile private[this] var dynamicCpusPerTask: Int = _

  SQLConf.SHUFFLE_PARTITIONS.defaultValue match {
    case Some(d) if (session ne null) && super.numShufflePartitions == d =>
      dynamicShufflePartitions = coreCountForShuffle
    case None if session ne null =>
      dynamicShufflePartitions = coreCountForShuffle
    case _ =>
      executionShufflePartitions = -1
      dynamicShufflePartitions = -1
  }

  resetOverrides()

  private def resetOverrides(): Unit = {
    val overrideConfs = session.overrideConfs
    if (overrideConfs.nonEmpty) {
      overrideConfs.foreach(p => setConfString(p._1, p._2))
    }
  }

  private def coreCountForShuffle: Int = {
    val count = SnappyContext.totalPhysicalCoreCount.get()
    if (count > 0 || (session eq null)) math.min(super.numShufflePartitions, count)
    else math.min(super.numShufflePartitions, session.sparkContext.defaultParallelism)
  }

  private lazy val allDefinedKeys = {
    val map = new CaseInsensitiveMutableHashMap[String](Map.empty)
    getAllDefinedConfs.foreach(e => map.put(e._1, e._1))
    map
  }

  private def keyUpdateActions(key: String, value: Option[Any],
      doSet: Boolean, search: Boolean = true): String = key match {
    // clear plan cache when some size related key that effects plans changes
    case SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key |
         Property.HashJoinSize.name |
         Property.HashAggregateSize.name |
         Property.ForceLinkPartitionsToBuckets.name |
         Property.UseOptimzedHashAggregate.name |
         Property.TestCodeSplitFunctionParamsSizeInSHA.name |
         Property.TestCodeSplitThresholdInSHA.name |
         Property.UseOptimizedHashAggregateForSingleKey.name |
         Property.TestExplodeComplexDataTypeInSHA.name =>
      session.clearPlanCache()
      key

    case SQLConf.SHUFFLE_PARTITIONS.key =>
      // stop dynamic determination of shuffle partitions
      if (doSet) {
        executionShufflePartitions = -1
        dynamicShufflePartitions = -1
      } else {
        dynamicShufflePartitions = coreCountForShuffle
      }
      session.clearPlanCache()
      key

    case Property.SchedulerPool.name =>
      schedulerPool = value match {
        case None => Property.SchedulerPool.defaultValue.get
        case Some(pool: String) if session.sparkContext.getPoolForName(pool).isDefined => pool
        case Some(pool) => throw new IllegalArgumentException(s"Invalid Pool $pool")
      }
      key

    case Property.PartitionPruning.name =>
      value match {
        case Some(b) => session.partitionPruning = b.toString.toBoolean
        case None => session.partitionPruning = Property.PartitionPruning.defaultValue.get
      }
      session.clearPlanCache()
      key

    case Property.PlanCaching.name =>
      value match {
        case Some(boolVal) =>
          val b = boolVal.toString.toBoolean
          if (b) {
            session.clearPlanCache()
          }
          session.planCaching = b
        case None => session.planCaching = Property.PlanCaching.defaultValue.get
      }
      key

    case Property.Tokenize.name =>
      value match {
        case Some(boolVal) => session.tokenize = boolVal.toString.toBoolean
        case None => session.tokenize = Property.Tokenize.defaultValue.get
      }
      session.clearPlanCache()
      key

    case Property.DisableHashJoin.name =>
      value match {
        case Some(boolVal) => session.disableHashJoin = boolVal.toString.toBoolean
        case None => session.disableHashJoin = Property.DisableHashJoin.defaultValue.get
      }
      session.clearPlanCache()
      key

    case CATALOG_IMPLEMENTATION.key if !session.hiveInitializing =>
      val newValue = value match {
        case Some(v) => session.isHiveSupportEnabled(v.toString)
        case None => CATALOG_IMPLEMENTATION.defaultValueString == "hive"
      }
      // initialize hive session upfront
      if (newValue) {
        session.hiveInitializing = true
        assert(session.sessionState.hiveSession ne null)
        session.hiveInitializing = false
      }
      session.enableHiveSupport = newValue
      key

    case Property.HiveCompatibility.name =>
      value match {
        case Some(level) => Utils.toLowerCase(level.toString) match {
          case "default" | "spark" | "full" =>
          case _ => throw new IllegalArgumentException(
            s"Unexpected value '$level' for ${Property.HiveCompatibility.name}. " +
                "Allowed values are: default, spark and full")
        }
        case None =>
      }
      key

    case SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key =>
      session.clearPlanCache()
      key

    case Constant.TRIGGER_AUTHENTICATION => value match {
      case Some(boolVal) if boolVal.toString.toBoolean =>
        if ((Misc.getMemStoreBootingNoThrow ne null) && Misc.isSecurityEnabled) {
          SecurityUtils.checkCredentials(getConfString(
            com.pivotal.gemfirexd.Attribute.USERNAME_ATTR),
            getConfString(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)) match {
            case None => // success
            case Some(failure) =>
              throw Util.generateCsSQLException(SQLState.NET_CONNECT_AUTH_FAILED, failure)
          }
        }
        key
      case _ => key
    }

    case Constant.CPUS_PER_TASK_PROP => value match {
      case Some(intVal) =>
        val intStr = intVal.toString
        val numCpus = intStr.toInt
        if (numCpus < 1) {
          throw new IllegalArgumentException(s"Value for ${Constant.CPUS_PER_TASK_PROP} " +
              s"should be >= 1 but was $intVal")
        }
        session.sparkContext.setLocalProperty(Constant.CPUS_PER_TASK_PROP, intStr)
        dynamicCpusPerTask = -1
        key
      case _ =>
        session.sparkContext.setLocalProperty(Constant.CPUS_PER_TASK_PROP, null)
        dynamicCpusPerTask = 0
        key
    }

    case GAttr.USERNAME_ATTR | GAttr.USERNAME_ALT_ATTR | GAttr.PASSWORD_ATTR =>
      if (session.catalogInitialized) session.sessionCatalog.invalidateAll(sessionOnly = true)
      key

    case _ if key.startsWith("spark.sql.aqp.") =>
      session.clearPlanCache()
      key

    case _ =>
      // search case-insensitively for other keys if required
      if (search) {
        allDefinedKeys.get(key) match {
          case None => key
          case Some(k) =>
            // execute keyUpdateActions again since it might be one of the pre-defined ones
            keyUpdateActions(k, value, doSet, search = false)
            k
        }
      } else key
  }

  private def hiveConf: SQLConf = session.sessionState.hiveSession.sessionState.conf

  private[sql] def resetDefaults(): Unit = synchronized {
    if (session ne null) {
      if (executionShufflePartitions != -1) {
        executionShufflePartitions = 0
      }
      if (dynamicShufflePartitions != -1) {
        dynamicShufflePartitions = coreCountForShuffle
      }
      if (dynamicCpusPerTask > 0) {
        unsetConf(Constant.CPUS_PER_TASK_PROP)
      }
    }
  }

  private[sql] def setExecutionShufflePartitions(n: Int): Unit = synchronized {
    if (executionShufflePartitions != -1 && session != null) {
      executionShufflePartitions = math.max(n, executionShufflePartitions)
      logDebug(s"Set execution shuffle partitions to $executionShufflePartitions")
    }
  }

  private[sql] def setDynamicCpusPerTask(): Unit = synchronized {
    if (dynamicCpusPerTask != -1) {
      val sparkCores = session.sparkContext.defaultParallelism.toDouble
      // calculate minimum required heap assuming a block size of 128M
      val minRequiredHeap = 128.0 * 1024.0 * 1024.0 * sparkCores * 1.2
      val totalUsableHeap = SnappyContext.foldLeftBlockIds(0L)(_ + _.usableHeapBytes)
      // select bigger among (required heap / available) and (logical cores / physical)
      val cpusPerTask0 = math.max(minRequiredHeap / totalUsableHeap,
        sparkCores / SnappyContext.totalPhysicalCoreCount.get())
      // keep a reasonable upper-limit so tasks can at least be scheduled:
      //   used below is average logical cores / 2
      val cpusPerTask = math.max(1, math.ceil(math.min(sparkCores /
          (2 * SnappyContext.numExecutors), cpusPerTask0)).toInt)
      setConfString(Constant.CPUS_PER_TASK_PROP, cpusPerTask.toString)
      dynamicCpusPerTask = cpusPerTask
      logDebug(s"Set dynamic ${Constant.CPUS_PER_TASK_PROP} to $cpusPerTask")
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

  def activeSchedulerPool: String = schedulerPool

  override def setConfString(key: String, value: String): Unit = {
    val rkey = keyUpdateActions(key, Some(value), doSet = true)
    super.setConfString(rkey, value)
    if (session.enableHiveSupport) hiveConf.setConfString(rkey, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    keyUpdateActions(entry.key, Some(value), doSet = true, search = false)
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    entry.defaultValue match {
      case Some(_) => super.setConf(entry, value)
      case None => super.setConf(entry.asInstanceOf[ConfigEntry[Option[T]]], Some(value))
    }
    if (session.enableHiveSupport) hiveConf.setConf(entry, value)
  }

  override def setConf(props: Properties): Unit = {
    super.setConf(props)
    if (session.enableHiveSupport) hiveConf.setConf(props)
  }

  override def unsetConf(key: String): Unit = {
    val rkey = keyUpdateActions(key, None, doSet = false)
    super.unsetConf(rkey)
    if (session.enableHiveSupport) hiveConf.unsetConf(rkey)
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    keyUpdateActions(entry.key, None, doSet = false, search = false)
    super.unsetConf(entry)
    if (session.enableHiveSupport) hiveConf.unsetConf(entry)
  }

  def foreach(f: (String, String) => Unit): Unit = settings.synchronized {
    settings.forEach(new BiConsumer[String, String] {
      override def accept(k: String, v: String): Unit = f(k, v)
    })
  }

  override def clear(): Unit = {
    super.clear()
    resetOverrides()
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

  private def get(conf: SparkConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.get(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.get(name, defaultValue)).get
    }
  }

  def get(conf: SparkConf): T = if (altName == null) {
    get(conf, name, configEntry.defaultValueString)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, name, configEntry.defaultValueString)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def get(properties: Properties): T = {
    val propertyValue = getProperty(properties)
    if (propertyValue ne null) configEntry.valueConverter[T](propertyValue)
    else defaultValue.get
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
      case Some(_) => configEntry.valueConverter[T](
        conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.getConfString(name, defaultValue)).get
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

/**
 * This rule checks the schema for insert/update/put/delete and casts the data if required.
 * It also turns a single row LocalRelation into a set of expressions to allow predicate push-down.
 */
private[sql] final class PreprocessTable(state: SnappySessionState) extends Rule[LogicalPlan] {

  private def conf: SQLConf = state.conf

  def apply(plan: LogicalPlan): LogicalPlan = {
    val hasJoin = plan.find(_.isInstanceOf[Join]).isDefined
    doApply(plan, hasJoin)
  }

  private def doApply(plan: LogicalPlan, hasJoin: Boolean): LogicalPlan = plan transformDown {

    // Add dbtable property for create table. While other routes can add it in
    // SnappySession.createTable, the DataFrameWriter path needs to be handled here.
    case c@CreateTable(tableDesc, mode, queryOpt) if DDLUtils.isDatasourceTable(tableDesc) =>
      val tableIdent = state.catalog.resolveTableIdentifier(tableDesc.identifier)
      val provider = tableDesc.provider.get
      val isBuiltin = SnappyContext.isBuiltInProvider(provider) ||
          CatalogObjectType.isGemFireProvider(provider)
      // treat saveAsTable with mode=Append as insertInto for builtin tables and "normal" cases
      // where no explicit bucket/partitioning has been specified
      if (mode == SaveMode.Append && queryOpt.isDefined && (isBuiltin ||
          (tableDesc.bucketSpec.isEmpty && tableDesc.partitionColumnNames.isEmpty)) &&
          state.catalog.tableExists(tableIdent)) {
        new Insert(table = UnresolvedRelation(tableIdent),
          partition = Map.empty, child = queryOpt.get,
          overwrite = OverwriteOptions(enabled = false), ifNotExists = false)
      } else if (isBuiltin) {
        val tableName = tableIdent.unquotedString
        // dependent tables are stored as comma-separated so don't allow comma in table name
        if (tableName.indexOf(',') != -1) {
          throw new AnalysisException(s"Table '$tableName' cannot contain comma in its name")
        }
        var newOptions = tableDesc.storage.properties +
            (SnappyExternalCatalog.DBTABLE_PROPERTY -> tableName)
        if (CatalogObjectType.isColumnTable(SnappyContext.getProviderType(provider))) {
          // add default batchSize and maxDeltaRows options for column tables
          val parameters = new ExternalStoreUtils.CaseInsensitiveMutableHashMap[String](
            tableDesc.storage.properties)
          if (!parameters.contains(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS)) {
            newOptions += (ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS ->
                ExternalStoreUtils.defaultColumnMaxDeltaRows(state.snappySession).toString)
          }
          if (!parameters.contains(ExternalStoreUtils.COLUMN_BATCH_SIZE)) {
            newOptions += (ExternalStoreUtils.COLUMN_BATCH_SIZE ->
                ExternalStoreUtils.defaultColumnBatchSize(state.snappySession).toString)
          }
        }
        c.copy(tableDesc.copy(storage = tableDesc.storage.copy(properties = newOptions)))
      } else c

    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) if r eq ir => i
        case Some(ir) =>
          val br = ir.asInstanceOf[BaseRelation]
          val relation = LogicalRelation(br, catalogTable = l.catalogTable)
          castAndRenameChildOutputForPut(i.copy(table = relation),
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
          castAndRenameChildOutputForPut(p, expectedOutput, ir, l, child)

        case _ => p
      }

    // Check for DELETE
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case d@DeleteFromTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(dr: MutableRelation, _, _) =>

          val keyColumns = dr.getPrimaryKeyColumns(state.snappySession)
          val childOutput = keyColumns.map(col =>
            child.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"$l requires that the query in the " +
                  "WHERE clause of the DELETE FROM statement " +
                  s"must have all the key column(s) ${keyColumns.mkString(",")} but found " +
                  s"${child.output.mkString(",")} instead.")
            })

          val expectedOutput = keyColumns.map(col =>
            l.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"The target table must contain all the" +
                  s" key column(s) ${keyColumns.mkString(",")}. " +
                  s"Actual schema: ${l.output.mkString(",")}")
            })

          castAndRenameChildOutputForPut(d, expectedOutput, dr, l, Project(childOutput, child))

        case _ => d
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, _, child, _, _)
      if table.resolved && child.resolved => PreprocessTableInsertion(conf).apply(i)

    case lr: LocalRelation if hasJoin && lr.data.length == 1 && lr.output.nonEmpty =>
      val row = lr.data.head
      Project(lr.output.zipWithIndex.map { case (a, i) =>
        Alias(new TokenLiteral(row.get(i, a.dataType), a.dataType), a.name)(
          a.exprId, a.qualifier, isGenerated = a.isGenerated)
      }, LocalRelation(Nil, lr.data))
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  def castAndRenameChildOutputForPut[T <: LogicalPlan](
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
        case d: DeleteFromTable => d.copy(table = newRelation).asInstanceOf[T]
        case _: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case d: DeleteFromTable => d.copy(table = newRelation,
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

private[sql] case class ConditionalPreWriteCheck(sparkPreWriteCheck: datasources.PreWriteCheck)
    extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan match {
      case PutIntoColumnTable(_, _, _) => // Do nothing
      case _ => sparkPreWriteCheck.apply(plan)
    }
  }
}

/**
 * Deals with any escape characters in the LIKE pattern in optimization.
 * Does not deal with startsAndEndsWith equivalent of Spark's LikeSimplification
 * so 'a%b' kind of pattern with additional escaped chars will not be optimized.
 */
object LikeEscapeSimplification {

  private def addTokenizedLiteral(parser: SnappyParser, s: String): Expression = {
    if (parser ne null) parser.addTokenizedLiteral(UTF8String.fromString(s), StringType)
    else Literal(UTF8String.fromString(s), StringType)
  }

  def simplifyLike(parser: SnappyParser, expr: Expression,
      left: Expression, pattern: String): Expression = {
    val len_1 = pattern.length - 1
    if (len_1 == -1) return EqualTo(left, addTokenizedLiteral(parser, ""))
    val str = new StringBuilder(pattern.length)
    var wildCardStart = false
    var i = 0
    while (i < len_1) {
      pattern.charAt(i) match {
        case '\\' =>
          val c = pattern.charAt(i + 1)
          c match {
            case '_' | '%' | '\\' => // literal char
            case _ => return expr
          }
          str.append(c)
          // if next character is last one then it is literal
          if (i == len_1 - 1) {
            if (wildCardStart) return EndsWith(left, addTokenizedLiteral(parser, str.toString))
            else return EqualTo(left, addTokenizedLiteral(parser, str.toString))
          }
          i += 1
        case '%' if i == 0 => wildCardStart = true
        case '%' | '_' => return expr // wildcards in middle are left as is
        case c => str.append(c)
      }
      i += 1
    }
    pattern.charAt(len_1) match {
      case '%' =>
        if (wildCardStart) Contains(left, addTokenizedLiteral(parser, str.toString))
        else StartsWith(left, addTokenizedLiteral(parser, str.toString))
      case '_' | '\\' => expr
      case c =>
        str.append(c)
        if (wildCardStart) EndsWith(left, addTokenizedLiteral(parser, str.toString))
        else EqualTo(left, addTokenizedLiteral(parser, str.toString))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case l@Like(left, Literal(pattern, StringType)) =>
      simplifyLike(null, l, left, pattern.toString)
  }
}

case class MarkerForCreateTableAsSelect(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class BypassRowLevelSecurity(child: LogicalFilter) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * Wrap plan-specific query hints (like joinType). This extends Spark's BroadcastHint
 * so that filters/projections etc can be pushed below this by optimizer.
 */
class LogicalPlanWithHints(_child: LogicalPlan, val hints: Map[String, String])
    extends BroadcastHint(_child) {

  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => child
    case 1 => hints
  }

  override def simpleString: String =
    s"LogicalPlanWithHints[hints = $hints; child = ${child.simpleString}]"
}
