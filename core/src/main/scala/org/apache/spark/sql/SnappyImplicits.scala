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
package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.sources.{DeleteFromTable, PutIntoTable}
import org.apache.spark.{Partition, TaskContext}

/**
 * Implicit conversions used by Snappy.
 */
// scalastyle:off
object snappy extends Serializable {
// scalastyle:on

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyDataFrameOperations = {
    df.sparkSession match {
      case sc: SnappySession => SnappyDataFrameOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def samplingOperationsOnDataFrame(df: DataFrame): SampleDataFrame = {
    df.sparkSession match {
      case sc: SnappySession =>
        val plan = snappy.unwrapSubquery(df.logicalPlan)
        if (sc.snappyContextFunctions.isStratifiedSample(plan)) {
          new SampleDataFrame(sc, plan)
        } else {
          throw new AnalysisException("Stratified sampling " +
              "operations require stratifiedSample plan and not " +
              s"${plan.getClass.getSimpleName}")
        }
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def convertToAQPFrame(df: DataFrame): AQPDataFrame = {
    AQPDataFrame(df.sparkSession.asInstanceOf[SnappySession], df.queryExecution)
  }

  def unwrapSubquery(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s: SubqueryAlias => unwrapSubquery(s.child)
      case _ => plan
    }
  }

  implicit class RDDExtensions[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
     * Return a new RDD by applying a function to all elements of this RDD.
     *
     * This variant also preserves the preferred locations of parent RDD.
     */
    def mapPreserve[U: ClassTag](f: T => U): RDD[U] = rdd.withScope {
      val cleanF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD[U, T](rdd, (_, _, iter) => iter.map(cleanF))
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD.
     *
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserve[U: ClassTag](
        f: Iterator[T] => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd, (_, _,
          itr: Iterator[T]) => cleanedF(itr), preservesPartitioning)
    }

    /**
     * Like [[mapPartitionsPreserve]] but also skips closure cleaning like
     * Spark's mapPartitionsInternal.
     */
    private[spark] def mapPartitionsPreserveInternal[U: ClassTag](
        f: Iterator[T] => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      new MapPartitionsPreserveRDD(rdd, (_, _, itr: Iterator[T]) => f(itr),
        preservesPartitioning)
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD,
     * while tracking the index of the original partition.
     *
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserveWithIndex[U: ClassTag](
        f: (Int, Iterator[T]) => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd, (_, part: Partition,
          itr: Iterator[T]) => cleanedF(part.index, itr), preservesPartitioning)
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD.
     *
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserveWithPartition[U: ClassTag](
        f: (TaskContext, Partition, Iterator[T]) => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd, (context: TaskContext, part: Partition,
          itr: Iterator[T]) => cleanedF(context, part, itr),
        preservesPartitioning)
    }

    def mapPartitionsWithIndexPreserveLocations[U: ClassTag](
        f: (Int, Iterator[T]) => Iterator[U],
        p: Int => Seq[String],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new PreserveLocationsRDD(rdd,
        (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
        preservesPartitioning, p)
    }
  }

  /**
   * Unfortunately everything including DataFrame is private in
   * DataFrameWriter so have to use reflection.
   */
  private[this] val dfField = classOf[DataFrameWriter[_]].getDeclaredFields.find {
    f => f.getName == "df" || f.getName.endsWith("$df")
  }.getOrElse(sys.error("Failed to obtain DataFrame from DataFrameWriter"))

  private[this] val parColsMethod = classOf[DataFrameWriter[_]]
      .getDeclaredMethods.find(_.getName.contains("$normalizedParCols"))
      .getOrElse(sys.error("Failed to obtain method  " +
          "normalizedParCols from DataFrameWriter"))

  dfField.setAccessible(true)
  parColsMethod.setAccessible(true)

  implicit class DataFrameWriterExtensions(writer: DataFrameWriter[_])
      extends Serializable {

    /**
     * "Puts" the content of the [[DataFrame]] to the specified table. It
     * requires that the schema of the [[DataFrame]] is the same as the schema
     * of the table. If some rows are already present then they are updated.
     *
     * This ignores all SaveMode.
     */
    def putInto(tableName: String): Unit = {
      val df: DataFrame = dfField.get(writer).asInstanceOf[DataFrame]
      val session = df.sparkSession match {
        case sc: SnappySession => sc
        case _ => sys.error("Expected a SnappyContext for putInto operation")
      }
      val normalizedParCols = parColsMethod.invoke(writer)
          .asInstanceOf[Option[Seq[String]]]
      // A partitioned relation's schema can be different from the input
      // logicalPlan, since partition columns are all moved after data columns.
      // We Project to adjust the ordering.
      // TODO: this belongs to the analyzer.
      val input = normalizedParCols.map { parCols =>
        val (inputPartCols, inputDataCols) = df.logicalPlan.output.partition {
          attr => parCols.contains(attr.name)
        }
        Project(inputDataCols ++ inputPartCols, df.logicalPlan)
      }.getOrElse(df.logicalPlan)

      df.sparkSession.sessionState.executePlan(PutIntoTable(UnresolvedRelation(
        session.tableIdentifier(tableName)), input)).executedPlan.executeCollect()
    }

    def deleteFrom(tableName: String): Unit = {
      val df: DataFrame = dfField.get(writer).asInstanceOf[DataFrame]
      val session = df.sparkSession match {
        case sc: SnappySession => sc
        case _ => sys.error("Expected a SnappyContext for deleteFrom operation")
      }

      df.sparkSession.sessionState.executePlan(DeleteFromTable(UnresolvedRelation(
        session.tableIdentifier(tableName)), df.logicalPlan)).executedPlan.executeCollect()
    }
  }

}

private[sql] case class SnappyDataFrameOperations(session: SnappySession,
    df: DataFrame) {


  /**
   * Creates stratified sampled data from given DataFrame
   * {{{
   *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
   * }}}
   */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(session, session.snappyContextFunctions.convertToStratifiedSample(
      options, session, df.logicalPlan))


  /**
   * Creates a DataFrame for given time instant that will be used when
   * inserting into top-K structures.
   *
   * @param time the time instant of the DataFrame as millis since epoch
   * @return
   */
  def withTime(time: Long): DataFrameWithTime =
    new DataFrameWithTime(session, df.logicalPlan, time)


  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToTempTableCache(tableName: String): Unit =
    session.appendToTempTableCache(df, tableName)
}
