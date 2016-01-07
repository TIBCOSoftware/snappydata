package org.apache.spark.sql.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
  * A SQL based DStream with support for schema/Product
  * This class offers the ability to manipulate SQL query on DStreams
  * It is similar to SchemaRDD, which offers the similar functions
  * Internally, RDD of each batch duration is treated as a small
  * table and CQs are evaluated on those small tables
  * Some of the code and idea is borrowed from the project:
  * https://github.com/Intel-bigdata/spark-streamingsql
  * @param snsc
  * @param queryExecution
  *
  */
final class SchemaDStream(@transient val snsc: SnappyStreamingContext,
    @transient val queryExecution: QueryExecution)
    extends DStream[Row](snsc) {

  @transient private val snappyContext: SnappyContext = snsc.snappyContext

  @transient private val catalog = snsc.snappyContext.catalog

  def this(ssc: SnappyStreamingContext, logicalPlan: LogicalPlan) =
    this(ssc, ssc.snappyContext.executePlan(logicalPlan))

  def saveStream(sampleTab: Seq[String]): Unit = {
    snappyContext.saveStream(this, sampleTab, None)
  }

  /** Return a new DStream containing only the elements that satisfy a predicate. */
  override def filter(filterFunc: Row => Boolean): DStream[Row] = {
    super.filter(filterFunc)
  }

  /**
    * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
    * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
    */
  def foreachDataFrame(foreachFunc: DataFrame => Unit): Unit = {
    val func = (rdd: RDD[Row]) => {
      foreachFunc(snappyContext.createDataFrame(rdd, this.schema, needsConversion = true))
    }
    this.foreachRDD(func)
  }

  /**
    * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
    * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
    */
  def foreachDataFrame(foreachFunc: (DataFrame, Time) => Unit): Unit = {
    val func = (rdd: RDD[Row], time: Time) => {
      foreachFunc(snappyContext.createDataFrame(rdd, this.schema, needsConversion = true), time)
    }
    this.foreachRDD(func)
  }

  /** Registers this SchemaDStream as a table in the catalog. */
  def registerAsTable(tableName: String): Unit = {
    snsc.snappyContext.catalog.registerTable(
      snsc.snappyContext.catalog.newQualifiedTableName(tableName),
      logicalPlan)
  }

  /** Returns the schema of this SchemaDStream (represented by
    * a [[StructType]]). */
  def schema: StructType = queryExecution.analyzed.schema

  /** List of parent DStreams on which this DStream depends on */
  override def dependencies: List[DStream[InternalRow]] = parentStreams.toList

  /** Time interval after which the DStream generates a RDD */
  override def slideDuration: Duration = parentStreams.head.slideDuration

  @transient val logicalPlan: LogicalPlan = queryExecution.logical
  match {
    case _: InsertIntoTable =>
      throw new IllegalStateException(s"logical plan ${queryExecution.logical} " +
          s"is not supported currently")
    case _ => queryExecution.logical
  }

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[Row]] = {
    StreamHelper.setValidTime(validTime)
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    Some(queryExecution.executedPlan.execute().map(converter(_).asInstanceOf[Row]))
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.rowStream :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    val streams = traverse(queryExecution.executedPlan)
    streams
  }

  def explain(): Unit = explain(extended = false)

  /**
    * Explain the query to get logical plan as well as physical plan.
    */
  def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    val sds = new SchemaDStream(snsc, explain)
    sds.queryExecution.executedPlan.executeCollect().map {
      r => println(r.getString(0)) // scalastyle:ignore
    }
  }

  /**
    * Returns all column names as an array.
    */
  def columns: Array[String] = schema.fields.map(_.name)

  def printSchema(): Unit = {
    println(schema.treeString) // scalastyle:ignore
  }
}
