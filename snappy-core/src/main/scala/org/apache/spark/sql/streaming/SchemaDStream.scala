package org.apache.spark.sql.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.{ExplainCommand, QueryExecution, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
  * A SQL based DStream with support for schema/Product
  * This class offers the ability to manipulate SQL query on DStreams
  * It is similar to SchemaRDD, which offers the similar functions
  * Internally, RDD of each batch duration is treated as a small
  * table and CQs are evaluated on those small tables
  *
  * @param streamingSnappy
  * @param queryExecution
  *
  */
final class SchemaDStream(
                           @transient val streamingSnappy: StreamingSnappyContext,
                           @transient val queryExecution: QueryExecution)
  extends DStream[Row](streamingSnappy.streamingContext) {

  def this(streamingSnappy: StreamingSnappyContext, logicalPlan: LogicalPlan) =
    this(streamingSnappy, streamingSnappy.executePlan(logicalPlan))

  /** Returns the schema of this SchemaDStream (represented by
    * a [[StructType]]). */
  def schema: StructType = queryExecution.analyzed.schema

  /** List of parent DStreams on which this DStream depends on */
  override def dependencies: List[DStream[InternalRow]] = parentStreams.toList

  def registerAsTable(tableName: String): Unit = {
    streamingSnappy.registerStreamAsTable(tableName, this)
  }

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
    // Set the valid batch duration for this rule to get
    // correct RDD in DStream of this batch duration
    StreamHelper.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan
    // to specific RDD logic plan.
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    Some(queryExecution.executedPlan.execute().map(converter(_).asInstanceOf[Row]))
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.stream :: Nil
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
    val sds: SchemaDStream = new SchemaDStream(streamingSnappy, explain)
    sds.queryExecution.executedPlan.executeCollect().map {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
    * Returns all column names as an array.
    */
  def columns: Array[String] = schema.fields.map(_.name)

  def printSchema(): Unit = {
    // scalastyle:off println
    println(schema.treeString)
    // scalastyle:on println
  }
}
