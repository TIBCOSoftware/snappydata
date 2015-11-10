package org.apache.spark.sql.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
 * A SQL based DStream with support for schema/Product
 * This class offers the ability to manipulate SQL query on DStreams
 * It is similar to SchemaRDD, which offers the similar functions
 * Internally, RDD of each batch duration is treated as a small table and CQs are evaluated on those small tables
 * @param streamingSnappy
 * @param queryExecution
 *
 * Created by ymahajan on 25/09/15.
 */

final class SchemaDStream(
                           @transient val streamingSnappy: StreamingSnappyContext,
                           @transient val queryExecution: org.apache.spark.sql.execution.QueryExecution)
  extends DStream[Row](streamingSnappy.streamingContext) {

  def this(streamingSnappy: StreamingSnappyContext, logicalPlan: LogicalPlan) = this(streamingSnappy, streamingSnappy.executePlan(logicalPlan))

  /** Returns the schema of this SchemaDStream (represented by a [[StructType]]). */
  def schema: StructType = queryExecution.analyzed.schema

  /** List of parent DStreams on which this DStream depends on */
  override def dependencies = parentStreams.toList

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
    DStreamHelper.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan
    // to specific RDD logic plan.
    Some(queryExecution.executedPlan.execute().asInstanceOf[RDD[Row]])
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.stream :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    val streams = traverse(queryExecution.executedPlan)
    //assert (!streams.isEmpty, s"Input query and related plan ${queryExecution.executedPlan} is not a stream plan")
    streams
  }

  //TODO Yogesh Does it make sense to add following APIS
  //printSchema
  //explain
  //columns
}
