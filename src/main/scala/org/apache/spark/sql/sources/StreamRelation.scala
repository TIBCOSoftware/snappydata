package org.apache.spark.sql.sources


import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Strategy}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hemantb on 5/13/15.
 */
case class StreamRelation[T](
                              dStream: DStream[T],
                              streamschema: StructType)(
                              @transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {
  override def schema: StructType = streamschema

  override def buildScan(): RDD[Row] =
    throw new IllegalAccessException("Take it easy boy!! It's a prototype. ")

}

case class InMemoryAppendableRelation(
                              cacheSchema: StructType)(
                              @transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {
  override def schema: StructType = cacheSchema

  override def buildScan(): RDD[Row] =
    throw new IllegalAccessException("Take it easy boy!! It's a prototype. ")

}


private[sql] class DefaultSource extends SchemaRelationProvider {


  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     schema: StructType): BaseRelation = {


    StreamingCtxt.sQLContext = Some(sqlContext)

    // Create a dstream here based on the parameters passed as part of create stream
    val dstream = StreamingCtxt.streamingContext.textFileStream("/hemantb1/snappy/data/")

    new StreamRelation(dstream, schema)(sqlContext)

  }
}

// this will go in SnappyContext
object StreamingCtxt {
  var sQLContext: Option[SQLContext] = None

  lazy val streamingContext: StreamingContext = {
    val ctxt = sQLContext.getOrElse(throw new IllegalStateException())
    new StreamingContext(ctxt.sparkContext, Seconds(1))
  }

}

object StreamStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateStreamUsing(streamName, userColumns, options) =>
      ExecutedCommand(
        CreateStreamTable(streamName, userColumns, options)) :: Nil
    case CreateSampledTableUsing(streamName, options) =>
      ExecutedCommand(
        CreateSampledTable(streamName, options)) :: Nil
    case _ => Nil
  }
}


private[sql] case class CreateStreamUsing(streamName: String,
                                          userColumns: Option[StructType],
                                          options: Map[String, String]) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = ???

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = ???
}

private[sql] case class CreateSampledTableUsing(streamName: String,
                                                options: Map[String, String]) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = ???

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = ???
}

private[sql] case class CreateStreamTable(
                                           streamName: String,
                                           userColumns: Option[StructType],
                                           options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val resolved = ResolvedDataSource(sqlContext, userColumns, Array.empty[String], "stream", options)
    sqlContext.registerDataFrameAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), streamName)
    Seq.empty
  }
}

private[sql] case class CreateSampledTable(
                                            sampledTableName: String,
                                            options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val tableName: Option[String] = options.get("table")
    val table: DataFrame = sqlContext.table(tableName.getOrElse(
      throw new IllegalStateException("Sampled table should define base table name ")))
    val streamrelation = table.queryExecution.optimizedPlan match {
      case LogicalRelation(streamrelation) => streamrelation match {
        case sr: StreamRelation[_] => (sr.dStream, sr.schema)
        case _ => throw new IllegalStateException("StreamRelation was expected")
      }
      case _ => throw new IllegalStateException("StreamRelation was expected")
    }

    applySamplingTransformations(streamrelation, options)

    // registerSampleTableInCatalog()

    // creating a dummy dataframe for now.
    sqlContext.registerDataFrameAsTable(
      DataFrame(sqlContext, table.queryExecution.optimizedPlan), sampledTableName)

    Seq.empty
  }

  private def applySamplingTransformations(streamrelation: Pair[DStream[_], StructType],
                                           options: Map[String, String]): Unit = {
    val schema = streamrelation._2
    val dStream = streamrelation._1
    dStream.foreachRDD(rdd => {
      //
      rdd.foreach(row => {

        // Sample 50% of the rows.
        if (row.hashCode() % 2 == 0) {
          insertInCacheTable(row, schema);
        }
      })
    })
  }

  private def insertInCacheTable(row: Any, schema: StructType) {
    System.out.println(row)
    System.out.println(schema)

  }
}