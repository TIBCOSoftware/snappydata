package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.{ParserDialect, SqlParser}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Snappy SQL extensions. Includes:
 *
 * Stratified sampled tables:
 * 1) ERROR ESTIMATE AVG: error estimate for mean of a column/expression
 * 2) ERROR ESTIMATE SUM: error estimate for sum of a column/expression
 */
private[sql] class SnappyParser extends SqlParser {

  protected val ERROR = Keyword("ERROR")
  protected val ESTIMATE = Keyword("ESTIMATE")

  protected val defaultConfidence = 0.75

  override protected lazy val function = functionDef |
      ERROR ~> ESTIMATE ~> ("(" ~> floatLit <~ ")").? ~
          (AVG | SUM) ~ ("(" ~> expression <~ ")") ^^ {
        case c ~ op ~ exp =>
          val confidence = c.map(_.toDouble).getOrElse(defaultConfidence)
          val aggType = ErrorAggregate.withName(
            op.toUpperCase(java.util.Locale.ENGLISH))
          ErrorEstimateAggregate(exp, confidence, null, c.isEmpty, aggType)
      }
}

/** Snappy dialect adds SnappyParser additions to the standard "sql" dialect */
private[sql] class SnappyParserDialect extends ParserDialect {

  @transient
  protected val sqlParser = new SnappyParser

  override def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}

/**
 * Snappy DDL extensions for streaming and sampling.
 */
private[sql] class SnappyDDLParser(parseQuery: String => LogicalPlan)
    extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | describeTable | refreshTable |
        createStream | createSampled | strmctxt

  protected val STREAM = Keyword("STREAM")
  protected val SAMPLED = Keyword("SAMPLED")
  protected val STRM = Keyword("STREAMING")
  protected val CTXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")


  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> (STREAM ~> (TABLE ~> ident)) ~
        tableCols.? ~ (OPTIONS ~> options)) ^^ {
      case streamname ~ cols ~ opts =>
        val userColumns = cols.flatMap(fields => Some(StructType(fields)))
        CreateStream(streamname, userColumns, new CaseInsensitiveMap(opts))
    }

  protected lazy val createSampled: Parser[LogicalPlan] =
    (CREATE ~> (SAMPLED ~> (TABLE ~> ident)) ~
        (OPTIONS ~> options)) ^^ {
      case samplename ~ opts =>
        CreateSampledTable(samplename, new CaseInsensitiveMap(opts))
    }

  protected lazy val strmctxt: Parser[LogicalPlan] =
    (STRM ~> CTXT ~>
        (INIT ^^^ 0 | START ^^^ 1 | STOP ^^^ 2) ~ numericLit.?) ^^ {
      case action ~ batchInterval =>
        if (batchInterval.isDefined)
          StreamingCtxtActions(action, Some(batchInterval.get.toInt))
        else
          StreamingCtxtActions(action, None)

    }
}

private[sql] case class CreateStream(streamName: String,
    userColumns: Option[StructType],
    options: Map[String, String])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class CreateSampledTable(streamName: String,
    options: Map[String, String])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class StreamingCtxtActions(action: Int,
    batchInterval: Option[Int])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private object StreamingCtxtHolder {

  private val atomicContext = new AtomicReference[StreamingContext]()

  def streamingContext = atomicContext.get()

  def apply(sparkCtxt: SparkContext,
      duration: Int): StreamingContext = {
    val context = atomicContext.get
    if (context != null) {
      context
    }
    else {
      atomicContext.compareAndSet(null,
        new StreamingContext(sparkCtxt, Seconds(duration)))
      atomicContext.get
    }
  }
}
