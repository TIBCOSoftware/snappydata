package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.{ParserDialect, SqlParser}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources._

import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.streaming.{StreamingContextState, Seconds, StreamingContext}


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
        createStream | createSampled | strmctxt | createExternalTable

  protected val STREAM = Keyword("STREAM")
  protected val SAMPLED = Keyword("SAMPLED")
  protected val STRM = Keyword("STREAMING")
  protected val CTXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")
  protected val EXTERNAL = Keyword("EXTERNAL")


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
  protected lazy val createExternalTable: Parser[LogicalPlan] =
    (CREATE ~> (EXTERNAL ~> (TABLE ~> ident)) ~
      externalTableInput ~ (USING ~> className) ~ (OPTIONS ~> options)) ^^ {
      case tableName ~ userSpecifiedSchema ~ provider ~ opts =>
        val newOpts = opts +  ("tableschema" ->  userSpecifiedSchema)
        CreateExternalTableUsing(
          tableName,
          None,
          provider,
          new CaseInsensitiveMap(newOpts))
    }

  protected lazy val externalTableInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val remaining = in.source.subSequence(in.offset, in.source.length()).toString
      assert(remaining.contains(USING.str))
      val externalTableDefinition = remaining.substring(0, remaining.indexOf(USING.str))
      val others = remaining.substring(externalTableDefinition.length)
      val reader = new PackratReader(new lexical.Scanner(others))
      Success(
        externalTableDefinition,reader)
    }
  }
}

private[sql] case class CreateExternalTableUsing(
                                                  tableName: String,
                                                  userSpecifiedSchema: Option[StructType],
                                                  provider: String,
                                                  options: Map[String, String]
                                                  ) extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}

object StreamStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateStream(streamName, userColumns, options) =>
      ExecutedCommand(
        CreateStreamTableCmd(streamName, userColumns, options)) :: Nil
    case CreateSampledTable(streamName, options) =>
      ExecutedCommand(
        CreateSampledTableCmd(streamName, options)) :: Nil
    case StreamingCtxtActions(action, batchInterval) =>
      ExecutedCommand(
        StreamingCtxtActionsCmd(action, batchInterval)) :: Nil
    case _ => Nil
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

private[sql] case class CreateStreamTableCmd(streamIdent: String,
    userColumns: Option[StructType],
    options: Map[String, String])
    extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val resolved = ResolvedDataSource(sqlContext, userColumns,
      Array.empty[String], "org.apache.spark.sql.sources.StreamSource", options)
    val plan = LogicalRelation(resolved.relation)

    val catalog = sqlContext.asInstanceOf[SnappyContext].catalog
    val streamName = catalog.processTableIdentifier(streamIdent)
    // add the stream to the tables in the catalog
    catalog.tables.get(streamName) match {
      case None => catalog.tables.put(streamName, plan)
      case Some(x) => throw new IllegalStateException(
        s"Stream name $streamName already defined")
    }
    Seq.empty
  }
}

private[sql] case class StreamingCtxtActionsCmd(action: Int,
    batchInterval: Option[Int])
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {

    action match {
      case 0 =>
        import org.apache.spark.sql.snappy._

        sqlContext.sparkContext.getOrCreateStreamingContext(
          batchInterval.getOrElse(throw new IllegalStateException()))

      case 1 =>
        // Register sampling of all the streams
        val snappyCtxt = sqlContext.asInstanceOf[SnappyContext]
        val catalog = snappyCtxt.catalog
        val streamTables = catalog.tables.collect {
          // runtime type is erased to Any, but we keep the actual ClassTag
          // around in StreamRelation so as to give the proper runtime type
          case (streamTableName, LogicalRelation(sr: StreamRelation[_])) =>
            (streamTableName, sr.asInstanceOf[StreamRelation[Any]])
        }
        streamTables.foreach {
          case (streamTableName, sr) =>
            val streamTable = Some(streamTableName)
            val aqpTables = catalog.tables.collect {
              case (sampleTableName, sr: StratifiedSample)
                if sr.streamTable == streamTable => sampleTableName
            } ++ catalog.topKStructures.collect {
              case (topKName, topK: TopKWrapper)
                if topK.streamTable == streamTable => topKName
            }
            if (aqpTables.nonEmpty) {
              snappyCtxt.saveStream(sr.dStream, aqpTables.toSeq, sr.formatter,
                sr.schema)(sr.ct)
            }
        }
        // start the streaming
        StreamingCtxtHolder.streamingContext.start()

      case 2 => StreamingCtxtHolder.streamingContext.stop()
    }
    Seq.empty[Row]
  }
}

private[sql] case class CreateSampledTableCmd(sampledTableName: String,
    options: Map[String, String])
    extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val tableIdent = OptsUtil.getOption(OptsUtil.BASETABLE, options)

    val snappyCtxt = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snappyCtxt.catalog

    val tableName = catalog.processTableIdentifier(tableIdent)
    val sr = catalog.getStreamTableRelation(tableName)

    // Add the sample table to the catalog as well.
    // StratifiedSampler is not expecting base table, remove it.
    snappyCtxt.registerSampleTable(sampledTableName, sr.schema,
      options - OptsUtil.BASETABLE, Some(tableName))

    Seq.empty
  }
}

object OptsUtil {

  // Options while creating sample/stream table
  val BASETABLE = "basetable"
  val SERVER_ADDRESS = "serveraddress"
  //val PORT = "port"
  val FORMAT = "format"
  val SLIDEDURATION = "slideduration"
  val WINDOWDURATION = "windowduration"
  val STORAGE_LEVEL = "storagelevel"

  def newAnalysisException(msg: String) = new AnalysisException(msg)

  def getOption(optionName: String, options: Map[String, String]): String =
    options.getOrElse(optionName, throw newAnalysisException(
      s"Option $optionName not defined"))

  def getOptionally(optionName: String,
      options: Map[String, String]): Option[String] = options.get(optionName)
}

private[spark] object StreamingCtxtHolder {

  private val atomicContext = new AtomicReference[StreamingContext]()

  def streamingContext = atomicContext.get()

  def apply(sparkCtxt: SparkContext,
      duration: Int): StreamingContext = {
    val context = atomicContext.get
    if (context != null &&
        !context.getState().equals(StreamingContextState.STOPPED)) {
      return context
    }
    atomicContext.compareAndSet(context,
      new StreamingContext(sparkCtxt, Seconds(duration)))
    atomicContext.get

  }
}
