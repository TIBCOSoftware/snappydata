package org.apache.spark.sql

import java.util.regex.Pattern

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.{ParserDialect, SqlParser}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._

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
  protected val DELETE = Keyword("DELETE")
  protected val UPDATE = Keyword("UPDATE")

  protected val defaultConfidence = 0.75

  override protected lazy val start: Parser[LogicalPlan] = start1 | insert |
      cte | dmlForExternalTable

  override protected lazy val function = functionDef |
      ERROR ~> ESTIMATE ~> ("(" ~> floatLit <~ ")").? ~
          ident ~ ("(" ~> expression <~ ")") ^^ {
        case c ~ op ~ exp =>
          try {
            val aggType = ErrorAggregate.withName(
              op.toUpperCase(java.util.Locale.ENGLISH))
            val confidence = c.map(_.toDouble).getOrElse(defaultConfidence)
            ErrorEstimateAggregate(exp, confidence, null, c.isEmpty, aggType)
          } catch {
            case _: java.util.NoSuchElementException =>
              throw new AnalysisException(
                s"No error estimate implemented for $op")
          }
      }

  protected lazy val dmlForExternalTable: Parser[LogicalPlan] =
    (INSERT ~> INTO | DELETE ~> FROM | UPDATE) ~> ident ~ wholeInput ^^ {
      case r ~ s => DMLExternalTable(r, UnresolvedRelation(Seq(r)), s)
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
    createTable | describeTable | refreshTable | dropTable |
        createStream | createSampled | strmctxt

  protected val STREAM = Keyword("STREAM")
  protected val SAMPLED = Keyword("SAMPLED")
  protected val STRM = Keyword("STREAMING")
  protected val CTXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")
  protected val DROP = Keyword("DROP")

  private val DDLEnd = Pattern.compile(USING.str + "\\s+[a-zA-Z_0-9\\.]+\\s+" +
      OPTIONS.str, Pattern.CASE_INSENSITIVE)


  protected override lazy val createTable: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ ident ~
        externalTableInput ~ (USING ~> className) ~ (OPTIONS ~> options) ~
        (AS ~> restInput).? ^^ {
      case temporary ~ allowExisting ~ tableName ~ schemaString ~
          providerName ~ opts ~ query =>

        val provider = SnappyContext.getProvider(providerName)
        if (query.isDefined) {
          if (schemaString.length > 0) {
            throw new DDLException("CREATE TABLE AS SELECT statement " +
                "does not allow column definitions.")
          }
          // When IF NOT EXISTS clause appears in the query,
          // the save mode will be ignore.
          val mode = if (allowExisting.isDefined) SaveMode.Ignore
          else SaveMode.ErrorIfExists
          val queryPlan = parseQuery(query.get)

          if (temporary.isDefined) {
            CreateTableUsingAsSelect(tableName, provider, temporary = true,
              Array.empty[String], mode, opts, queryPlan)
          } else {
            CreateExternalTableUsingSelect(tableName, provider,
              Array.empty[String], mode, opts, queryPlan)
          }
        } else {
          val hasExternalSchema = if (temporary.isDefined) false
          else {
            // check if provider class implements ExternalSchemaRelationProvider
            val clazz: Class[_] = ResolvedDataSource.lookupDataSource(provider)
            classOf[ExternalSchemaRelationProvider].isAssignableFrom(clazz)
          }
          val userSpecifiedSchema = if (hasExternalSchema) None
          else {
            phrase(tableCols)(new lexical.Scanner(schemaString)) match {
              case Success(columns, _) => Some(StructType(columns))
              case failure => throw new DDLException(failure.toString)
            }
          }
          val schemaDDL = if (hasExternalSchema) Some(schemaString) else None

          if (temporary.isDefined) {
            CreateTableUsing(tableName, userSpecifiedSchema, provider,
              temporary = true, opts, allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateExternalTableUsing(tableName, userSpecifiedSchema,
              schemaDDL, provider, allowExisting.isDefined, opts)
          }
        }
    }

  protected lazy val dropTable: Parser[LogicalPlan] =
    (DROP ~> TEMPORARY.? <~ TABLE) ~ (IF ~> EXISTS).? ~ ident ^^ {
      case temporary ~ allowExisting ~ tableName =>
        DropTable(tableName, temporary.isDefined, allowExisting.isDefined)
    }

  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> STREAM ~> TABLE ~> ident) ~
        tableCols.? ~ (OPTIONS ~> options) ^^ {
      case streamname ~ cols ~ opts =>
        val userColumns = cols.flatMap(fields => Some(StructType(fields)))
        CreateStream(streamname, userColumns, new CaseInsensitiveMap(opts))
    }

  protected lazy val createSampled: Parser[LogicalPlan] =
    (CREATE ~> SAMPLED ~> TABLE ~> ident) ~
        (OPTIONS ~> options) ^^ {
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

  protected lazy val externalTableInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val source = in.source
      val remaining = source.subSequence(in.offset, source.length).toString
      val m = DDLEnd.matcher(remaining)
      if (m.find) {
        val index = m.start()
        val externalTableDefinition = remaining.substring(0, index).trim
        val others = remaining.substring(index)
        val reader = new PackratReader(new lexical.Scanner(others))
        Success(externalTableDefinition, reader)
      } else {
        Failure("USING missing", in)
      }
    }
  }
}

private[sql] case class CreateExternalTableUsing(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = SnappyContext(sqlContext.sparkContext)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(tableName, provider, userSpecifiedSchema,
      schemaDDL, mode, options)
    Seq.empty
  }
}

private[sql] case class CreateExternalTableUsingSelect(
    tableName: String,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = SnappyContext(sqlContext.sparkContext)
    snc.createTable(tableName, provider, partitionColumns, mode,
      options, query)
    // refresh cache of the table in catalog
    val catalog = snc.catalog
    catalog.invalidateTable(catalog.newQualifiedTableName(tableName))
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableName: String,
    temporary: Boolean,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = SnappyContext(sqlContext.sparkContext)
    if (temporary) snc.dropTempTable(tableName, ifExists)
    else snc.dropExternalTable(tableName, ifExists)
    Seq.empty
  }
}

case class DMLExternalTable(
    tableName: String,
    child: LogicalPlan,
    command: String)
    extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil

  override def output: Seq[Attribute] = child.output
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
      Array.empty[String], classOf[StreamSource].getCanonicalName, options)
    val plan = LogicalRelation(resolved.relation)

    val catalog = SnappyContext(sqlContext.sparkContext).catalog
    val streamTable = catalog.newQualifiedTableName(streamIdent)
    // add the stream to the tables in the catalog
    catalog.tables.get(streamTable) match {
      case None => catalog.tables.put(streamTable, plan)
      case Some(x) => throw new IllegalStateException(
        s"Stream name $streamTable already defined")
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
        val snappyCtxt = SnappyContext(sqlContext.sparkContext)
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
              case (sampleTable, sr: StratifiedSample)
                if sr.streamTable == streamTable => sampleTable.qualifiedName
            } ++ catalog.topKStructures.collect {
              case (topKName, (topK, _))
                if topK.streamTable == streamTable => topKName.qualifiedName
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

    val snappyCtxt = SnappyContext(sqlContext.sparkContext)
    val catalog = snappyCtxt.catalog

    val tableName = catalog.newQualifiedTableName(tableIdent)
    val sr = catalog.getStreamTableRelation(tableName)

    // Add the sample table to the catalog as well.
    // StratifiedSampler is not expecting base table, remove it.
    snappyCtxt.registerSampleTable(sampledTableName, sr.schema,
      options - OptsUtil.BASETABLE, Some(tableName.qualifiedName))

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

  @volatile private[this] var globalContext: StreamingContext = _
  private[this] val contextLock = new AnyRef

  def streamingContext = globalContext

  def apply(sparkCtxt: SparkContext,
      duration: Int): StreamingContext = {
    val context = globalContext
    if (context != null &&
        context.getState() != StreamingContextState.STOPPED) {
      context
    } else contextLock.synchronized {
      val context = globalContext
      if (context != null &&
          context.getState() != StreamingContextState.STOPPED) {
        context
      } else {
        val context = new StreamingContext(sparkCtxt, Seconds(duration))
        globalContext = context
        context
      }
    }
  }
}
