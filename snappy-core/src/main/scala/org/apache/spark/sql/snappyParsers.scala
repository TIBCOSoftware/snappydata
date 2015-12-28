package org.apache.spark.sql

import java.sql.SQLException
import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{ParserDialect, SqlParserBase, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds}


class SnappyParserBase extends SqlParserBase {

  protected val DELETE = Keyword("DELETE")
  protected val UPDATE = Keyword("UPDATE")
  // Added for streaming window CQs
  protected val WINDOW = Keyword("WINDOW")
  protected val DURATION = Keyword("DURATION")
  protected val SLIDE = Keyword("SLIDE")
  protected val MILLISECONDS = Keyword("MILLISECONDS")
  protected val SECONDS = Keyword("SECONDS")
  protected val MINUTES = Keyword("MINUTES")

  override protected lazy val start: Parser[LogicalPlan] = start1 | insert |
      cte | dmlForExternalTable

  protected lazy val dmlForExternalTable: Parser[LogicalPlan] =
    (INSERT ~> INTO | DELETE ~> FROM | UPDATE) ~> tableIdentifier ~ wholeInput ^^ {
      case r ~ s => DMLExternalTable(r, UnresolvedRelation(r), s)
    }

  protected lazy val unit: Parser[Duration] =
    (
        stringLit <~ MILLISECONDS ^^ { case str => Milliseconds(str.toInt) }
      | stringLit <~ SECONDS ^^ { case str => Seconds(str.toInt) }
      | stringLit <~ MINUTES ^^ { case str => Minutes(str.toInt) }
    )

  protected lazy val windowOptions: Parser[(Duration, Option[Duration])] =
    WINDOW ~ "(" ~> (DURATION ~> unit) ~
      ("," ~ SLIDE ~> unit).? <~ ")" ^^ {
      case duration ~ slide => (duration, slide)
    }

  protected override lazy val relationFactor: Parser[LogicalPlan] =
    (tableIdentifier ~ windowOptions.? ~ (opt(AS) ~> opt(ident)) ^^ {
      case tableIdent ~ window ~ alias => window.map { win =>
        WindowLogicalPlan(
          win._1,
          win._2,
          UnresolvedRelation(tableIdent, alias))
      }.getOrElse(UnresolvedRelation(tableIdent, alias))
    }
      |
      ("(" ~> start <~ ")") ~ windowOptions.? ~ (AS.? ~> ident) ^^ {
      case child ~ window ~ alias => window.map { win =>
        WindowLogicalPlan(
          win._1,
          win._2,
          Subquery(alias, child))
      }.getOrElse(Subquery(alias, child))
    })

  override def parseExpression(input: String): Expression = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(projection)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      //case failureOrError => sys.error(failureOrError.toString)
      case failureOrError => throw new SQLException(failureOrError.toString, "42X01")
    }
  }

}

object SnappyParser extends SnappyParserBase{

}

/** Snappy dialect adds SnappyParser additions to the standard "sql" dialect */
private[sql] class SnappyParserDialect extends ParserDialect {

  override def parse(sqlText: String): LogicalPlan = {
    SnappyParser.parse(sqlText)
  }
}

/**
 * Snappy DDL extensions for streaming and sampling.
 */
private[sql] class SnappyDDLParser(parseQuery: String => LogicalPlan)
    extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | describeTable | refreshTable | dropTable |
        createStream  | strmctxt | truncateTable | createIndex | dropIndex

  protected val STREAM = Keyword("STREAM")
  protected val STREAMING = Keyword("STREAMING")
  protected val CONTEXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")
  protected val DROP = Keyword("DROP")
  protected val TRUNCATE = Keyword("TRUNCATE")
  protected val INDEX = Keyword("INDEX")
  protected val ON = Keyword("ON")

  private val DDLEnd = Pattern.compile(USING.str + "\\s+[a-zA-Z_0-9\\.]+\\s*" +
      s"(\\s${OPTIONS.str}|\\s${AS.str}|$$)", Pattern.CASE_INSENSITIVE)

  protected override lazy val createTable: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~
        tableIdentifier ~ externalTableInput ~ (USING ~> className).? ~
        (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temporary ~ allowExisting ~ tableIdent ~ schemaString ~
          providerName ~ opts ~ query =>

        val options = opts.getOrElse(Map.empty[String, String])
        val provider = SnappyContext.getProvider(providerName.getOrElse(SnappyContext.DEFAULT_SOURCE))
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
            CreateTableUsingAsSelect(tableIdent, provider, temporary = true,
              Array.empty[String], mode, options, queryPlan)
          } else {
            CreateExternalTableUsingSelect(tableIdent, provider,
              Array.empty[String], mode, options, queryPlan)
          }
        } else {
          val hasExternalSchema = if (temporary.isDefined) false
          else {
            // check if provider class implements ExternalSchemaRelationProvider
            try {
              val clazz: Class[_] = ResolvedDataSource.lookupDataSource(provider)
              classOf[ExternalSchemaRelationProvider].isAssignableFrom(clazz)
            } catch {
              case cnfe: ClassNotFoundException => throw new DDLException(cnfe.toString)
              case t: Throwable => throw t
            }
          }
          val userSpecifiedSchema = if (hasExternalSchema) None
          else {
            phrase(tableCols.?)(new lexical.Scanner(schemaString)) match {
              case Success(columns, _) => columns.flatMap(fields => Some(StructType(fields)))
              case failure => throw new DDLException(failure.toString)
            }
          }
          val schemaDDL = if (hasExternalSchema) Some(schemaString) else None

          if (temporary.isDefined) {
            CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
              temporary = true, options, allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateExternalTableUsing(tableIdent, userSpecifiedSchema,
              schemaDDL, provider, allowExisting.isDefined, options)
          }
        }
    }

  protected lazy val createIndex: Parser[LogicalPlan] =
    (CREATE ~> INDEX ~> ident) ~ (ON ~> ident) ~ wholeInput ^^ {
      case indexName ~ tableName ~ sql =>
        CreateIndex(tableName, sql)
    }

  protected lazy val dropIndex: Parser[LogicalPlan] =
    (DROP ~> INDEX ~> ident) ~ wholeInput ^^ {
      case indexName ~ sql =>
        DropIndex(sql)
    }

  protected lazy val dropTable: Parser[LogicalPlan] =
    (DROP ~> TEMPORARY.? <~ TABLE) ~ (IF ~> EXISTS).? ~ ident ^^ {
      case temporary ~ allowExisting ~ tableName =>
        DropTable(tableName, temporary.isDefined, allowExisting.isDefined)
    }

  protected lazy val truncateTable: Parser[LogicalPlan] =
    (TRUNCATE ~> TEMPORARY.? <~ TABLE) ~ ident ^^ {
      case temporary ~ tableName =>
        TruncateTable(tableName, temporary.isDefined)
    }

  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> STREAM ~> TABLE ~> ident) ~
        tableCols.? ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case streamname ~ cols ~ providerName ~ opts =>
        val userColumns = cols.flatMap(fields => Some(StructType(fields)))
        val provider = SnappyContext.getProvider(providerName)
        val userOpts  = opts.updated(USING.str, provider)
        CreateStreamTable(streamname, userColumns, new CaseInsensitiveMap(userOpts))
    }



  protected lazy val strmctxt: Parser[LogicalPlan] =
    (STREAMING ~> CONTEXT ~>
        (INIT ^^^ 0 | START ^^^ 1 | STOP ^^^ 2) ~ numericLit.?) ^^ {
      case action ~ batchInterval =>
        if (batchInterval.isDefined)
          StreamOperationsLogicalPlan(action, Some(batchInterval.get.toInt))
        else
          StreamOperationsLogicalPlan(action, None)

    }

  protected override lazy val tableIdentifier: Parser[TableIdentifier] =
    (ident <~ ".").? ~ (ident <~ ".").? ~ ident ^^ {
      case maybeDbName ~ maybeSchemaName ~ tableName =>
        val schemaPrefix = maybeSchemaName.map(_ + '.').getOrElse("")
        TableIdentifier(schemaPrefix + tableName, maybeDbName)
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
        Success(
          in.source.subSequence(in.offset, in.source.length()).toString,
          in.drop(in.source.length()))
      }
    }
  }
}

private[sql] case class CreateExternalTableUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.catalog.newQualifiedTableName(tableIdent), provider,
      userSpecifiedSchema, schemaDDL, mode, options)
    Seq.empty
  }
}

private[sql] case class CreateExternalTableUsingSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snc.catalog
    snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
      partitionColumns, mode, options, query)
    // refresh cache of the table in catalog
    catalog.invalidateTable(tableIdent)
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableName: String,
    temporary: Boolean,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    if (temporary) snc.dropTempTable(tableName, ifExists)
    else snc.dropExternalTable(tableName, ifExists)
    Seq.empty
  }
}

private[sql] case class TruncateTable(
    tableName: String,
    temporary: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    if (temporary) snc.truncateTable(tableName)
    else snc.truncateExternalTable(tableName)
    Seq.empty
  }
}

private[sql] case class CreateIndex(
    tableName: String,
    sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc =sqlContext.asInstanceOf[SnappyContext]
    snc.createIndexOnExternalTable(tableName, sql)
    Seq.empty
  }
}

private[sql] case class DropIndex(
    sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.dropIndexOnExternalTable(sql)
    Seq.empty
  }
}

case class DMLExternalTable(
    tableName: TableIdentifier,
    child: LogicalPlan,
    command: String)
    extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil

  override def output: Seq[Attribute] = child.output
}


private[sql] case class CreateStreamTable(streamName: String,
    userColumns: Option[StructType],
    options: Map[String, String])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}



private[sql] case class StreamOperationsLogicalPlan(action: Int,
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
    val provider = SnappyContext.getProvider(options("using"))
    val resolved = ResolvedDataSource(sqlContext, userColumns,
      Array.empty[String], provider, options)
    val plan = LogicalRelation(resolved.relation)
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snc.catalog
    val streamTable = catalog.newQualifiedTableName(new TableIdentifier(streamIdent))

    // add the stream to the tables in the catalog
    catalog.tables.get(streamTable) match {
      case None => catalog.tables.put(streamTable, plan)
      case Some(x) => throw new IllegalStateException(
        s"Stream table name $streamTable already defined")
    }
    Seq.empty
  }
}

private[sql] case class StreamingCtxtActionsCmd(action: Int,
                                                batchInterval: Option[Int],
                                                sampleTablePopulation: Option[(SQLContext) => Unit])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {

    action match {
      case 0 =>
        import org.apache.spark.sql.snappy._

        sqlContext.sparkContext.getOrCreateStreamingContext(
          batchInterval.getOrElse(throw new IllegalStateException()))

      case 1 =>
        // Register sampling of all the streams
        // Register sampling of all the streams
        sampleTablePopulation match {
          case Some(func) => func(sqlContext)
          case None => // do nothing
        }
        // start the streaming
        StreamingCtxtHolder.streamingContext.start()

      case 2 => StreamingCtxtHolder.streamingContext.stop()
    }
    Seq.empty[Row]
  }
}



