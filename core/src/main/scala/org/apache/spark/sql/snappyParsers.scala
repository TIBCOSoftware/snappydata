/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
import scala.util.{Failure, Success, Try}

import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.SnappyParserConsts.{falseFn, plusOrMinus, trueFn}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, DataSource, RefreshTable}
import org.apache.spark.sql.internal.SnappySessionState
import org.apache.spark.sql.sources.{ExternalSchemaRelationProvider, PutIntoTable}
import org.apache.spark.sql.streaming.{StreamPlanProvider, WindowLogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds, SnappyStreamingContext}
import org.apache.spark.unsafe.types.CalendarInterval

class SnappyParser(session: SnappySession)
    extends SnappyBaseParser(session) {

  private[this] final var _input: ParserInput = _

  override final def input: ParserInput = _input

  private[sql] final def input_=(in: ParserInput): Unit = {
    reset()
    _input = in
  }

  final def ALL: Rule0 = rule { keyword(Consts.ALL) }
  final def AND: Rule0 = rule { keyword(Consts.AND) }
  final def AS: Rule0 = rule { keyword(Consts.AS) }
  final def ASC: Rule0 = rule { keyword(Consts.ASC) }
  final def BETWEEN: Rule0 = rule { keyword(Consts.BETWEEN) }
  final def BY: Rule0 = rule { keyword(Consts.BY) }
  final def CASE: Rule0 = rule { keyword(Consts.CASE) }
  final def CAST: Rule0 = rule { keyword(Consts.CAST) }
  final def CREATE: Rule0 = rule { keyword(Consts.CREATE) }
  final def CURRENT: Rule0 = rule { keyword(Consts.CURRENT) }
  final def DELETE: Rule0 = rule { keyword(Consts.DELETE) }
  final def DESC: Rule0 = rule { keyword(Consts.DESC) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DISTINCT: Rule0 = rule { keyword(Consts.DISTINCT) }
  final def DROP: Rule0 = rule { keyword(Consts.DROP) }
  final def ELSE: Rule0 = rule { keyword(Consts.ELSE) }
  final def END: Rule0 = rule { keyword(Consts.END) }
  final def EXCEPT: Rule0 = rule { keyword(Consts.EXCEPT) }
  final def EXISTS: Rule0 = rule { keyword(Consts.EXISTS) }
  final def EXTERNAL: Rule0 = rule { keyword(Consts.EXTERNAL) }
  final def FALSE: Rule0 = rule { keyword(Consts.FALSE) }
  final def FROM: Rule0 = rule { keyword(Consts.FROM) }
  final def FULL: Rule0 = rule { keyword(Consts.FULL) }
  final def FUNCTION: Rule0 = rule { keyword(Consts.FUNCTION) }
  final def GROUP: Rule0 = rule { keyword(Consts.GROUP) }
  final def HAVING: Rule0 = rule { keyword(Consts.HAVING) }
  final def IF: Rule0 = rule { keyword(Consts.IF) }
  final def IN: Rule0 = rule { keyword(Consts.IN) }
  final def INNER: Rule0 = rule { keyword(Consts.INNER) }
  final def INSERT: Rule0 = rule { keyword(Consts.INSERT) }
  final def INTERSECT: Rule0 = rule { keyword(Consts.INTERSECT) }
  final def INTERVAL: Rule0 = rule { keyword(Consts.INTERVAL) }
  final def INTO: Rule0 = rule { keyword(Consts.INTO) }
  final def IS: Rule0 = rule { keyword(Consts.IS) }
  final def JOIN: Rule0 = rule { keyword(Consts.JOIN) }
  final def LEFT: Rule0 = rule { keyword(Consts.LEFT) }
  final def LIKE: Rule0 = rule { keyword(Consts.LIKE) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def NOT: Rule0 = rule { keyword(Consts.NOT) }
  final def NULL: Rule0 = rule { keyword(Consts.NULL) }
  final def ON: Rule0 = rule { keyword(Consts.ON) }
  final def OR: Rule0 = rule { keyword(Consts.OR) }
  final def ORDER: Rule0 = rule { keyword(Consts.ORDER) }
  final def OUTER: Rule0 = rule { keyword(Consts.OUTER) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PUT: Rule0 = rule { keyword(Consts.PUT) }
  final def REGEXP: Rule0 = rule { keyword(Consts.REGEXP) }
  final def RIGHT: Rule0 = rule { keyword(Consts.RIGHT) }
  final def RLIKE: Rule0 = rule { keyword(Consts.RLIKE) }
  final def SCHEMA: Rule0 = rule { keyword(Consts.SCHEMA) }
  final def SELECT: Rule0 = rule { keyword(Consts.SELECT) }
  final def SEMI: Rule0 = rule { keyword(Consts.SEMI) }
  final def SET: Rule0 = rule { keyword(Consts.SET) }
  final def SORT: Rule0 = rule { keyword(Consts.SORT) }
  final def TABLE: Rule0 = rule { keyword(Consts.TABLE) }
  final def TEMPORARY: Rule0 = rule { keyword(Consts.TEMPORARY) }
  final def THEN: Rule0 = rule { keyword(Consts.THEN) }
  final def TO: Rule0 = rule { keyword(Consts.TO) }
  final def TRUE: Rule0 = rule { keyword(Consts.TRUE) }
  final def UNION: Rule0 = rule { keyword(Consts.UNION) }
  final def UNIQUE: Rule0 = rule { keyword(Consts.UNIQUE) }
  final def UPDATE: Rule0 = rule { keyword(Consts.UPDATE) }
  final def USING: Rule0 = rule { keyword(Consts.USING) }
  final def WHEN: Rule0 = rule { keyword(Consts.WHEN) }
  final def WHERE: Rule0 = rule { keyword(Consts.WHERE) }
  final def WITH: Rule0 = rule { keyword(Consts.WITH) }
  // interval units are not reserved (handled in Consts singleton)
  final def DAY: Rule0 = rule { intervalUnit(Consts.DAY) }
  final def HOUR: Rule0 = rule { intervalUnit(Consts.HOUR) }
  final def MICROS: Rule0 = rule { intervalUnit("micro") }
  final def MICROSECOND: Rule0 = rule { intervalUnit(Consts.MICROSECOND) }
  final def MILLIS: Rule0 = rule { intervalUnit("milli") }
  final def MILLISECOND: Rule0 = rule { intervalUnit(Consts.MILLISECOND) }
  final def MINS: Rule0 = rule { intervalUnit("min") }
  final def MINUTE: Rule0 = rule { intervalUnit(Consts.MINUTE) }
  final def MONTH: Rule0 = rule { intervalUnit(Consts.MONTH) }
  final def SECS: Rule0 = rule { intervalUnit("sec") }
  final def SECOND: Rule0 = rule { intervalUnit(Consts.SECOND) }
  final def WEEK: Rule0 = rule { intervalUnit(Consts.WEEK) }
  final def YEAR: Rule0 = rule { intervalUnit(Consts.YEAR) }
  // Added for streaming window CQs
  final def DURATION: Rule0 = rule { keyword(Consts.DURATION) }
  final def SLIDE: Rule0 = rule { keyword(Consts.SLIDE) }
  final def WINDOW: Rule0 = rule { keyword(Consts.WINDOW) }
  // DDL/misc commands (non-reserved)
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def EXTENDED: Rule0 = rule { keyword(Consts.EXTENDED) }
  final def FUNCTIONS: Rule0 = rule { keyword(Consts.FUNCTIONS) }
  final def GLOBAL: Rule0 = rule { keyword(Consts.GLOBAL) }
  final def HASH: Rule0 = rule { keyword(Consts.HASH) }
  final def INDEX: Rule0 = rule { keyword(Consts.INDEX) }
  final def INIT: Rule0 = rule { keyword(Consts.INIT) }
  final def LAZY: Rule0 = rule { keyword(Consts.LAZY) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def REFRESH: Rule0 = rule { keyword(Consts.REFRESH) }
  final def SHOW: Rule0 = rule { keyword(Consts.SHOW) }
  final def START: Rule0 = rule { keyword(Consts.START) }
  final def STOP: Rule0 = rule { keyword(Consts.STOP) }
  final def STREAM: Rule0 = rule { keyword(Consts.STREAM) }
  final def STREAMING: Rule0 = rule { keyword(Consts.STREAMING) }
  final def TABLES: Rule0 = rule { keyword(Consts.TABLES) }
  final def TRUNCATE: Rule0 = rule { keyword(Consts.TRUNCATE) }
  final def UNCACHE: Rule0 = rule { keyword(Consts.UNCACHE) }

  private def toDecimalOrDoubleLiteral(s: String,
      scientific: Boolean): Literal = {
    // follow the behavior in MS SQL Server
    // https://msdn.microsoft.com/en-us/library/ms179899.aspx
    if (scientific) {
      Literal(s.toDouble)
    } else {
      Literal(new java.math.BigDecimal(s, BigDecimal.defaultMathContext))
    }
  }

  private def toNumericLiteral(s: String): Literal = {
    // quick pass through the string to check for floats
    var decimal = false
    var index = 0
    val len = s.length
    while (index < len) {
      val c = s.charAt(index)
      if (!decimal) {
        if (c == '.') {
          decimal = true
        } else if (c == 'e' || c == 'E') {
          return toDecimalOrDoubleLiteral(s, scientific = true)
        }
      } else if (c == 'e' || c == 'E') {
        return toDecimalOrDoubleLiteral(s, scientific = true)
      }
      index += 1
    }
    if (decimal) {
      toDecimalOrDoubleLiteral(s, scientific = false)
    } else {
      // case of integral value
      // most cases should be handled by Long, so try that first
      try {
        val longValue = java.lang.Long.parseLong(s)
        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          Literal(longValue.toInt)
        } else {
          Literal(longValue)
        }
      } catch {
        case nfe: NumberFormatException =>
          val decimal = BigDecimal(s)
          if (decimal.isValidInt) {
            Literal(decimal.toIntExact)
          } else if (decimal.isValidLong) {
            Literal(decimal.toLongExact)
          } else {
            Literal(decimal)
          }
      }
    }
  }

  protected final def booleanLiteral: Rule1[Literal] = rule {
    TRUE ~> (() => Literal.create(true, BooleanType)) |
    FALSE ~> (() => Literal.create(false, BooleanType))
  }

  protected final def numericLiteral: Rule1[Literal] = rule {
    capture(plusOrMinus.? ~ Consts.numeric.+) ~ delimiter ~>
        ((s: String) => toNumericLiteral(s))
  }

  protected final def literal: Rule1[Literal] = rule {
    stringLiteral ~> ((s: String) => Literal.create(s, StringType)) |
    numericLiteral |
    booleanLiteral |
    NULL ~> (() => Literal.create(null, NullType)) |
    intervalLiteral
  }

  /** the string passed in *SHOULD* be lower case */
  private def intervalUnit(k: String): Rule0 = rule {
    atomic(ignoreCase(k) ~ Consts.plural.?) ~ delimiter
  }

  private def intervalUnit(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower) ~ Consts.plural.?) ~ delimiter
  }

  protected def month: Rule1[Int] = rule {
    integral ~ MONTH ~> ((num: String) => num.toInt)
  }

  protected def year: Rule1[Int] = rule {
    integral ~ YEAR ~> ((num: String) => num.toInt * 12)
  }

  protected def microsecond: Rule1[Long] = rule {
    integral ~ (MICROS | MICROSECOND) ~> ((num: String) => num.toLong)
  }

  protected def millisecond: Rule1[Long] = rule {
    integral ~ (MILLIS | MILLISECOND) ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_MILLI)
  }

  protected def second: Rule1[Long] = rule {
    integral ~ (SECS | SECOND) ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_SECOND)
  }

  protected def minute: Rule1[Long] = rule {
    integral ~ (MINS | MINUTE) ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_MINUTE)
  }

  protected def hour: Rule1[Long] = rule {
    integral ~ HOUR ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_HOUR)
  }

  protected def day: Rule1[Long] = rule {
    integral ~ DAY ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_DAY)
  }

  protected def week: Rule1[Long] = rule {
    integral ~ WEEK ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_WEEK)
  }

  protected def intervalLiteral: Rule1[Literal] = rule {
    INTERVAL ~ (
        stringLiteral ~ (
            YEAR ~ TO ~ MONTH ~> ((s: String) =>
              Literal(CalendarInterval.fromYearMonthString(s))) |
            DAY ~ TO ~ (SECS | SECOND) ~> ((s: String) =>
              Literal(CalendarInterval.fromDayTimeString(s))) |
            YEAR ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("year", s))) |
            MONTH ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("month", s))) |
            DAY ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("day", s))) |
            HOUR ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("hour", s))) |
            (MINS | MINUTE) ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("minute", s))) |
            (SECS | SECOND) ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("second", s)))
        ) |
        year.? ~ month.? ~ week.? ~ day.? ~ hour.? ~ minute.? ~
            second.? ~ millisecond.? ~ microsecond.? ~> { (y: Option[Int],
            m: Option[Int], w: Option[Long], d: Option[Long], h: Option[Long],
            min: Option[Long], s: Option[Long], millis: Option[Long],
            micros: Option[Long]) =>
          if (!Seq(y, m, w, d, h, min, s, millis, micros).exists(_.isDefined)) {
            throw Utils.analysisException(
              "at least one time unit should be given for interval literal")
          }
          val months = Seq(y, m).map(_.getOrElse(0)).sum
          val microseconds = Seq(w, d, h, min, s, millis, micros)
              .map(_.getOrElse(0L)).sum
          Literal(new CalendarInterval(months, microseconds))
        }
    )
  }

  protected final def unsignedFloat: Rule1[String] = rule {
    capture(
      CharPredicate.Digit.* ~ '.' ~ CharPredicate.Digit.+ ~
          scientificNotation.? |
      CharPredicate.Digit.+ ~ scientificNotation
    ) ~ ws
  }

  protected final def projection: Rule1[Expression] = rule {
    expression ~ (
        AS.? ~ identifier ~> ((e: Expression, a: String) => Alias(e, a)()) |
        MATCH.asInstanceOf[Rule[Expression::HNil, Expression::HNil]]
    )
  }

  final def expression: Rule1[Expression] = rule {
    andExpression ~ (OR ~ andExpression ~>
        ((e1: Expression, e2: Expression) => Or(e1, e2))).*
  }

  protected final def andExpression: Rule1[Expression] = rule {
    notExpression ~ (AND ~ notExpression ~>
        ((e1: Expression, e2: Expression) => And(e1, e2))).*
  }

  protected final def notExpression: Rule1[Expression] = rule {
    (NOT ~> falseFn).? ~ comparisonExpression ~> ((not: Option[Boolean],
        e: Expression) => if (not.isEmpty) e else Not(e))
  }

  protected final def comparisonExpression: Rule1[Expression] = rule {
    termExpression ~ (
        '=' ~ ws ~ termExpression ~>
            ((e1: Expression, e2: Expression) => EqualTo(e1, e2)) |
        '>' ~ (
          '=' ~ ws ~ termExpression ~>
              ((e1: Expression, e2: Expression) => GreaterThanOrEqual(e1, e2)) |
          ws ~ termExpression ~>
              ((e1: Expression, e2: Expression) => GreaterThan(e1, e2))
        ) |
        '<' ~ (
          "=>" ~ ws ~ termExpression ~>
              ((e1: Expression, e2: Expression) => EqualNullSafe(e1, e2)) |
          '=' ~ ws  ~ termExpression ~>
              ((e1: Expression, e2: Expression) => LessThanOrEqual(e1, e2)) |
          '>' ~ ws ~ termExpression ~>
              ((e1: Expression, e2: Expression) => Not(EqualTo(e1, e2))) |
          ws ~ termExpression ~>
              ((e1: Expression, e2: Expression) => LessThan(e1, e2))
        ) |
        '!' ~ '=' ~ ws ~ termExpression ~>
            ((e1: Expression, e2: Expression) => Not(EqualTo(e1, e2))) |
        IS ~ (NOT ~> trueFn).? ~ NULL ~>
            ((e: Expression, not: Option[Boolean]) =>
              if (not.isEmpty) IsNull(e) else IsNotNull(e)) |
        invertibleExpression |
        NOT ~ invertibleExpression ~> Not |
        (RLIKE | REGEXP) ~ termExpression ~> RLike |
        NOT ~ (RLIKE | REGEXP) ~ termExpression ~>
            ((e1: Expression, e2: Expression) => Not(RLike(e1, e2))) |
        MATCH.asInstanceOf[Rule[Expression::HNil, Expression::HNil]]
    )
  }

  /**
    * Expressions which can be preceeded by a NOT. This assumes one expression
    * already pushed on stack which it will pop and then push back the result
    * Expression (hence the slightly odd looking type)
    */
  protected final def invertibleExpression: Rule[Expression::HNil,
      Expression::HNil] = rule {
    IN ~ '(' ~ ws ~ (termExpression * (',' ~ ws)) ~ ')' ~ ws ~> In |
    LIKE ~ termExpression ~> Like |
    BETWEEN ~ termExpression ~ AND ~ termExpression ~>
        ((e: Expression, el: Expression, eu: Expression) =>
          And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))) |
    IN ~ '(' ~ ws ~ query ~ ')' ~ ws ~> ((e1: Expression
        , plan: LogicalPlan) => In(e1, Seq(ListQuery(plan))))
  }

  protected final def termExpression: Rule1[Expression] = rule {
    productExpression ~ (capture(plusOrMinus) ~ ws ~ productExpression ~>
        ((e1: Expression, op: String, e2: Expression) =>
          if (op.charAt(0) == '+') Add(e1, e2) else Subtract(e1, e2))).*
  }

  protected final def productExpression: Rule1[Expression] = rule {
    baseExpression ~ (
        "||" ~ ws ~ baseExpression ~> ((e1: Expression, e2: Expression) =>
          e1 match {
            case Concat(children) => Concat(children :+ e2)
            case _ => Concat(Seq(e1, e2))
          }) |
        capture(Consts.arithmeticOperator) ~ ws ~ baseExpression ~>
            ((e1: Expression, op: String, e2: Expression) =>
          op.charAt(0) match {
            case '*' => Multiply(e1, e2)
            case '/' => Divide(e1, e2)
            case '%' => Remainder(e1, e2)
            case '&' => BitwiseAnd(e1, e2)
            case '|' => BitwiseOr(e1, e2)
            case '^' => BitwiseXor(e1, e2)
            case c => throw new IllegalStateException(
              s"unexpected operation '$c'")
          }) |
        '[' ~ ws ~ baseExpression ~ ']' ~ ws ~> ((base: Expression,
            ordinal: Expression) => UnresolvedExtractValue(base, ordinal)) |
        '.' ~ ws ~ identifier ~> ((base: Expression, fieldName: String) =>
          UnresolvedExtractValue(base, Literal(fieldName)))
    ).*
  }

  protected def durationUnit: Rule1[Duration] = rule {
    integral ~ (
        (MILLIS | MILLISECOND) ~> ((s: String) => Milliseconds(s.toInt)) |
        (SECS | SECOND) ~> ((s: String) => Seconds(s.toInt)) |
        (MINS | MINUTE) ~> ((s: String) => Minutes(s.toInt))
    )
  }

  protected def windowOptions: Rule1[(Duration, Option[Duration])] = rule {
    WINDOW ~ '(' ~ ws ~ DURATION ~ durationUnit ~ (',' ~ ws ~
        SLIDE ~ durationUnit).? ~ ')' ~ ws ~>
        ((d: Duration, s: Option[Duration]) => (d, s))
  }

  protected final def relationFactor: Rule1[LogicalPlan] = rule {
    tableIdentifier ~ windowOptions.? ~ (AS.? ~ identifier).? ~>
        ((tableIdent: TableIdentifier,
            window: Option[(Duration, Option[Duration])],
            alias: Option[String]) => window match {
          case None => UnresolvedRelation(tableIdent, alias)
          case Some(win) => WindowLogicalPlan(win._1, win._2,
            UnresolvedRelation(tableIdent, alias))
        }) |
    '(' ~ ws ~ start ~ ')' ~ ws ~ windowOptions.? ~ AS.? ~ identifier ~>
        ((child: LogicalPlan, window: Option[(Duration, Option[Duration])],
            alias: String) => window match {
          case None => SubqueryAlias(alias, child)
          case Some(win) => WindowLogicalPlan(win._1, win._2,
            SubqueryAlias(alias, child))
        })
  }

  protected final def joinConditions: Rule1[Expression] = rule {
    ON ~ expression
  }

  protected final def joinType: Rule1[JoinType] = rule {
    INNER ~> (() => Inner) |
    LEFT ~ OUTER.? ~> (() => LeftOuter) |
    LEFT ~ SEMI ~> (() => LeftSemi) |
    RIGHT ~ OUTER.? ~> (() => RightOuter) |
    FULL ~ OUTER.? ~> (() => FullOuter) |
    LEFT.? ~ ANTI ~> (() => LeftAnti)
  }

  protected final def sortDirection: Rule1[SortDirection] = rule {
    ASC ~> (() => Ascending) | DESC ~> (() => Descending)
  }

  protected final def ordering: Rule1[Seq[SortOrder]] = rule {
    ((expression ~ sortDirection.? ~> ((e: Expression,
        d: Option[SortDirection]) => (e, d))) + (',' ~ ws)) ~>
        ((exps: Seq[(Expression, Option[SortDirection])]) =>
          exps.map(pair => SortOrder(pair._1, pair._2.getOrElse(Ascending))))
  }

  protected final def sortType: Rule1[LogicalPlan => LogicalPlan] = rule {
    ORDER ~ BY ~ ordering ~> ((o: Seq[SortOrder]) =>
      (l: LogicalPlan) => Sort(o, global = true, l)) |
    SORT ~ BY ~ ordering ~> ((o: Seq[SortOrder]) =>
      (l: LogicalPlan) => Sort(o, global = false, l))
  }

  protected final def relation: Rule1[LogicalPlan] = rule {
    relationFactor ~ (
        (joinType.? ~ JOIN ~ relationFactor ~
            joinConditions.? ~> ((t: Option[JoinType], r: LogicalPlan,
            j: Option[Expression]) => (t, r, j))).+ ~> ((r1: LogicalPlan,
            joins: Seq[(Option[JoinType], LogicalPlan, Option[Expression])]) =>
          joins.foldLeft(r1) { case (lhs, (jt, rhs, cond)) =>
            Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond)
          }) |
        MATCH.asInstanceOf[Rule[LogicalPlan::HNil, LogicalPlan::HNil]]
    )
  }

  protected final def relations: Rule1[LogicalPlan] = rule {
    (relation + (',' ~ ws)) ~> ((joins: Seq[LogicalPlan]) =>
      if (joins.size == 1) joins.head
      else joins.tail.foldLeft(joins.head) {
        case (lhs, rel) => Join(lhs, rel, Inner, None)
      })
  }

  protected final def cast: Rule1[Expression] = rule {
    CAST ~ '(' ~ ws ~ expression ~ AS ~ dataType ~ ')' ~ ws ~>
        ((exp: Expression, t: DataType) => Cast(exp, t))
  }

  protected final def keyWhenThenElse: Rule1[Seq[Expression]] = rule {
    (WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression,
        t: Expression) => (w, t))).+ ~ (ELSE ~ expression).? ~ END ~>
        ((altPart: Seq[(Expression, Expression)], elsePart: Option[Expression]) =>
          altPart.flatMap(e => Seq(e._1, e._2)) ++ elsePart)
  }

  protected final def whenThenElse: Rule1[(Seq[(Expression, Expression)],
      Option[Expression])] = rule {
    (WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression,
        t: Expression) => (w, t))).+ ~ (ELSE ~ expression).? ~ END ~>
        ((altPart: Seq[(Expression, Expression)],
            elsePart: Option[Expression]) => (altPart, elsePart))
  }

  protected final def primary: Rule1[Expression] = rule {
    identifier ~ (
        '(' ~ ws ~ (
            '*' ~ ws ~ ')' ~ ws ~> ((udfName: String) =>
              if (udfName == "COUNT") {
                AggregateExpression(Count(Literal(1)), mode = Complete,
                  isDistinct = false)
              } else {
                throw Utils.analysisException(s"invalid expression $udfName(*)")
              }) |
            (DISTINCT ~> trueFn).? ~ (expression * (',' ~ ws)) ~ ')' ~ ws ~>
                ((udfName: String, d: Option[Boolean], exprs: Seq[Expression]) =>
              if (d.isEmpty) {
                UnresolvedFunction(udfName, exprs, isDistinct = false)
              } else if (udfName.equalsIgnoreCase("count")) {
                aggregate.Count(exprs).toAggregateExpression(isDistinct = true)
              } else {
                UnresolvedFunction(udfName, exprs, isDistinct = true)
              })
        ) |
        '.' ~ ws ~ (
            identifier ~ ('.' ~ ws ~ identifier).* ~>
                ((i1: String, i2: String, rest: Seq[String]) =>
                  UnresolvedAttribute(Seq(i1, i2) ++ rest)) |
            (identifier ~ '.' ~ ws).* ~ '*' ~ ws ~>
                ((i1: String, target: Seq[String]) =>
                  UnresolvedStar(Option(i1 +: target)))
        ) |
        MATCH ~> UnresolvedAttribute.quoted _
    ) |
    literal |
    cast |
    CASE ~ (
        whenThenElse ~> (s => CaseWhen(s._1, s._2)) |
        expression ~ keyWhenThenElse ~> (CaseKeyWhen(_, _))
    ) |
    EXISTS ~ '(' ~ ws ~ query ~ ')' ~ ws ~> (Exists(_)) |
    '(' ~ ws ~ (
        expression ~ ')' ~ ws |
        query ~ ')' ~ ws ~> (ScalarSubquery(_))
    ) |
    signedPrimary |
    '~' ~ ws ~ expression ~> BitwiseNot
  }

  protected final def signedPrimary: Rule1[Expression] = rule {
    capture(plusOrMinus) ~ ws ~ primary ~> ((s: String, e: Expression) =>
      if (s.charAt(0) == '-') UnaryMinus(e) else e)
  }

  protected final def baseExpression: Rule1[Expression] = rule {
    '*' ~ ws ~> (() => UnresolvedStar(None)) |
    primary
  }

  protected def select: Rule1[LogicalPlan] = rule {
    SELECT ~ (DISTINCT ~> trueFn).? ~
    (projection + (',' ~ ws)) ~
    (FROM ~ relations).? ~
    (WHERE ~ expression).? ~
    (GROUP ~ BY ~ (expression + (',' ~ ws))).? ~
    (HAVING ~ expression).? ~
    sortType.? ~
    (LIMIT ~ expression).? ~> { (d: Option[Boolean], p: Seq[Expression],
        f: Option[LogicalPlan], w: Option[Expression],
        g: Option[Seq[Expression]], h: Option[Expression],
        s: Option[LogicalPlan => LogicalPlan], l: Option[Expression]) =>
      val base = f.getOrElse(OneRowRelation)
      val withFilter = w.map(Filter(_, base)).getOrElse(base)
      val withProjection = g.map(Aggregate(_, p.map(UnresolvedAlias(_, None)),
        withFilter)).getOrElse(Project(p.map(UnresolvedAlias(_, None)), withFilter))
      val withDistinct =
        if (d.isEmpty) withProjection else Distinct(withProjection)
      val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
      val withOrder = s.map(_ (withHaving)).getOrElse(withHaving)
      val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
      withLimit
    }
  }

  protected final def select1: Rule1[LogicalPlan] = rule {
    select | ('(' ~ ws ~ select ~ ')' ~ ws)
  }

  protected def query: Rule1[LogicalPlan] = rule {
    select1.named("select") ~ (
        UNION ~ (
            ALL ~ select1.named("select") ~>
                ((q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2)) |
            DISTINCT.? ~ select1.named("select") ~>
                ((q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)))
        ) |
        INTERSECT ~ select1.named("select") ~>
            ((q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2)) |
        EXCEPT ~ select1.named("select") ~>
            ((q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2))
    ).*
  }

  protected final def insert: Rule1[LogicalPlan] = rule {
    INSERT ~ ((OVERWRITE ~> (() => true)) | (INTO ~> (() => false))) ~
    TABLE.? ~ relation ~ query ~> ((o: Boolean, r: LogicalPlan,
        s: LogicalPlan) => InsertIntoTable(r, Map.empty[String,
        Option[String]], s, o, ifNotExists = false))
  }

  protected final def put: Rule1[LogicalPlan] = rule {
    PUT ~ INTO ~ TABLE.? ~ relation ~ query ~> PutIntoTable
  }

  protected final def withIdentifier: Rule1[LogicalPlan] = rule {
    WITH ~ ((identifier ~ AS ~ '(' ~ ws ~ query ~ ')' ~ ws ~>
        ((id: String, p: LogicalPlan) => (id, p))) + (',' ~ ws)) ~
        (query | insert) ~> ((r: Seq[(String, LogicalPlan)], s: LogicalPlan) =>
        With(s, r.map(ns => (ns._1, SubqueryAlias(ns._1, ns._2))).toMap))
  }

  protected def dmlOperation: Rule1[LogicalPlan] = rule {
    (INSERT ~ INTO | PUT ~ INTO | DELETE ~ FROM | UPDATE) ~ tableIdentifier ~
        ANY.* ~> ((r: TableIdentifier) => DMLExternalTable(r,
        UnresolvedRelation(r), input.sliceString(0, input.length)))
  }

  // DDLs, SET, SHOW etc

  type TableEnd = (Option[String], Option[Map[String, String]],
      Option[LogicalPlan])

  protected def createTable: Rule1[LogicalPlan] = rule {
    CREATE ~ (EXTERNAL ~> trueFn | TEMPORARY ~> falseFn).? ~ TABLE ~
        (IF ~ NOT ~ EXISTS ~> trueFn).? ~ tableIdentifier ~ tableEnd ~> { (
        tempOrExternal: Option[Boolean], ifNotExists: Option[Boolean],
        tableIdent: TableIdentifier, schemaStr: StringBuilder,
        remaining: TableEnd) =>

      val options = remaining._2.getOrElse(Map.empty[String, String])
      val provider = remaining._1.getOrElse(SnappyContext.DEFAULT_SOURCE)
      val allowExisting = ifNotExists.isDefined
      val schemaString = schemaStr.toString().trim

      val hasExternalSchema = if (tempOrExternal.isDefined) false
      else {
        // check if provider class implements ExternalSchemaRelationProvider
        try {
          val clazz: Class[_] = DataSource(session, SnappyContext
              .getProvider(provider, onlyBuiltIn = false)).providingClass
          classOf[ExternalSchemaRelationProvider].isAssignableFrom(clazz)
        } catch {
          case ce: ClassNotFoundException =>
            throw Utils.analysisException(ce.toString)
          case t: Throwable => throw t
        }
      }
      val userSpecifiedSchema = if (hasExternalSchema) None
      else synchronized {
        // parse the schema string expecting Spark SQL format
        val colParser = newInstance()
        colParser.parseSQL(schemaString, colParser.tableColsOrNone.run())
            .map(StructType(_))
      }
      val schemaDDL = if (hasExternalSchema) Some(schemaString) else None

      remaining._3 match {
        case Some(queryPlan) =>
          // When IF NOT EXISTS clause appears in the query,
          // the save mode will be ignore.
          val mode = if (allowExisting) SaveMode.Ignore
          else SaveMode.ErrorIfExists
          tempOrExternal match {
            case None =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = false,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = true)
            case Some(true) =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = false,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = false)
            case Some(false) if remaining._1.isEmpty && remaining._2.isEmpty =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = true,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = false)
            case Some(_) => throw Utils.analysisException(
              "CREATE TEMPORARY TABLE ... USING ... does not allow AS query")
          }
        case None =>
          tempOrExternal match {
            case None =>
              CreateMetastoreTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = true)
            case Some(true) =>
              CreateMetastoreTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = false)
            case Some(false) =>
              CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
                temporary = true, options, Array.empty[String], None,
                allowExisting, managedIfNoPath = false)
          }
      }
    }
  }

  protected def tableEnd2: Rule1[TableEnd] = rule {
    (USING ~ qualifiedName).? ~ (OPTIONS ~ options).? ~ (AS ~ query).? ~ ws ~
        &((';' ~ ws).* ~ EOI) ~> ((provider: Option[String],
        options: Option[Map[String, String]],
        asQuery: Option[LogicalPlan]) => (provider, options, asQuery))
  }

  protected def tableEnd1: Rule[StringBuilder :: HNil,
      StringBuilder :: TableEnd :: HNil] = rule {
    tableEnd2 ~> ((s: StringBuilder, end: TableEnd) => s :: end :: HNil) |
    (capture(ANY ~ Consts.ddlEnd.*) ~> ((s: StringBuilder, n: String) =>
      s.append(n))) ~ tableEnd1
  }

  protected def tableEnd: Rule2[StringBuilder, TableEnd] = rule {
    (capture(Consts.ddlEnd.*) ~> ((s: String) =>
      new StringBuilder().append(s))) ~ tableEnd1
  }

  protected def createIndex: Rule1[LogicalPlan] = rule {
    (CREATE ~ (GLOBAL ~ HASH ~> falseFn | UNIQUE ~> trueFn).? ~ INDEX) ~
        tableIdentifier ~ ON ~ tableIdentifier ~
        colsWithDirection ~ (OPTIONS ~ options).? ~> {
      (indexType: Option[Boolean], indexName: TableIdentifier,
          tableName: TableIdentifier, cols: Map[String, Option[SortDirection]],
          opts: Option[Map[String, String]]) =>
        val parameters = opts.getOrElse(Map.empty[String, String])
        val options = indexType match {
          case Some(false) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "unique")
          case Some(true) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "global hash")
          case None => parameters
        }
        CreateIndex(indexName, tableName, cols, options)
    }
  }

  protected def colsWithDirection: Rule1[Map[String, Option[SortDirection]]] = rule {
    '(' ~ ws ~ (identifier ~ sortDirection.? ~> ((id: String,
        direction: Option[SortDirection]) => (id, direction))).*(',' ~ ws) ~ ws ~
        ')' ~ ws ~> ((cols: Seq[(String, Option[SortDirection])]) => cols.toMap)
  }

  protected def dropIndex: Rule1[LogicalPlan] = rule {
    DROP ~ INDEX ~ (IF ~ EXISTS ~> trueFn).? ~ tableIdentifier ~>
        ((ifExists: Option[Boolean], indexName: TableIdentifier) =>
          DropIndex(indexName, ifExists.isDefined))
  }

  protected def dropTable: Rule1[LogicalPlan] = rule {
    DROP ~ TABLE ~ (IF ~ EXISTS ~> trueFn).? ~ tableIdentifier ~>
        ((allowExisting: Option[Boolean], tableIdent: TableIdentifier) =>
          DropTable(tableIdent, allowExisting.isDefined))
  }

  protected def truncateTable: Rule1[LogicalPlan] = rule {
    TRUNCATE ~ TABLE ~ tableIdentifier ~> TruncateTable
  }

  protected def createStream: Rule1[LogicalPlan] = rule {
    CREATE ~ STREAM ~ TABLE ~ tableIdentifier ~ (IF ~ NOT ~ EXISTS ~>
        trueFn).? ~ tableCols.? ~ USING ~ qualifiedName ~ OPTIONS ~ options ~> {
      (streamIdent: TableIdentifier, ifNotExists: Option[Boolean],
          cols: Option[Seq[StructField]], pname: String,
          opts: Map[String, String]) =>
        val specifiedSchema = cols.map(fields => StructType(fields))
        val provider = SnappyContext.getProvider(pname, onlyBuiltIn = false)
        // check that the provider is a stream relation
        val clazz = DataSource(session, provider).providingClass
        if (!classOf[StreamPlanProvider].isAssignableFrom(clazz)) {
          throw Utils.analysisException(s"CREATE STREAM provider $pname" +
              " does not implement StreamPlanProvider")
        }
        // provider has already been resolved, so isBuiltIn==false allows
        // for both builtin as well as external implementations
        CreateMetastoreTableUsing(streamIdent, None, specifiedSchema, None,
          provider, ifNotExists.isDefined, opts, isBuiltIn = false)
    }
  }

  protected def streamContext: Rule1[LogicalPlan] = rule {
    STREAMING ~ (INIT ~> (() => 0) | START ~> (() => 1) | STOP ~> (() => 2)) ~
        durationUnit ~ ws ~> SnappyStreamingActionsCommand
  }

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,
   *   column_type,comment
   */
  protected def describeTable: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ (EXTENDED ~> trueFn).? ~ tableIdentifier ~>
        ((extended: Option[Boolean], tableIdent: TableIdentifier) =>
          DescribeTableCommand(tableIdent, extended.isDefined,
            isFormatted = false))
  }

  protected def refreshTable: Rule1[LogicalPlan] = rule {
    REFRESH ~ TABLE ~ tableIdentifier ~> RefreshTable
  }

  protected def cache: Rule1[LogicalPlan] = rule {
    CACHE ~ (LAZY ~> trueFn).? ~ TABLE ~ tableIdentifier ~
        (AS ~ query).? ~> ((isLazy: Option[Boolean],
        tableIdent: TableIdentifier, plan: Option[LogicalPlan]) =>
      CacheTableCommand(tableIdent, plan, isLazy.isDefined))
  }

  protected def uncache: Rule1[LogicalPlan] = rule {
    UNCACHE ~ TABLE ~ tableIdentifier ~> UncacheTableCommand |
    CLEAR ~ CACHE ~> (() => ClearCacheCommand)
  }

  protected def set: Rule1[LogicalPlan] = rule {
    SET ~ (
        CURRENT.? ~ SCHEMA ~ '='.? ~ ws ~ identifier ~>
            ((schemaName: String) => SetSchema(schemaName)) |
        capture(ANY.*) ~> { (rest: String) =>
          val separatorIndex = rest.indexOf('=')
          if (separatorIndex >= 0) {
            val key = rest.substring(0, separatorIndex).trim
            val value = rest.substring(separatorIndex + 1).trim
            SetCommand(Some(key -> Option(value)))
          } else if (rest.nonEmpty) {
            SetCommand(Some(rest.trim -> None))
          } else {
            SetCommand(None)
          }
        }
    )
  }

  // It can be the following patterns:
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  protected def show: Rule1[LogicalPlan] = rule {
    SHOW ~ TABLES ~ ((FROM | IN) ~ identifier).? ~> ((ident: Option[String]) =>
      ShowTablesCommand(ident, None)) |
    SHOW ~ identifier.? ~ FUNCTIONS ~ LIKE.? ~
        (tableIdentifier | stringLiteral).? ~> { (id: Option[String],
        nameOrPat: Option[Any]) =>
      val (user, system) = id.map(_.toLowerCase) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) =>
          throw Utils.analysisException(s"SHOW $x FUNCTIONS not supported")
      }
      nameOrPat match {
        case Some(name: TableIdentifier) => ShowFunctionsCommand(
          name.database, Some(name.table), user, system)
        case Some(pat: String) => ShowFunctionsCommand(
          None, Some(ParserUtils.unescapeSQLString(pat)), user, system)
        case None => ShowFunctionsCommand(None, None, user, system)
        case _ => throw Utils.analysisException(
          s"SHOW FUNCTIONS $nameOrPat unexpected")
      }
    }
  }

  protected def desc: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ FUNCTION ~ (EXTENDED ~> trueFn).? ~
        (identifier | stringLiteral) ~> ((extended: Option[Boolean],
        functionName: String) => DescribeFunctionCommand(
      FunctionIdentifier(functionName), extended.isDefined))
  }

  protected final def qualifiedName: Rule1[String] = rule {
    capture((Consts.identifier | '.').*) ~ delimiter
  }

  protected def column: Rule1[StructField] = rule {
    identifier ~ columnDataType ~ ((NOT ~> trueFn).? ~ NULL).? ~
        (COMMENT ~ stringLiteral).? ~> { (columnName: String,
        t: DataType, notNull: Option[Option[Boolean]], cm: Option[String]) =>
      val builder = new MetadataBuilder()
      val empty = t match {
        case CharType(size, fixed) => builder.putString("size", size.toString)
            .putString("fixed", fixed.toString)
          false
        case _ => true
      }
      val metadata = cm match {
        case Some(comment) => builder.putString(
          Consts.COMMENT.lower, comment).build()
        case None => if (empty) Metadata.empty else builder.build()
      }
      StructField(columnName, t, notNull.isEmpty ||
          notNull.get.isEmpty, metadata)
    }
  }

  protected final def tableCols: Rule1[Seq[StructField]] = rule {
    '(' ~ ws ~ (column + (',' ~ ws)) ~ ')' ~ ws
  }

  protected final def tableColsOrNone: Rule1[Option[Seq[StructField]]] = rule {
    tableCols ~> (Some(_)) | ws ~> (() => None)
  }

  protected final def pair: Rule1[(String, String)] = rule {
    qualifiedName ~ stringLiteral ~ ws ~> ((k: String, v: String) => k -> v)
  }

  protected final def options: Rule1[Map[String, String]] = rule {
    '(' ~ ws ~ (pair * (',' ~ ws)) ~ ')' ~ ws ~>
        ((pairs: Seq[(String, String)]) => pairs.toMap)
  }

  protected def ddl: Rule1[LogicalPlan] = rule {
    createTable | describeTable | refreshTable | dropTable | truncateTable |
        createStream | streamContext |
        createIndex | dropIndex
  }

  override protected def start: Rule1[LogicalPlan] = rule {
    query.named("select") | insert | put | dmlOperation | withIdentifier |
        ddl | set | cache | uncache | show | desc
  }

  def parse[T](sqlText: String, parseRule: => Try[T]): T = synchronized {
    session.queryHints.clear()
    parseSQL(sqlText, parseRule)
  }

  protected def parseSQL[T](sqlText: String, parseRule: => Try[T]): T = {
    this.input = sqlText
    val plan = parseRule match {
      case Success(p) => p
      case Failure(e: ParseError) =>
        throw Utils.analysisException(formatError(e))
      case Failure(e) =>
        throw Utils.analysisException(e.toString, Some(e))
    }
    if (queryHints.nonEmpty) {
      session.queryHints ++= queryHints
    }
    plan
  }

  protected def newInstance(): SnappyParser = new SnappyParser(session)
}

private[sql] case class CreateMetastoreTableUsing(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String],
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.sessionState.asInstanceOf[SnappySessionState].catalog
        .newQualifiedTableName(tableIdent), provider, userSpecifiedSchema,
      schemaDDL, mode, snc.addBaseTableOption(baseTable, options), isBuiltIn)
    Seq.empty
  }
}

private[sql] case class CreateMetastoreTableUsingSelect(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    temporary: Boolean,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.asInstanceOf[SnappySessionState].catalog
    if (temporary) {
      // the equivalent of a registerTempTable of a DataFrame
      if (tableIdent.database.isDefined) {
        throw Utils.analysisException(
          s"Temporary table '$tableIdent' should not have specified a database")
      }
      Dataset.ofRows(session, query).createTempView(tableIdent.table)
    } else {
      snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
        userSpecifiedSchema, schemaDDL, partitionColumns, mode,
        snc.addBaseTableOption(baseTable, options), query, isBuiltIn)
    }
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableIdent: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.asInstanceOf[SnappySessionState].catalog
    snc.dropTable(catalog.newQualifiedTableName(tableIdent), ifExists)
    Seq.empty
  }
}

private[sql] case class TruncateTable(
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.asInstanceOf[SnappySessionState].catalog
    snc.truncateTable(catalog.newQualifiedTableName(tableIdent))
    Seq.empty
  }
}

private[sql] case class CreateIndex(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.asInstanceOf[SnappySessionState].catalog
    val tableIdent = catalog.newQualifiedTableName(baseTable)
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.createIndex(indexIdent, tableIdent, indexColumns, options)
    Seq.empty
  }
}

private[sql] case class DropIndex(
    indexName: TableIdentifier,
    ifExists : Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.asInstanceOf[SnappySessionState].catalog
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.dropIndex(indexIdent, ifExists)
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

private[sql] case class SetSchema(schemaName: String) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setSchema(schemaName)
    Seq.empty[Row]
  }
}

private[sql] case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Duration) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      new SnappyStreamingContext(session.sparkContext, batchInterval)
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => // TODO .We should create a named Streaming
          // Context and check if the configurations match
          case None => SnappyStreamingContext.getActiveOrCreate(creatingFunc)
        }
      case 1 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => x.start()
          case None => throw Utils.analysisException(
            "Streaming Context has not been initialized")
        }
      case 2 =>
        val ssc = SnappyStreamingContext.getActive()
        ssc match {
          case Some(strCtx) => strCtx.stop(stopSparkContext = false,
            stopGracefully = true)
          case None => // throw Utils.analysisException(
          // "There is no running Streaming Context to be stopped")
        }
    }
    Seq.empty[Row]
  }
}
