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

import java.sql.SQLException
import java.util.regex.Pattern

import scala.language.implicitConversions

import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.SnappyParserConsts.{plusOrMinus, trueFn}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.{ParserDialect, SqlLexical, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect, DDLException, DDLParser, ResolvedDataSource}
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.sources.{ExternalSchemaRelationProvider, PutIntoTable}
import org.apache.spark.sql.streaming.{StreamPlanProvider, WindowLogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds, SnappyStreamingContext}
import org.apache.spark.unsafe.types.CalendarInterval

class SnappyParser(context: SnappyContext)
    extends SnappyBaseParser(context) {

  private[this] final var _input: ParserInput = _

  override final def input: ParserInput = _input

  private[sql] final def input_=(in: ParserInput): Unit = {
    reset()
    _input = in
  }

  final def ALL = rule { keyword(SnappyParserConsts.ALL) }
  final def AND = rule { keyword(SnappyParserConsts.AND) }
  final def APPROXIMATE = rule { keyword(SnappyParserConsts.APPROXIMATE) }
  final def AS = rule { keyword(SnappyParserConsts.AS) }
  final def ASC = rule { keyword(SnappyParserConsts.ASC) }
  final def BETWEEN = rule { keyword(SnappyParserConsts.BETWEEN) }
  final def BY = rule { keyword(SnappyParserConsts.BY) }
  final def CASE = rule { keyword(SnappyParserConsts.CASE) }
  final def CAST = rule { keyword(SnappyParserConsts.CAST) }
  final def DELETE = rule { keyword(SnappyParserConsts.DELETE) }
  final def DESC = rule { keyword(SnappyParserConsts.DESC) }
  final def DISTINCT = rule { keyword(SnappyParserConsts.DISTINCT) }
  final def ELSE = rule { keyword(SnappyParserConsts.ELSE) }
  final def END = rule { keyword(SnappyParserConsts.END) }
  final def EXCEPT = rule { keyword(SnappyParserConsts.EXCEPT) }
  final def EXISTS = rule { keyword(SnappyParserConsts.EXISTS) }
  final def FALSE = rule { keyword(SnappyParserConsts.FALSE) }
  final def FROM = rule { keyword(SnappyParserConsts.FROM) }
  final def FULL = rule { keyword(SnappyParserConsts.FULL) }
  final def GROUP = rule { keyword(SnappyParserConsts.GROUP) }
  final def HAVING = rule { keyword(SnappyParserConsts.HAVING) }
  final def IN = rule { keyword(SnappyParserConsts.IN) }
  final def INNER = rule { keyword(SnappyParserConsts.INNER) }
  final def INSERT = rule { keyword(SnappyParserConsts.INSERT) }
  final def INTERSECT = rule { keyword(SnappyParserConsts.INTERSECT) }
  final def INTERVAL = rule { keyword(SnappyParserConsts.INTERVAL) }
  final def INTO = rule { keyword(SnappyParserConsts.INTO) }
  final def IS = rule { keyword(SnappyParserConsts.IS) }
  final def JOIN = rule { keyword(SnappyParserConsts.JOIN) }
  final def LEFT = rule { keyword(SnappyParserConsts.LEFT) }
  final def LIKE = rule { keyword(SnappyParserConsts.LIKE) }
  final def LIMIT = rule { keyword(SnappyParserConsts.LIMIT) }
  final def NOT = rule { keyword(SnappyParserConsts.NOT) }
  final def NULL = rule { keyword(SnappyParserConsts.NULL) }
  final def ON = rule { keyword(SnappyParserConsts.ON) }
  final def OR = rule { keyword(SnappyParserConsts.OR) }
  final def ORDER = rule { keyword(SnappyParserConsts.ORDER) }
  final def OUTER = rule { keyword(SnappyParserConsts.OUTER) }
  final def OVERWRITE = rule { keyword(SnappyParserConsts.OVERWRITE) }
  final def PUT = rule { keyword(SnappyParserConsts.PUT) }
  final def REGEXP = rule { keyword(SnappyParserConsts.REGEXP) }
  final def RIGHT = rule { keyword(SnappyParserConsts.RIGHT) }
  final def RLIKE = rule { keyword(SnappyParserConsts.RLIKE) }
  final def SELECT = rule { keyword(SnappyParserConsts.SELECT) }
  final def SEMI = rule { keyword(SnappyParserConsts.SEMI) }
  final def SORT = rule { keyword(SnappyParserConsts.SORT) }
  final def TABLE = rule { keyword(SnappyParserConsts.TABLE) }
  final def THEN = rule { keyword(SnappyParserConsts.THEN) }
  final def TO = rule { keyword(SnappyParserConsts.TO) }
  final def TRUE = rule { keyword(SnappyParserConsts.TRUE) }
  final def UNION = rule { keyword(SnappyParserConsts.UNION) }
  final def UPDATE = rule { keyword(SnappyParserConsts.UPDATE) }
  final def WHEN = rule { keyword(SnappyParserConsts.WHEN) }
  final def WHERE = rule { keyword(SnappyParserConsts.WHERE) }
  final def WITH = rule { keyword(SnappyParserConsts.WITH) }
  // interval units are not reserved (handled in SnappyParserConsts singleton)
  final def DAY = rule { intervalUnit(SnappyParserConsts.DAY) }
  final def HOUR = rule { intervalUnit(SnappyParserConsts.HOUR) }
  final def MICROSECOND = rule { intervalUnit(SnappyParserConsts.MICROSECOND) }
  final def MILLISECOND = rule { intervalUnit(SnappyParserConsts.MILLISECOND) }
  final def MINUTE = rule { intervalUnit(SnappyParserConsts.MINUTE) }
  final def MONTH = rule { intervalUnit(SnappyParserConsts.MONTH) }
  final def SECOND = rule { intervalUnit(SnappyParserConsts.SECOND) }
  final def WEEK = rule { intervalUnit(SnappyParserConsts.WEEK) }
  final def YEAR = rule { intervalUnit(SnappyParserConsts.YEAR) }
  // Added for streaming window CQs
  final def DURATION = rule { keyword(SnappyParserConsts.DURATION) }
  final def SLIDE = rule { keyword(SnappyParserConsts.SLIDE) }
  final def WINDOW = rule { keyword(SnappyParserConsts.WINDOW) }

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
    capture(plusOrMinus.? ~ SnappyParserConsts.numeric.+) ~ delimiter ~>
        ((s: String) => toNumericLiteral(s))
  }

  protected final def literal: Rule1[Literal] = rule {
    stringLiteral ~> ((s: String) => Literal.create(s, StringType)) |
    numericLiteral |
    booleanLiteral |
    NULL ~> (() => Literal.create(null, NullType)) |
    intervalLiteral
  }

  private def intervalUnit(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower) ~ SnappyParserConsts.plural.?) ~ delimiter
  }

  protected def month: Rule1[Int] = rule {
    integral ~ MONTH ~> ((num: String) => num.toInt)
  }

  protected def year: Rule1[Int] = rule {
    integral ~ YEAR ~> ((num: String) => num.toInt * 12)
  }

  protected def microsecond: Rule1[Long] = rule {
    integral ~ MICROSECOND ~> ((num: String) => num.toLong)
  }

  protected def millisecond: Rule1[Long] = rule {
    integral ~ MILLISECOND ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_MILLI)
  }

  protected def second: Rule1[Long] = rule {
    integral ~ SECOND ~> ((num: String) =>
      num.toLong * CalendarInterval.MICROS_PER_SECOND)
  }

  protected def minute: Rule1[Long] = rule {
    integral ~ MINUTE ~> ((num: String) =>
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
            DAY ~ TO ~ SECOND ~> ((s: String) =>
              Literal(CalendarInterval.fromDayTimeString(s))) |
            YEAR ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("year", s))) |
            MONTH ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("month", s))) |
            DAY ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("day", s))) |
            HOUR ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("hour", s))) |
            MINUTE ~> ((s: String) =>
              Literal(CalendarInterval.fromSingleUnitString("minute", s))) |
            SECOND ~> ((s: String) =>
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

  protected final def expression: Rule1[Expression] = rule {
    andExpression ~ (OR ~ andExpression ~>
        ((e1: Expression, e2: Expression) => Or(e1, e2))).*
  }

  protected final def andExpression: Rule1[Expression] = rule {
    notExpression ~ (AND ~ notExpression ~>
        ((e1: Expression, e2: Expression) => And(e1, e2))).*
  }

  protected final def notExpression: Rule1[Expression] = rule {
    (NOT ~> trueFn).? ~ comparisonExpression ~> ((not: Option[Boolean],
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
        IN ~ '(' ~ ws ~ (termExpression * (',' ~ ws)) ~ ')' ~ ws ~>
            ((e1: Expression, e2: Seq[Expression]) => In(e1, e2)) |
        LIKE ~ termExpression ~>
            ((e1: Expression, e2: Expression) => Like(e1, e2)) |
        IS ~ (NOT ~> trueFn).? ~ NULL ~>
            ((e: Expression, not: Option[Boolean]) =>
              if (not.isEmpty) IsNull(e) else IsNotNull(e)) |
        BETWEEN ~ termExpression ~ AND ~ termExpression ~>
            ((e: Expression, el: Expression, eu: Expression) =>
              And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))) |
        NOT ~ (
            IN ~ '(' ~ ws ~ (termExpression * (',' ~ ws)) ~ ')' ~ ws ~>
                ((e1: Expression, e2: Seq[Expression]) => Not(In(e1, e2))) |
            LIKE ~ termExpression ~>
                ((e1: Expression, e2: Expression) => Not(Like(e1, e2))) |
            BETWEEN ~ termExpression ~ AND ~ termExpression ~>
                ((e: Expression, el: Expression, eu: Expression) =>
                  Not(And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))))
        ) |
        comparisonExpression1 |
        MATCH.asInstanceOf[Rule[Expression::HNil, Expression::HNil]]
    )
  }

  protected def comparisonExpression1: Rule[Expression :: HNil,
      Expression :: HNil] = rule {
    (RLIKE | REGEXP) ~ termExpression ~>
        ((e1: Expression, e2: Expression) => RLike(e1, e2))
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
        capture(SnappyParserConsts.arithmeticOperator) ~ ws ~
            baseExpression ~> ((e1: Expression, op: String, e2: Expression) =>
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
        MILLISECOND ~> ((s: String) => Milliseconds(s.toInt)) |
        SECOND ~> ((s: String) => Seconds(s.toInt)) |
        MINUTE ~> ((s: String) => Minutes(s.toInt))
    )
  }

  protected def windowOptions: Rule1[(Duration, Option[Duration])] = rule {
    WINDOW ~ '(' ~ ws ~ DURATION ~ durationUnit ~ (',' ~ ws ~
        SLIDE ~ durationUnit).? ~ ')' ~ ws ~>
        ((d: Duration, s: Option[Duration]) => (d, s))
  }

  protected final def relationFactor: Rule1[LogicalPlan] = rule {
    tableIdentifier ~ windowOptions.? ~ (AS.? ~ identifier).? ~>
        ((tableIdent: QualifiedTableName,
            window: Option[(Duration, Option[Duration])],
            alias: Option[String]) => window match {
          case None => UnresolvedRelation(tableIdent, alias)
          case Some(win) => WindowLogicalPlan(win._1, win._2,
            UnresolvedRelation(tableIdent, alias))
        }) |
    '(' ~ ws ~ start ~ ')' ~ ws ~ windowOptions.? ~ AS.? ~ identifier ~>
        ((child: LogicalPlan, window: Option[(Duration, Option[Duration])],
            alias: String) => window match {
          case None => Subquery(alias, child)
          case Some(win) => WindowLogicalPlan(win._1, win._2,
            Subquery(alias, child))
        })
  }

  protected final def joinConditions: Rule1[Expression] = rule {
    ON ~ expression
  }

  protected final def joinType: Rule1[JoinType] = rule {
    INNER ~> (() => Inner) |
    LEFT ~ SEMI ~> (() => LeftSemi) |
    LEFT ~ OUTER.? ~> (() => LeftOuter) |
    RIGHT ~ OUTER.? ~> (() => RightOuter) |
    FULL ~ OUTER.? ~> (() => FullOuter)
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

  protected final def whenThenElse: Rule1[Seq[Expression]] = rule {
    (WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression,
        t: Expression) => (w, t))).+ ~ (ELSE ~ expression).? ~ END ~>
        ((altPart: Seq[(Expression, Expression)], elsePart: Option[Expression]) =>
          altPart.flatMap(e => Seq(e._1, e._2)) ++ elsePart)
  }

  protected def specialFunction: Rule1[Expression] = rule {
    CASE ~ (
        whenThenElse ~> CaseWhen |
        expression ~ whenThenElse ~> CaseKeyWhen
    ) |
    APPROXIMATE ~ ('(' ~ ws ~ unsignedFloat ~ ')' ~ ws).? ~
        identifier ~ '(' ~ ws ~ DISTINCT ~ expression ~ ')' ~ ws ~>
        ((s: Option[String], udfName: String, exp: Expression) =>
          if (udfName == "COUNT") {
            AggregateExpression(
              if (s.isEmpty) HyperLogLogPlusPlus(exp)
              else HyperLogLogPlusPlus(exp, s.get.toDouble),
              mode = Complete, isDistinct = false)
          } else {
            throw Utils.analysisException(
              s"invalid function approximate $udfName")
          })
  }

  protected def primary: Rule1[Expression] = rule {
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
              } else if (udfName == "COUNT") {
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
    '(' ~ ws ~ expression ~ ')' ~ ws |
    cast |
    specialFunction |
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
      val withProjection = g.map(Aggregate(_, p.map(UnresolvedAlias),
        withFilter)).getOrElse(Project(p.map(UnresolvedAlias), withFilter))
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

  protected def insert: Rule1[LogicalPlan] = rule {
    INSERT ~ ((OVERWRITE ~> (() => true)) | (INTO ~> (() => false))) ~
    TABLE.? ~ relation ~ select ~> ((o: Boolean, r: LogicalPlan,
        s: LogicalPlan) => InsertIntoTable(r, Map.empty[String, Option[String]],
        s, o, ifNotExists = false))
  }

  protected def put: Rule1[LogicalPlan] = rule {
    PUT ~ INTO ~ TABLE.? ~ relation ~ select ~> ((r: LogicalPlan,
        s: LogicalPlan) => PutIntoTable(r, s))
  }

  protected def withIdentifier: Rule1[LogicalPlan] = rule {
    WITH ~ ((identifier ~ AS ~ '(' ~ ws ~ query ~ ')' ~ ws ~>
        ((id: String, p: LogicalPlan) => (id, p))) + (',' ~ ws)) ~
        (query | insert) ~> ((r: Seq[(String, LogicalPlan)], s: LogicalPlan) =>
        With(s, r.map(ns => (ns._1, Subquery(ns._1, ns._2))).toMap))
  }

  protected def dmlOperation: Rule1[LogicalPlan] = rule {
    (INSERT ~ INTO | PUT ~ INTO | DELETE ~ FROM | UPDATE) ~ tableIdentifier ~
        ANY.* ~> ((r: QualifiedTableName) => DMLExternalTable(r,
        UnresolvedRelation(r), input.sliceString(0, input.length)))
  }

  override protected def start: Rule1[LogicalPlan] = rule {
    query.named("select") | insert | put | withIdentifier | dmlOperation
  }
}

/**
 * Snappy dialect uses a much more optimized parser and adds SnappyParser
 * additions to the standard "sql" dialect.
 */
private[sql] class SnappyParserDialect(context: SnappyContext)
    extends ParserDialect {

  @transient private[sql] val sqlParser = new SnappyParser(context)

  override def parse(sqlText: String): LogicalPlan = synchronized {
    val sqlParser = this.sqlParser
    sqlParser.input = sqlText
    val plan = sqlParser.parse()
    context.queryHints.clear()
    if (sqlParser.queryHints.nonEmpty) {
      context.queryHints ++= sqlParser.queryHints
    }
    plan
  }
}

final class SnappyLexical(caseSensitive: Boolean) extends SqlLexical {

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token)
    else if (caseSensitive) {
      Identifier(name)
    } else {
      Identifier(Utils.toUpperCase(name))
    }
  }
}

/**
 * Snappy DDL extensions for streaming and sampling.
 */
private[sql] class SnappyDDLParser(caseSensitive: Boolean,
    parseQuery: String => LogicalPlan) extends DDLParser(parseQuery) {

  override val lexical = new SnappyLexical(caseSensitive)

  override def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        throw new SQLException(failureOrError.toString, "42X01")
    }
  }

  override def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {

    try {
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      case t: SQLException if !exceptionOnError =>
        parseQuery(input)
    }
  }

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | describeTable | refreshTable | dropTable |
        createStream  | streamContext | truncateTable | createIndex | dropIndex

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
  protected val GLOBAL = Keyword("GLOBAL")
  protected val HASH = Keyword("HASH")
  protected val UNIQUE = Keyword("UNIQUE")
  protected val ASC = Keyword("ASC")
  protected val DESC = Keyword("DESC")


  protected override lazy val className: Parser[String] =
    repsep(ident, ".") ^^ { case s =>
      // A workaround to address lack of case information at this point.
      // If value is all CAPS then convert to lowercase else preserve case.
      if (s.exists(Utils.hasLowerCase)) s.mkString(".")
      else s.map(Utils.toLowerCase).mkString(".")
    }

  private val DDLEnd = Pattern.compile(USING.str + "\\s+[a-zA-Z_0-9\\.]+\\s*" +
      s"(\\s${OPTIONS.str}|\\s${AS.str}|$$)|\\s${AS.str}", Pattern.CASE_INSENSITIVE)

  protected override lazy val createTable: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~
        tableIdentifier ~ externalTableInput ~ (USING ~> className).? ~
        (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temporary ~ allowExisting ~ tableIdent ~ schemaString ~
          providerName ~ opts ~ query =>

        val options = opts.getOrElse(Map.empty[String, String])
        val provider = SnappyContext.getProvider(providerName.getOrElse(
          SnappyContext.DEFAULT_SOURCE), onlyBuiltin = false)
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
            CreateMetastoreTableUsingSelect(tableIdent, provider,
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
              case Success(columns, _) =>
                columns.flatMap(fields => Some(StructType(fields)))
              case failure =>
                throw new DDLException(failure.toString)
            }
          }
          val schemaDDL = if (hasExternalSchema) Some(schemaString) else None

          if (temporary.isDefined) {
            CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
              temporary = true, options, allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateMetastoreTableUsing(tableIdent, userSpecifiedSchema,
              schemaDDL, provider, allowExisting.isDefined, options)
          }
        }
    }

  // This is the same as tableIdentifier in SnappyParser.
  protected override lazy val tableIdentifier: Parser[QualifiedTableName] =
    (ident <~ ".").? ~ ident ^^ {
      case maybeSchemaName ~ tableName =>
        new QualifiedTableName(maybeSchemaName, tableName)
    }

  protected override lazy val primitiveType: Parser[DataType] =
    "(?i)(?:string|clob)".r ^^^ StringType |
    "(?i)(?:int|integer)".r ^^^ IntegerType |
    "(?i)(?:bigint|long)".r ^^^ LongType |
    fixedDecimalType |
    "(?i)(?:decimal|numeric)".r ^^^ DecimalType.SYSTEM_DEFAULT |
    "(?i)double".r ^^^ DoubleType |
    "(?i)(?:float|real)".r ^^^ FloatType |
    "(?i)(?:binary|blob)".r ^^^ BinaryType |
    "(?i)date".r ^^^ DateType |
    "(?i)timestamp".r ^^^ TimestampType |
    "(?i)(?:smallint|short)".r ^^^ ShortType |
    "(?i)(?:tinyint|byte)".r ^^^ ByteType |
    "(?i)boolean".r ^^^ BooleanType |
    varchar

  protected override lazy val fixedDecimalType: Parser[DataType] =
    ("(?i)(?:decimal|numeric)".r ~> "(" ~> numericLit) ~ ("," ~> numericLit <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val createIndex: Parser[LogicalPlan] =
    (CREATE ~> (GLOBAL ~ HASH | UNIQUE).? <~ INDEX) ~
      (tableIdentifier) ~ (ON ~> tableIdentifier) ~
      colWithDirection ~ (OPTIONS ~> options).? ^^ {
      case indexType ~ indexName ~ tableName ~ cols ~ opts =>
        val parameters = opts.getOrElse(Map.empty[String, String])
        if (indexType.isDefined) {
          val typeString = indexType match {
            case Some("unique") =>
              "unique"
            case Some(x) if x.toString.equals("(global~hash)") =>
              "global hash"
          }
          CreateIndex(indexName.toString, tableName,
            cols, parameters + (ExternalStoreUtils.INDEX_TYPE -> typeString))
        } else {
          CreateIndex(indexName.toString, tableName, cols, parameters)
        }

    }

  protected lazy val colWithDirection: Parser[Map[String, Option[SortDirection]]] =
    ("(" ~> repsep(ident ~ direction.?, ",") <~ ")")  ^^ {
    case exp => exp.map(pair => (pair._1, pair._2) ).toMap
  }

  protected lazy val dropIndex: Parser[LogicalPlan] =
    DROP ~> INDEX ~> (IF ~> EXISTS).? ~ tableIdentifier ^^ {
      case ifExists ~ indexName =>
        DropIndex(indexName.toString, ifExists.isDefined)
    }

  protected lazy val direction: Parser[SortDirection] =
    ( ASC  ^^^ Ascending
      | DESC ^^^ Descending
    )


  protected lazy val dropTable: Parser[LogicalPlan] =
    (DROP ~> TEMPORARY.? <~ TABLE) ~ (IF ~> EXISTS).? ~ tableIdentifier ^^ {
      case temporary ~ allowExisting ~ tableName =>
        DropTable(tableName, temporary.isDefined, allowExisting.isDefined)
    }

  protected lazy val truncateTable: Parser[LogicalPlan] =
    (TRUNCATE ~> TEMPORARY.? <~ TABLE) ~ tableIdentifier ^^ {
      case temporary ~ tableName =>
        TruncateTable(tableName, temporary.isDefined)
    }

  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> STREAM ~> TABLE ~> tableIdentifier) ~ (IF ~> NOT <~ EXISTS).? ~
        tableCols.? ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case streamName ~ allowExisting ~ cols ~ providerName ~ opts =>
        val specifiedSchema = cols.flatMap(fields => Some(StructType(fields)))
        val provider = SnappyContext.getProvider(providerName,
          onlyBuiltin = false)
        // check that the provider is a stream relation
        val clazz = ResolvedDataSource.lookupDataSource(provider)
        if (!classOf[StreamPlanProvider].isAssignableFrom(clazz)) {
          throw Utils.analysisException(s"CREATE STREAM provider $providerName" +
              " does not implement StreamPlanProvider")
        }
        CreateMetastoreTableUsing(streamName, specifiedSchema, None,
          provider, allowExisting.isDefined, opts, onlyExternal = false)
    }

  protected lazy val streamContext: Parser[LogicalPlan] =
    (STREAMING ~>
        (INIT ^^^ 0 | START ^^^ 1 | STOP ^^^ 2) ~ numericLit.?) ^^ {
      case action ~ batchInterval =>
        if (batchInterval.isDefined) {
          StreamOperationsLogicalPlan(action, Some(batchInterval.get.toInt))
        } else {
          StreamOperationsLogicalPlan(action, None)
        }
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

private[sql] case class CreateMetastoreTableUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String],
    onlyExternal: Boolean = false) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.catalog.newQualifiedTableName(tableIdent), provider,
      userSpecifiedSchema, schemaDDL, mode, options,
      onlyBuiltIn = false, onlyExternal)
    Seq.empty
  }
}

private[sql] case class CreateMetastoreTableUsingSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    onlyExternal: Boolean = false) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snc.catalog
    val qualifiedName = catalog.newQualifiedTableName(tableIdent)
    snc.createTable(qualifiedName, provider, partitionColumns, mode,
      options, query, onlyBuiltIn = false, onlyExternal)
    // refresh cache of the table in catalog
    catalog.invalidateTable(qualifiedName)
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableIdent: QualifiedTableName,
    temporary: Boolean,
    ifExists: Boolean) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.dropTable(snc.catalog.newQualifiedTableName(tableIdent), ifExists)
    Seq.empty
  }
}

private[sql] case class TruncateTable(
    tableIdent: QualifiedTableName,
    temporary: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.truncateTable(snc.catalog.newQualifiedTableName(tableIdent))
    Seq.empty
  }
}

private[sql] case class CreateIndex(indexName: String,
    baseTable: QualifiedTableName,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.createIndex(indexName, baseTable.toString,
      indexColumns, options)
    Seq.empty
  }
}

private[sql] case class DropIndex(
    indexName: String,
    ifExists : Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.dropIndex(indexName, ifExists)
    Seq.empty
  }
}

case class DMLExternalTable(
    tableName: QualifiedTableName,
    child: LogicalPlan,
    command: String)
    extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil

  override def output: Seq[Attribute] = child.output
}

private[sql] case class StreamOperationsLogicalPlan(action: Int,
    batchInterval: Option[Int])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}


private[sql] case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Int])
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      new SnappyStreamingContext(sqlContext.sparkContext, Seconds(batchInterval.get))
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => // TODO .We should create a named Streaming Context and check if the configurations match
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
          case Some(strCtx) => strCtx.stop(stopSparkContext = false, stopGracefully = true)
          case None => //throw Utils.analysisException(
            //"There is no running Streaming Context to be stopped")
        }
    }
    Seq.empty[Row]
  }
}
