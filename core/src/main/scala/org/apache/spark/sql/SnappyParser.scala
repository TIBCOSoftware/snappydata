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

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import com.pivotal.gemfirexd.internal.iapi.types.SQLDecimal
import io.snappydata.{Constant, QueryHint}
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.SnappyParserConsts.{falseFn, plusOrMinus, trueFn}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.CachedPlanHelperExec
import org.apache.spark.sql.sources.{Delete, PutIntoTable, Update}
import org.apache.spark.sql.streaming.WindowLogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming.Duration
import org.apache.spark.unsafe.types.CalendarInterval

class SnappyParser(session: SnappySession)
    extends SnappyDDLParser(session) {

  private[this] final var _input: ParserInput = _

  override final def input: ParserInput = _input

  private var paramcounter = 0
  private[sql] final def input_=(in: ParserInput): Unit = {
    reset()
    _input = in
    paramcounter = 0
    session.sessionState.questionMarkCounter = 0
    tokenize = false
  }

  protected final type WhenElseType = (Seq[(Expression, Expression)],
      Option[Expression])
  protected final type JoinRuleType = (Option[JoinType], LogicalPlan,
      Option[Expression])

  private def toDecimalOrDoubleLiteral(s: String,
      scientific: Boolean): Literal = {
    // follow the behavior in MS SQL Server
    // https://msdn.microsoft.com/en-us/library/ms179899.aspx
    if (scientific) {
      Literal(s.toDouble, DoubleType)
    } else {
      val decimal = new java.math.BigDecimal(s, BigDecimal.defaultMathContext)
      // try to use SYSTEM_DEFAULT instead of creating new DecimalType which
      // is expensive (due to typeTag etc resolved by reflection in AtomicType)
      val sysDefaultType = DecimalType.SYSTEM_DEFAULT
      if (decimal.precision <= sysDefaultType.precision &&
          decimal.scale <= sysDefaultType.scale) {
        Literal(Decimal(decimal), sysDefaultType)
      } else {
        Literal(decimal)
      }
    }
  }

  private def toNumericLiteral(s: String): Literal = {
    // quick pass through the string to check for floats
    var noDecimalPoint = true
    var index = 0
    val len = s.length
    val lastChar = s.charAt(len - 1)
    // use double if ending with 'D'
    if (lastChar == 'D') {
      return Literal(java.lang.Double.parseDouble(s.substring(0, len - 1)),
        DoubleType)
    } else if (lastChar == 'L') {
      return Literal(java.lang.Long.parseLong(s.substring(0, len - 1)),
        LongType)
    }
    while (index < len) {
      val c = s.charAt(index)
      if (noDecimalPoint && c == '.') {
        noDecimalPoint = false
      } else if (c == 'e' || c == 'E') {
        return toDecimalOrDoubleLiteral(s, scientific = true)
      }
      index += 1
    }
    if (noDecimalPoint) {
      // case of integral value
      // most cases should be handled by Long, so try that first
      try {
        val longValue = java.lang.Long.parseLong(s)
        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          Literal(longValue.toInt, IntegerType)
        } else {
          Literal(longValue, LongType)
        }
      } catch {
        case _: NumberFormatException =>
          val decimal = BigDecimal(s)
          if (decimal.isValidInt) {
            Literal(decimal.toIntExact)
          } else if (decimal.isValidLong) {
            Literal(decimal.toLongExact, LongType)
          } else {
            val sysDefaultType = DecimalType.SYSTEM_DEFAULT
            if (decimal.precision <= sysDefaultType.precision &&
              decimal.scale <= sysDefaultType.scale) {
              Literal(decimal, sysDefaultType)
            } else Literal(decimal)
          }
      }
    } else {
      toDecimalOrDoubleLiteral(s, scientific = false)
    }
  }

  private final def updatePerTableQueryHint(tableIdent: TableIdentifier,
      optAlias: Option[String]) = {
    val indexHint = queryHints.remove(QueryHint.Index.toString)
    if (indexHint.nonEmpty) {
      val table = optAlias match {
        case Some(alias) => alias
        case _ => tableIdent.unquotedString
      }

      queryHints.put(QueryHint.Index.toString + table, indexHint.get)
    }
  }

  private final def assertNoQueryHint(hint: QueryHint.Value, msg: String) = {
    if (queryHints.exists({
      case (key, _) => key.startsWith(hint.toString)
    })) {
      throw Utils.analysisException(msg)
    }
  }


  protected final def booleanLiteral: Rule1[Literal] = rule {
    TRUE ~> (() => Literal.create(true, BooleanType)) |
    FALSE ~> (() => Literal.create(false, BooleanType))
  }

  protected final def numericLiteral: Rule1[Literal] = rule {
    capture(plusOrMinus.? ~ Consts.numeric. + ~ (Consts.exponent ~
        plusOrMinus.? ~ CharPredicate.Digit. +).? ~ Consts.numericSuffix.?) ~
        delimiter ~> ((s: String) => toNumericLiteral(s))
  }

  protected final def literal: Rule1[Literal] = rule {
    stringLiteral ~> ((s: String) => Literal.create(s, StringType)) |
    numericLiteral |
    booleanLiteral |
    NULL ~> (() => Literal.create(null, NullType)) |
    intervalLiteral
  }

  protected final def paramLiteralQuestionMark: Rule1[ParamLiteral] = rule {
    questionMark ~> (() => {
      session.sessionState.questionMarkCounter = session.sessionState.questionMarkCounter + 1
      if (session.sessionState.isPreparePhase) {
        ParamLiteral(Row(session.sessionState.questionMarkCounter), NullType, 0)
      } else {
        assert(session.sessionState.pvs.isDefined,
          "For Prepared Statement, Parameter constants are not provided")
        if (session.sessionState.questionMarkCounter >
            session.sessionState.pvs.get.getParameterCount) {
          assert(assertion = false, s"For Prepared Statement, Got more number of" +
              s" placeholders = ${session.sessionState.questionMarkCounter}" +
              s" than given number of parameter" +
              s" constants = ${session.sessionState.pvs.get.getParameterCount}")
        }
        val dvd =
          session.sessionState.pvs.get.getParameter(session.sessionState.questionMarkCounter - 1)
        val scalaTypeVal = CachedPlanHelperExec.getValue(dvd)
        val catalystTypeVal = CatalystTypeConverters.convertToCatalyst(scalaTypeVal)
        val storeType = dvd.getTypeFormatId
        val storePrecision = dvd match {
          case d: SQLDecimal => d.getDecimalValuePrecision
          case _ => -1
        }
        val storeScale = dvd match {
          case d: SQLDecimal => d.getDecimalValueScale
          case _ => -1
        }
        val dataType = SnappySession.getDataType(storeType, storePrecision, storeScale)
        paramcounter = paramcounter + 1
        val p = ParamLiteral(catalystTypeVal, dataType, paramcounter)
        addParamLiteralToContext(p)
        p
      }
    })
  }

  def addParamLiteralToContext(p: ParamLiteral): Unit =
    session.getContextObject[mutable.ArrayBuffer[ParamLiteral]](
      CachedPlanHelperExec.WRAPPED_CONSTANTS) match {
      case Some(list) => list += p
      case None => session.addContextObject(CachedPlanHelperExec.WRAPPED_CONSTANTS,
        mutable.ArrayBuffer(p))
    }

  def removeParamLiteralFromContext(p: ParamLiteral): Unit =
    session.getContextObject[mutable.ArrayBuffer[ParamLiteral]](
      CachedPlanHelperExec.WRAPPED_CONSTANTS) match {
      case Some(list) => list -= p
      case None =>
    }

  protected final def paramliteral: Rule1[ParamLiteral] = rule {
    literal ~> ((l: Literal) => {
      paramcounter = paramcounter + 1
      val p = ParamLiteral(l.value, l.dataType, paramcounter)
      addParamLiteralToContext(p)
      p
    })
  }

  protected final def month: Rule1[Int] = rule {
    integral ~ MONTH ~> ((num: String) => num.toInt)
  }

  protected final def year: Rule1[Int] = rule {
    integral ~ YEAR ~> ((num: String) => num.toInt)
  }

  protected final def microsecond: Rule1[Long] = rule {
    integral ~ (MICROS | MICROSECOND) ~> ((num: String) => num.toLong)
  }

  protected final def millisecond: Rule1[Long] = rule {
    integral ~ (MILLIS | MILLISECOND) ~> ((num: String) => num.toLong)
  }

  protected final def second: Rule1[Long] = rule {
    integral ~ (SECS | SECOND) ~> ((num: String) => num.toLong)
  }

  protected final def minute: Rule1[Long] = rule {
    integral ~ (MINS | MINUTE) ~> ((num: String) => num.toLong)
  }

  protected final def hour: Rule1[Long] = rule {
    integral ~ HOUR ~> ((num: String) => num.toLong)
  }

  protected final def day: Rule1[Long] = rule {
    integral ~ DAY ~> ((num: String) => num.toLong)
  }

  protected final def week: Rule1[Long] = rule {
    integral ~ WEEK ~> ((num: String) => num.toLong)
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
            second.? ~ millisecond.? ~ microsecond.? ~> { (y: Any, m: Any,
            w: Any, d: Any, h: Any, m2: Any, s: Any, m3: Any, m4: Any) =>
          val year = y.asInstanceOf[Option[Int]]
          val month = m.asInstanceOf[Option[Int]]
          val week = w.asInstanceOf[Option[Int]]
          val day = d.asInstanceOf[Option[Long]]
          val hour = h.asInstanceOf[Option[Long]]
          val minute = m2.asInstanceOf[Option[Long]]
          val second = s.asInstanceOf[Option[Long]]
          val millis = m3.asInstanceOf[Option[Long]]
          val micros = m4.asInstanceOf[Option[Long]]
          if (!Seq(year, month, week, day, hour, minute, second, millis,
            micros).exists(_.isDefined)) {
            throw Utils.analysisException(
              "at least one time unit should be given for interval literal")
          }
          val months = year.map(_ * 12).getOrElse(0) + month.getOrElse(0)
          val microseconds =
            week.map(_ * CalendarInterval.MICROS_PER_WEEK).getOrElse(0L) +
            day.map(_ * CalendarInterval.MICROS_PER_DAY).getOrElse(0L) +
            hour.map(_ * CalendarInterval.MICROS_PER_HOUR).getOrElse(0L) +
            minute.map(_ * CalendarInterval.MICROS_PER_MINUTE).getOrElse(0L) +
            second.map(_ * CalendarInterval.MICROS_PER_SECOND).getOrElse(0L) +
            millis.map(_ * CalendarInterval.MICROS_PER_MILLI).getOrElse(0L) +
            micros.getOrElse(0L)
          Literal(new CalendarInterval(months, microseconds))
        }
    )
  }

  protected final def unsignedFloat: Rule1[String] = rule {
    capture(
      CharPredicate.Digit.* ~ '.' ~ CharPredicate.Digit. + ~
          scientificNotation.? |
      CharPredicate.Digit. + ~ scientificNotation
    ) ~ ws
  }

  final def namedExpression: Rule1[Expression] = rule {
    expression ~ (
        AS ~ identifier ~> ((e: Expression, a: String) => Alias(e, a)()) |
        strictIdentifier ~> ((e: Expression, a: String) => Alias(e, a)()) |
        MATCH.asInstanceOf[Rule[Expression::HNil, Expression::HNil]]
    )
  }

  final def parsedDataType: Rule1[DataType] = rule {
    ws ~ dataType
  }

  final def parsedExpression: Rule1[Expression] = rule {
    ws ~ namedExpression
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
    (NOT ~> falseFn).? ~ comparisonExpression ~> ((not: Any, e: Expression) =>
      if (not.asInstanceOf[Option[Boolean]].isEmpty) e else Not(e))
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
        invertibleExpression |
        IS ~ (NOT ~> trueFn).? ~ NULL ~>
            ((e: Expression, not: Any) =>
              if (not.asInstanceOf[Option[Boolean]].isEmpty) IsNull(e)
              else IsNotNull(e)) |
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
  protected final def invertibleExpression: Rule[Expression :: HNil,
      Expression :: HNil] = rule {
    IN ~ '(' ~ ws ~ (termExpression * commaSep) ~ ')' ~ ws ~>
        ((e: Expression, es: Any) => In(e, es.asInstanceOf[Seq[Expression]])) |
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

  protected final def streamWindowOptions: Rule1[(Duration,
      Option[Duration])] = rule {
    WINDOW ~ '(' ~ ws ~ DURATION ~ durationUnit ~ (commaSep ~
        SLIDE ~ durationUnit).? ~ ')' ~ ws ~> ((d: Duration, s: Any) =>
      (d, s.asInstanceOf[Option[Duration]]))
  }

  protected final def extractGroupingSet(
      child: LogicalPlan,
      aggregations: Seq[NamedExpression],
      groupByExprs: Seq[Expression],
      groupingSets: Seq[Seq[Expression]]): GroupingSets = {
    val keyMap = groupByExprs.zipWithIndex.toMap
    val numExpressions = keyMap.size
    val mask = (1 << numExpressions) - 1
    val bitmasks: Seq[Int] = groupingSets.map(set => set.foldLeft(mask)((bitmap, col) => {
      require(keyMap.contains(col), s"$col doesn't show up in the GROUP BY list")
      bitmap & ~(1 << (numExpressions - 1 - keyMap(col)))
    }))
    GroupingSets(bitmasks, groupByExprs, child, aggregations)
  }

  protected final def groupingSetExpr: Rule1[Seq[Expression]] = rule {
    '(' ~ ws ~ (expression * commaSep) ~ ')' ~ ws ~>
        ((e: Any) => e.asInstanceOf[Seq[Expression]]) |
    (expression + commaSep)
  }

  protected final def cubeRollUpGroupingSet: Rule1[
      (Seq[Seq[Expression]], String)] = rule {
    WITH ~ (
        CUBE ~> (() => (Seq(Seq[Expression]()), "CUBE")) |
        ROLLUP ~> (() => (Seq(Seq[Expression]()), "ROLLUP"))
    ) |
    GROUPING ~ SETS ~ ('(' ~ ws ~ (groupingSetExpr + commaSep) ~ ')' ~ ws)  ~>
        ((gs: Seq[Seq[Expression]]) => (gs, "GROUPINGSETS"))
  }

  protected final def groupBy: Rule1[(Seq[Expression],
      Seq[Seq[Expression]], String)] = rule {
    GROUP ~ BY ~ (expression + commaSep) ~ cubeRollUpGroupingSet.? ~>
        ((groupingExpr: Any, crgs: Any) =>
        {  // if cube, rollup, GrSet is not used
          val emptyCubeRollupGrSet = (Seq(Seq[Expression]()), "")
          val cubeRollupGrSetExprs = crgs.asInstanceOf[Option[(Seq[
              Seq[Expression]], String)]].getOrElse(emptyCubeRollupGrSet)
          (groupingExpr.asInstanceOf[Seq[Expression]], cubeRollupGrSetExprs._1,
              cubeRollupGrSetExprs._2)
        })
  }

  protected final def relationFactor: Rule1[LogicalPlan] = rule {
    tableIdentifier ~ streamWindowOptions.? ~
    (AS ~ identifier | strictIdentifier).? ~>
        ((tableIdent: TableIdentifier,
            window: Any, alias: Any) => window.asInstanceOf[Option[
            (Duration, Option[Duration])]] match {
          case None =>
            val optAlias = alias.asInstanceOf[Option[String]]
            updatePerTableQueryHint(tableIdent, optAlias)
            UnresolvedRelation(tableIdent, optAlias)
          case Some(win) =>
            val optAlias = alias.asInstanceOf[Option[String]]
            updatePerTableQueryHint(tableIdent, optAlias)
            WindowLogicalPlan(win._1, win._2,
              UnresolvedRelation(tableIdent, optAlias))
        }) |
    '(' ~ ws ~ start ~ ')' ~ ws ~ streamWindowOptions.? ~
        (AS ~ identifier | strictIdentifier).? ~> { (child: LogicalPlan, w: Any, alias: Any) =>
      val aliasPlan = alias.asInstanceOf[Option[String]] match {
        case None => child
        case Some(name) => SubqueryAlias(name, child, None)
      }
      w.asInstanceOf[Option[(Duration, Option[Duration])]] match {
        case None =>
          assertNoQueryHint(QueryHint.Index,
            s"${QueryHint.Index} cannot be applied to derived table $alias")
          aliasPlan
        case Some(win) =>
          assertNoQueryHint(QueryHint.Index,
            s"${QueryHint.Index} cannot be applied to derived table $alias")
          WindowLogicalPlan(win._1, win._2, aliasPlan)
      }
    }
  }

  /*
  protected final def inlineTable: Rule1[LogicalPlan] = rule {
    VALUES ~ (expression + commaSep) ~ AS.? ~ identifier.? ~
        ('(' ~ ws ~ (identifier + commaSep) ~ ')' ~ ws).? ~>
        ((valuesExpr: Seq[Expression], alias: Any, identifiers: Any) => {
          val rows = valuesExpr.map {
            case CreateStruct(children) => children // e.g. values (1), (2), (3)
            case child => Seq(child) // e.g. values 1, 2, 3
          }
          val aliases = identifiers match {
            case None => Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
            case Some(ids) => ids.asInstanceOf[Seq[String]]
          }
          alias match {
            case None => UnresolvedInlineTable(aliases, rows)
            case Some(a) => SubqueryAlias(a.asInstanceOf[String],
              UnresolvedInlineTable(aliases, rows))
          }
        })
  }
  */

  protected final def join: Rule1[JoinRuleType] = rule {
    joinType.? ~ JOIN ~ relationWithExternal ~ (
        ON ~ expression ~> ((t: Any, r: LogicalPlan, j: Expression) =>
          (t.asInstanceOf[Option[JoinType]], r, Some(j))) |
        USING ~ '(' ~ ws ~ (identifier + commaSep) ~ ')' ~ ws ~>
            ((t: Any, r: LogicalPlan, ids: Any) =>
              (Some(UsingJoin(t.asInstanceOf[Option[JoinType]]
                  .getOrElse(Inner), ids.asInstanceOf[Seq[String]])), r, None)) |
        MATCH ~> ((t: Option[JoinType], r: LogicalPlan) => (t, r, None))
    ) |
    NATURAL ~ joinType.? ~ JOIN ~ relationWithExternal ~> ((t: Any,
        r: LogicalPlan) => (Some(NaturalJoin(t.asInstanceOf[Option[JoinType]]
        .getOrElse(Inner))), r, None))
  }

  protected final def joinType: Rule1[JoinType] = rule {
    INNER ~> (() => Inner) |
    LEFT ~ (
        SEMI ~> (() => LeftSemi) |
        ANTI ~> (() => LeftAnti) |
        OUTER.? ~> (() => LeftOuter)
    ) |
    RIGHT ~ OUTER.? ~> (() => RightOuter) |
    FULL ~ OUTER.? ~> (() => FullOuter) |
    ANTI ~> (() => LeftAnti)
  }

  protected final def ordering: Rule1[Seq[SortOrder]] = rule {
    ((expression ~ sortDirection.? ~ (NULLS ~ (FIRST ~> trueFn | LAST ~> falseFn)).? ~>
        ((e: Expression, d: Any, n: Any) => (e, d, n))) + commaSep) ~> ((exps: Any) =>
      exps.asInstanceOf[Seq[(Expression, Option[SortDirection], Option[Boolean])]].map {
        case (child, d, n) =>
          val direction = d match {
            case Some(v) => v
            case None => Ascending
          }
          val nulls = n match {
            case Some(false) => NullsLast
            case Some(true) => NullsFirst
            case None => direction.defaultNullOrdering
          }
          SortOrder(child, direction, nulls)
      })
  }

  protected final def queryOrganization: Rule1[LogicalPlan =>
      LogicalPlan] = rule {
    (ORDER ~ BY ~ ordering ~> ((o: Seq[SortOrder]) =>
      (l: LogicalPlan) => Sort(o, global = true, l)) |
    SORT ~ BY ~ ordering ~ distributeBy.? ~> ((o: Seq[SortOrder], d: Any) =>
      (l: LogicalPlan) => Sort(o, global = false, d.asInstanceOf[Option[
          LogicalPlan => LogicalPlan]].map(_ (l)).getOrElse(l))) |
    distributeBy |
    CLUSTER ~ BY ~ (expression + commaSep) ~> ((e: Seq[Expression]) =>
      (l: LogicalPlan) => Sort(e.map(SortOrder(_, Ascending)), global = false,
        RepartitionByExpression(e, l)))).? ~
    (WINDOW ~ ((identifier ~ AS ~ windowSpec ~>
        ((id: String, w: WindowSpec) => id -> w)) + commaSep)).? ~
    (LIMIT ~ TOKENIZE_END ~ expression).? ~> { (o: Any, w: Any, e: Any) => (l: LogicalPlan) =>
      val withOrder = o.asInstanceOf[Option[LogicalPlan => LogicalPlan]]
          .map(_ (l)).getOrElse(l)
      val window = w.asInstanceOf[Option[Seq[(String, WindowSpec)]]].map { ws =>
        val baseWindowMap = ws.toMap
        val windowMapView = baseWindowMap.mapValues {
          case WindowSpecReference(name) =>
            baseWindowMap.get(name) match {
              case Some(spec: WindowSpecDefinition) => spec
              case Some(_) => throw Utils.analysisException(
                s"Window reference '$name' is not a window specification")
              case None => throw Utils.analysisException(
                s"Cannot resolve window reference '$name'")
            }
          case spec: WindowSpecDefinition => spec
        }

        // Note that mapValues creates a view, so force materialization.
        WithWindowDefinition(windowMapView.map(identity), withOrder)
      }.getOrElse(withOrder)
      e.asInstanceOf[Option[Expression]].map(Limit(_, window)).getOrElse(window)
    }
  }

  protected final def distributeBy: Rule1[LogicalPlan => LogicalPlan] = rule {
    DISTRIBUTE ~ BY ~ (expression + commaSep) ~> ((e: Seq[Expression]) =>
      (l: LogicalPlan) => RepartitionByExpression(e, l))
  }

  protected final def windowSpec: Rule1[WindowSpec] = rule {
    '(' ~ ws ~ ((PARTITION | DISTRIBUTE | CLUSTER) ~ BY ~ (expression +
        commaSep)).? ~ (ORDER | SORT) ~ BY ~ ordering ~ windowFrame.? ~ ')' ~
        ws ~> ((p: Any, o: Seq[SortOrder], w: Any) => WindowSpecDefinition(
      p.asInstanceOf[Option[Seq[Expression]]].getOrElse(Seq.empty), o,
      w.asInstanceOf[Option[SpecifiedWindowFrame]]
          .getOrElse(UnspecifiedFrame))) |
    identifier ~> WindowSpecReference
  }

  protected final def windowFrame: Rule1[SpecifiedWindowFrame] = rule {
    (RANGE ~> (() => RangeFrame) | ROWS ~> (() => RowFrame)) ~ (
        BETWEEN ~ frameBound ~ AND ~ frameBound ~> ((t: FrameType,
            s: FrameBoundary, e: FrameBoundary) => SpecifiedWindowFrame(t, s, e)) |
        frameBound ~> ((t: FrameType, s: FrameBoundary) =>
          SpecifiedWindowFrame(t, s, CurrentRow))
    )
  }

  protected final def frameBound: Rule1[FrameBoundary] = rule {
    UNBOUNDED ~ (
        PRECEDING ~> (() => UnboundedPreceding) |
        FOLLOWING ~> (() => UnboundedFollowing)
    ) |
    CURRENT ~ ROW ~> (() => CurrentRow) |
    integral ~ (
        PRECEDING ~> ((num: String) => ValuePreceding(num.toInt)) |
        FOLLOWING ~> ((num: String) => ValueFollowing(num.toInt))
    )
  }

  protected final def tableValuedFunctionExpressions: Rule1[Seq[Expression]] = rule {
    '(' ~ ws ~ (expression + commaSep).? ~ ')' ~>
      ((e: Any) => e.asInstanceOf[Option[Vector[Expression]]] match {
        case Some(ve) => ve
        case _ => Seq.empty
      })
  }

  protected final def relationWithExternal: Rule1[LogicalPlan] = rule {
    (relationFactor ~ (tableValuedFunctionExpressions).?) ~> ((lp: LogicalPlan, se: Any) => {
      se.asInstanceOf[Option[Seq[Expression]]] match {
        case None => lp
        case Some(exprs) => {
          val ur = lp.asInstanceOf[UnresolvedRelation]
          val fname = org.apache.spark.sql.collection.Utils.toLowerCase(
            ur.tableIdentifier.identifier)
          UnresolvedTableValuedFunction(fname, exprs)
        }
      }
    })
  }

  protected final def relation: Rule1[LogicalPlan] = rule {
    relationWithExternal ~ (
        join. + ~> ((r1: LogicalPlan, joins: Any) => joins.asInstanceOf[
            Seq[JoinRuleType]].foldLeft(r1) { case (lhs, (jt, rhs, cond)) =>
          Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond)
        }) |
        MATCH.asInstanceOf[Rule[LogicalPlan :: HNil, LogicalPlan :: HNil]]
    )
  }

  protected final def relations: Rule1[LogicalPlan] = rule {
    (relation + commaSep) ~> ((joins: Seq[LogicalPlan]) =>
      if (joins.size == 1) joins.head
      else joins.tail.foldLeft(joins.head) {
        case (lhs, rel) => Join(lhs, rel, Inner, None)
      })
  }

  protected final def keyWhenThenElse: Rule1[WhenElseType] = rule {
    expression ~ (WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression,
        t: Expression) => (w, t))). + ~ (ELSE ~ expression).? ~ END ~>
        ((key: Expression, altPart: Any, elsePart: Any) =>
          (altPart.asInstanceOf[Seq[(Expression, Expression)]].map(
            e => EqualTo(key, e._1) -> e._2), elsePart).asInstanceOf[WhenElseType])
  }

  protected final def whenThenElse: Rule1[WhenElseType] = rule {
    (WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression,
        t: Expression) => (w, t))). + ~ (ELSE ~ expression).? ~ END ~>
        ((altPart: Any, elsePart: Any) =>
          (altPart, elsePart).asInstanceOf[WhenElseType])
  }

  protected final def primary: Rule1[Expression] = rule {
    identifier ~ (
      ('.' ~ identifier).? ~ '(' ~ ws ~ (
        '*' ~ ws ~ ')' ~ ws ~> ((n1: String, n2: Option[String]) =>
          if (n1.equalsIgnoreCase("COUNT") && n2.isEmpty) {
            AggregateExpression(Count(Literal(1, IntegerType)),
              mode = Complete, isDistinct = false)
          } else {
            val n2str = if (n2.isEmpty) "" else s".${n2.get}"
            throw Utils.analysisException(s"invalid expression $n1$n2str(*)")
          }) |
          (DISTINCT ~> trueFn).? ~ (expression * commaSep) ~ ')' ~ ws ~
            (OVER ~ windowSpec).? ~> { (n1: String, n2: Any, d: Any, e: Any, w: Any) =>
            val f2 = n2.asInstanceOf[Option[String]]
            val udfName = f2.fold(new FunctionIdentifier(n1))(new FunctionIdentifier(_, Some(n1)))
            val allExprs = e.asInstanceOf[Seq[Expression]]
            var exprs = allExprs
            Constant.FOLDABLE_FUNCTIONS.get(n1) match {
              case Some(args) =>
                exprs = allExprs.zipWithIndex.collect {
                  case (pl: ParamLiteral, index) if args.contains(index) ||
                      // all args          // all odd args
                      (args.head == -10) || (args.head == -1 && (index & 0x1) == 1) ||
                      // all even args
                      (args.head == -2 && (index & 0x1) == 0) =>
                    removeParamLiteralFromContext(pl)
                    Literal.create(pl.value, pl.dataType)
                  case (ex: Expression, _) => ex
                }
              case None =>
            }
            val function = if (d.asInstanceOf[Option[Boolean]].isEmpty) {
              UnresolvedFunction(udfName, exprs, isDistinct = false)
            } else if (udfName.funcName.equalsIgnoreCase("COUNT")) {
              aggregate.Count(exprs).toAggregateExpression(isDistinct = true)
            } else {
              UnresolvedFunction(udfName, exprs, isDistinct = true)
            }
            w.asInstanceOf[Option[WindowSpec]] match {
              case None => function
              case Some(spec: WindowSpecDefinition) =>
                WindowExpression(function, spec)
              case Some(ref: WindowSpecReference) =>
                UnresolvedWindowExpression(function, ref)
            }
          }
        ) |
        '.' ~ ws ~ (
            identifier. +('.' ~ ws) ~> ((i1: String, rest: Any) =>
              UnresolvedAttribute(i1 +: rest.asInstanceOf[Seq[String]])) |
            (identifier ~ '.' ~ ws).* ~ '*' ~ ws ~> ((i1: String, rest: Any) =>
              UnresolvedStar(Option(i1 +: rest.asInstanceOf[Seq[String]])))
        ) |
        MATCH ~> UnresolvedAttribute.quoted _
    ) |
    '{' ~ FN ~ ws ~ functionIdentifier ~ '(' ~ (expression * commaSep) ~ ')' ~ ws ~ '}' ~ ws ~> {
      (fn: FunctionIdentifier, e: Any) =>
        val allExprs = e.asInstanceOf[Seq[Expression]].toList
        var exprs = allExprs
        Constant.FOLDABLE_FUNCTIONS.get(fn.funcName) match {
          case Some(args) =>
            exprs = allExprs.zipWithIndex.collect {
              case (pl: ParamLiteral, index) if args.contains(index) ||
                  // all args          // all odd args
                  (args.head == -10) || (args.head == -1 && (index & 0x1) == 1) ||
                  // all even args
                  (args.head == -2 && (index & 0x1) == 0) =>
                removeParamLiteralFromContext(pl)
                Literal.create(pl.value, pl.dataType)
              case (ex: Expression, _) => ex
            }
          case None =>
        }
        fn match {
          case f if f.funcName.equalsIgnoreCase("TIMESTAMPADD") =>
            assert(exprs.length == 3)
            assert(exprs.head.isInstanceOf[UnresolvedAttribute] &&
                exprs.head.asInstanceOf[UnresolvedAttribute].name.equals("SQL_TSI_DAY"))
            DateAdd(exprs(2), exprs(1))
          case f => UnresolvedFunction(f, exprs, isDistinct = false)
        }
    } |
    ( ( test(tokenize) ~ paramliteral ) | literal | paramLiteralQuestionMark) |
    CAST ~ '(' ~ ws ~ expression ~ AS ~ dataType ~ ')' ~ ws ~> (Cast(_, _)) |
    CASE ~ (
        whenThenElse ~> (s => CaseWhen(s._1, s._2)) |
        keyWhenThenElse ~> (s => CaseWhen(s._1, s._2))
    ) |
    EXISTS ~ '(' ~ ws ~ query ~ ')' ~ ws ~> (Exists(_)) |
    CURRENT_DATE ~> CurrentDate |
    CURRENT_TIMESTAMP ~> CurrentTimestamp |
    '(' ~ ws ~ (
        (expression + commaSep) ~ ')' ~ ws ~> ((exprs: Seq[Expression]) =>
          if (exprs.length == 1) exprs.head else CreateStruct(exprs)
        ) |
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
    (namedExpression + commaSep) ~
    (FROM ~ relations).? ~
    (WHERE ~ TOKENIZE_BEGIN ~ expression ~ TOKENIZE_END).? ~
    groupBy.? ~
    (HAVING ~ TOKENIZE_BEGIN ~ expression ~ TOKENIZE_END).? ~
    queryOrganization ~> { (d: Any, p: Any, f: Any, w: Any, g: Any, h: Any,
        q: LogicalPlan => LogicalPlan) =>
      val base = f match {
        case Some(plan) => plan.asInstanceOf[LogicalPlan]
        case _ => OneRowRelation
      }
      val withFilter = w match {
        case Some(expr) => Filter(expr.asInstanceOf[Expression], base)
        case _ => base
      }
      val expressions = p.asInstanceOf[Seq[Expression]].map {
        case ne: NamedExpression => ne
        case e => UnresolvedAlias(e)
      }
      val gr = g.asInstanceOf[Option[(Seq[Expression], Seq[Seq[Expression]], String)]]
      val withProjection = gr match {
        case Some(x) => x._3 match {
          // group by cols with rollup
          case "ROLLUP" => Aggregate(Seq(Rollup(x._1)), expressions, withFilter)
          // group by cols with cube
          case "CUBE" => Aggregate(Seq(Cube(x._1)), expressions, withFilter)
          // group by cols with grouping sets()()
          case "GROUPINGSETS" => extractGroupingSet(withFilter, expressions, x._1, x._2)
          // just "group by cols"
          case _ => Aggregate(x._1, expressions, withFilter)
        }
        case _ => Project(expressions, withFilter)
      }
      val withDistinct = d match {
        case None => withProjection
        case Some(_) => Distinct(withProjection)
      }
      val withHaving = h match {
        case Some(expr) => Filter(expr.asInstanceOf[Expression], withDistinct)
        case _ => withDistinct
      }
      q(withHaving)
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
        Option[String]], s, OverwriteOptions(o), ifNotExists = false))
  }

  protected final def put: Rule1[LogicalPlan] = rule {
    PUT ~ INTO ~ TABLE.? ~ relationFactor ~ query ~> PutIntoTable
  }

  protected final def update: Rule1[LogicalPlan] = rule {
    UPDATE ~ tableIdentifier ~ SET ~ TOKENIZE_BEGIN ~ (((identifier + ('.' ~ ws)) ~
        '=' ~ ws ~ expression ~> ((cols: Seq[String], e: Expression) =>
      UnresolvedAttribute(cols) -> e)) + commaSep) ~
        (WHERE ~ expression).? ~ TOKENIZE_END ~>
        ((tableName: TableIdentifier, updateExprs: Seq[(UnresolvedAttribute,
            Expression)], whereExpr: Any) => {
          val base = UnresolvedRelation(tableName)
          val withFilter = whereExpr match {
            case None => base
            case Some(w) => Filter(w.asInstanceOf[Expression], base)
          }
          val (updateColumns, updateExpressions) = updateExprs.unzip
          Update(base, withFilter, Seq.empty, updateColumns, updateExpressions)
        })
  }

  protected final def delete: Rule1[LogicalPlan] = rule {
    DELETE ~ FROM ~ tableIdentifier ~
        (WHERE ~ TOKENIZE_BEGIN ~ expression ~ TOKENIZE_END).? ~>
        ((tableName: TableIdentifier, whereExpr: Any) => {
          val base = UnresolvedRelation(tableName)
          val child = whereExpr match {
            case None => base
            case Some(w) => Filter(w.asInstanceOf[Expression], base)
          }
          Delete(base, child, Seq.empty)
        })
  }

  protected final def ctes: Rule1[LogicalPlan] = rule {
    WITH ~ ((identifier ~ AS.? ~ '(' ~ ws ~ query ~ ')' ~ ws ~>
        ((id: String, p: LogicalPlan) => (id, p))) + commaSep) ~
        (query | insert) ~> ((r: Seq[(String, LogicalPlan)], s: LogicalPlan) =>
        With(s, r.map(ns => (ns._1, SubqueryAlias(ns._1, ns._2, None)))))
  }

  protected def dmlOperation: Rule1[LogicalPlan] = rule {
    (INSERT ~ INTO | PUT ~ INTO) ~ tableIdentifier ~
        ANY.* ~> ((r: TableIdentifier) => DMLExternalTable(r,
        UnresolvedRelation(r), input.sliceString(0, input.length)))
  }

  // Only when wholeStageEnabled try for tokenization. It should be true
  private val tokenizationDisabled = java.lang.Boolean.getBoolean("DISABLE_TOKENIZATION")

  private var tokenize = !tokenizationDisabled && session.sessionState.conf.wholeStageEnabled

  private var isSelect = false

  protected final def TOKENIZE_BEGIN: Rule0 = rule {
    MATCH ~> (() =>
      tokenize = !tokenizationDisabled && isSelect && session.sessionState.conf.wholeStageEnabled)
  }

  protected final def TOKENIZE_END: Rule0 = rule {
    MATCH ~> {() => tokenize = false}
  }

  protected final def SET_SELECT: Rule0 = rule {
    MATCH ~> (() => isSelect = true)
  }

  protected final def SET_NOSELECT: Rule0 = rule {
    MATCH ~> (() => isSelect = false)
  }

  override protected def start: Rule1[LogicalPlan] = rule {
    (SET_SELECT ~ (query.named("select") | insert | put | update | delete)) |
        (SET_NOSELECT ~ (dmlOperation | ctes | ddl | set | cache | uncache | desc))
  }

  final def parse[T](sqlText: String, parseRule: => Try[T]): T = session.synchronized {
    session.clearQueryData()
    session.sessionState.clearExecutionData()
    caseSensitive = session.sessionState.conf.caseSensitiveAnalysis
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
