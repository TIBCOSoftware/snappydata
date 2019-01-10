/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.benchmark.snappy

import scala.util.matching.Regex

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait SnappyTPCH extends TPCH {
  override def q3: String =
    s"""
       |${super.q3}
       |limit 10
     """.stripMargin
}

trait SnappyAdapter extends Adapter with DynamicQueryGetter {

  type RETURN = (scala.Array[org.apache.spark.sql.Row], DataFrame)

  protected val functionOverrides = Map(
    " 1 0 " -> "DATE_SUB('1997-12-31',[VAL])",
    " 7 0 " -> "between '1995-01-01' and '1996-12-31'",
    " 8 0 " -> "between '1995-01-01' and '1996-12-31'",
    "22 0 " -> "SUBSTR(C_PHONE,1,2)",
    "" -> ""
  )

  override def replace(qNum: String, tokens: QUERY_TYPE, queryStr: String, args: String*):
  String = {
    tokens.zipWithIndex.foldLeft(queryStr) { case (src, (tok, i)) =>
      replaceEach(qNum, i, src, tok, args: _*)
    }
  }

  private def replaceEach(qNum: String,
      tokNum: Int, query: String,
      tok: String, args: String*): String = {

    val qNumPattern(queryNumber, _) = qNum
    val seek = (if (queryNumber.toInt < 10) " " else "") +
        s"$qNum $tokNum" +
        (if (tokNum < 10) " " else "")

    def rep(newArg: String): String = query.replace(tok, newArg)

    def modifyWith(arg: String) = {
      functionModifier(tok).fold(rep(arg)) { r =>
        rep(functionOverrides.getOrElse(seek, r)).replace("[VAL]", arg)
      }
    }

    args.length match {
      case 0 => modifyWith(defaults(seek))
      case _ if tokNum < args.length => args(tokNum) match {
        case s if s.isEmpty => modifyWith(defaults(seek))
        case s => modifyWith(s)
      }
    }
  }

  private def functionModifier(tok: String): Option[String] = {
    def find(elems: Array[String], str: String) = elems.indexWhere(_.indexOf(str) > 0)

    tok match {
      case t if t.startsWith("date") && t.indexOf("month") > 0 =>
        val elems = t.split("'")
        // println(s"${elems.mkString("--")} ${elems(find(elems, "interval") + 1)} ")
        val interval = Integer.parseInt(elems(find(elems, "interval") + 1))
        Some(s"ADD_MONTHS('[VAL]', $interval)")
      case t if t.startsWith("date") && t.indexOf("year") > 0 =>
        val elems = t.split("'")
        // println(s"${elems.mkString("--")} ${elems(find(elems, "interval") + 1)} ")
        val interval = 12 * Integer.parseInt(elems(find(elems, "interval") + 1))
        Some(s"ADD_MONTHS('[VAL]', $interval)")
      case t if t.startsWith("date") =>
        Some("'[VAL]'")
      case t if t.startsWith("substring") =>
        Some("SUBSTR")
      case t if t.startsWith("between") =>
        Some("BETWEEN")
      case t if t.startsWith("extract(year from ") =>
        Some("YEAR(")
      case _ => None
    }
  }

  def estimateSizes(qNum: Int, tableSizes: Map[String, Long],
      executor: (String) => DataFrame): Long = {
    getQueryStrings(qNum, executor).foldLeft(0L) { case (sz, queryString) =>
      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan. Note that this
      // currently doesn't take WITH subqueries into account which might lead to fairly inaccurate
      // per-row processing time for those cases.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      executor(queryString).queryExecution.logical.map {
        case ur@UnresolvedRelation(t: TableIdentifier, _) =>
          queryRelations.add(t.table.toLowerCase)
        case lp: LogicalPlan =>
          lp.expressions.foreach {
            _ foreach {
              case subquery: SubqueryExpression =>
                subquery.plan.foreach {
                  case ur@UnresolvedRelation(t: TableIdentifier, _) =>
                    queryRelations.add(t.table.toLowerCase)
                  case _ =>
                }
              case _ =>
            }
          }
        case _ =>
      }
      sz + queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
    }
  }

  def execute(qNum: Int, executor: (String) => DataFrame): RETURN = {
    val df = executor(getQueryStrings(qNum, executor).last)
    if (df == null) {
      return (null, null)
    }
    (df.collect(), df)
  }

  private def getQueryStrings(qNum: Int, executor: (String) => DataFrame): Seq[String] = {
    qNum match {
      case 15 =>
        val viewName(_, vn, _, viewQuery) = getFinalQueryString(s"${qNum}v")
        val result = executor(viewQuery)
        if (result == null) return "" :: Nil
        result.createOrReplaceTempView(vn)
        viewQuery :: getFinalQueryString(qNum.toString) :: Nil
      case _ =>
        getFinalQueryString(qNum.toString) :: Nil
    }
  }
}
