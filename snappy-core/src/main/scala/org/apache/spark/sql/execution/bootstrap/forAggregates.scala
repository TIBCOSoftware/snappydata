package org.apache.spark.sql.execution.bootstrap

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnaryNode

case class IntegrityInfo(
    check: Expression,
    updateIndexes: Array[Int],
    updateExpressions: Seq[Expression],
    schema: Seq[Attribute]) {

  private[this] def pretty(elems: Seq[Any]) = elems.mkString("[", ",", "]")

  override def toString =
    s"Integrity($check, ${pretty(updateIndexes.zip(updateExpressions))}, ${pretty(schema)})"
}

case class LineageRelay(filterExpression: Expression, broadcastExpressions: Seq[Expression]) {
  override def toString =
    s"LineageRelay($filterExpression, ${broadcastExpressions.mkString("[", ",", "]")})"
}

trait Relay {
  self: UnaryNode =>

  val lineageRelayInfo: LineageRelay
  def output: Seq[Attribute]

  def buildRelayFilter(): InternalRow => Boolean =
    newPredicate(lineageRelayInfo.filterExpression, output)

  def buildRelayProjection(): Projection =
    if (lineageRelayInfo.broadcastExpressions.isEmpty) EmptyProjection
    else new InterpretedProjection(lineageRelayInfo.broadcastExpressions, output)
}

object EmptyProjection extends Projection {
  override def apply(v1: InternalRow): InternalRow = EmptyRow
}

