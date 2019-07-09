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
package io.snappydata.gemxd

import java.io.DataOutput

import scala.collection.mutable

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, CaseWhen, Cast, Exists, Expression, Like, ListQuery, ParamLiteral, PredicateSubquery, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.PutIntoValuesColumnTable
import org.apache.spark.sql.types._
import org.apache.spark.util.SnappyUtils


class SparkSQLPrepareImpl(val sql: String,
    val schema: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute {

  if (Thread.currentThread().getContextClassLoader != null) {
    val loader = SnappyUtils.getSnappyStoreContextLoader(
      SparkSQLExecuteImpl.getContextOrCurrentClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
  }

  private[this] val session = SnappySessionPerConnection
      .getSnappySessionForConnection(ctx.getConnId)

  if (ctx.getUserName != null && !ctx.getUserName.isEmpty) {
    session.conf.set(Attribute.USERNAME_ATTR, ctx.getUserName)
    session.conf.set(Attribute.PASSWORD_ATTR, ctx.getAuthToken)
  }

  session.setCurrentSchema(schema)

  session.setPreparedQuery(preparePhase = true, None)

  private[this] val analyzedPlan: LogicalPlan = {
    var aplan = session.prepareSQL(sql)
    val questionMarkCounter = session.snappyParser.questionMarkCounter
    val paramLiterals = new mutable.HashSet[ParamLiteral]()
    SparkSQLPrepareImpl.allParamLiterals(aplan, paramLiterals)
    if (paramLiterals.size != questionMarkCounter) {
      aplan = session.prepareSQL(sql, true)
    }
    aplan
  }

  private[this] val thresholdListener = Misc.getMemStore.thresholdListener()

  protected[this] val hdos = new GfxdHeapDataOutputStream(
    thresholdListener, sql, false, senderVersion)

  private lazy val (tableNames, nullability) = SparkSQLExecuteImpl.
      getTableNamesAndNullability(session, analyzedPlan.output)

  private lazy val (columnNames, columnDataTypes) = SparkSQLPrepareImpl.
      getTableNamesAndDatatype(analyzedPlan.output)

  // check for query hint to serialize complex types as JSON strings
  private[this] val complexTypeAsJson = SparkSQLExecuteImpl.getJsonProperties(session)

  private def getColumnTypes: Array[(Int, Int, Int)] =
    columnDataTypes.map(d => SparkSQLExecuteImpl.getSQLType(d, complexTypeAsJson))

  override def packRows(msg: LeadNodeExecutorMsg,
      srh: SnappyResultHolder): Unit = {
    hdos.clearForReuse()
    SparkSQLExecuteImpl.writeMetaData(srh, hdos, tableNames, nullability, columnNames,
      getColumnTypes, columnDataTypes, session.getWarnings)

    val questionMarkCounter = session.snappyParser.questionMarkCounter
    if (questionMarkCounter > 0) {
      val paramLiterals = new mutable.HashSet[ParamLiteral]()
      analyzedPlan match {
        case PutIntoValuesColumnTable(_, _, _, _) => analyzedPlan.expressions.foreach {
          exp => exp.map {
            case QuestionMark(pos) =>
              SparkSQLPrepareImpl.addParamLiteral(pos, exp.dataType, exp.nullable, paramLiterals)
          }
        }
        case _ =>
      }
      SparkSQLPrepareImpl.allParamLiterals(analyzedPlan, paramLiterals)
      if (paramLiterals.size != questionMarkCounter) {
        SparkSQLPrepareImpl.remainingParamLiterals(analyzedPlan, paramLiterals)
      }
      val paramLiteralsAtPrepare = paramLiterals.toArray.sortBy(_.pos)
      val paramCount = paramLiteralsAtPrepare.length
      if (paramCount != questionMarkCounter) {
        throw Util.generateCsSQLException(SQLState.NOT_FOR_PREPARED_STATEMENT, sql)
      }
      val types = new Array[Int](paramCount * 4 + 1)
      types(0) = paramCount
      (0 until paramCount) foreach (i => {
        assert(paramLiteralsAtPrepare(i).pos == i + 1)
        val index = i * 4 + 1
        val dType = paramLiteralsAtPrepare(i).dataType
        val sqlType = getSQLType(dType)
        types(index) = sqlType._1
        types(index + 1) = sqlType._2
        types(index + 2) = sqlType._3
        types(index + 3) = if (paramLiteralsAtPrepare(i).value.asInstanceOf[Boolean]) 1 else 0
      })
      DataSerializer.writeIntArray(types, hdos)
    } else {
      DataSerializer.writeIntArray(Array[Int](0), hdos)
    }

    if (msg.isLocallyExecuted) {
      SparkSQLExecuteImpl.handleLocalExecution(srh, hdos)
    }
    msg.lastResult(srh)
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit =
    SparkSQLExecuteImpl.serializeRows(out, hasMetadata, hdos)

  // Also see SnappyResultHolder.getNewNullDVD(
  def getSQLType(dataType: DataType): (Int, Int, Int) = dataType match {
    case IntegerType => (StoredFormatIds.SQL_INTEGER_ID, -1, -1)
    case StringType => (StoredFormatIds.SQL_CLOB_ID, -1, -1)
    case LongType => (StoredFormatIds.SQL_LONGINT_ID, -1, -1)
    case TimestampType => (StoredFormatIds.SQL_TIMESTAMP_ID, -1, -1)
    case DateType => (StoredFormatIds.SQL_DATE_ID, -1, -1)
    case DoubleType => (StoredFormatIds.SQL_DOUBLE_ID, -1, -1)
    case t: DecimalType => (StoredFormatIds.SQL_DECIMAL_ID,
        t.precision, t.scale)
    case FloatType => (StoredFormatIds.SQL_REAL_ID, -1, -1)
    case BooleanType => (StoredFormatIds.SQL_BOOLEAN_ID, -1, -1)
    case ShortType => (StoredFormatIds.SQL_SMALLINT_ID, -1, -1)
    case ByteType => (StoredFormatIds.SQL_TINYINT_ID, -1, -1)
    case BinaryType => (StoredFormatIds.SQL_BLOB_ID, -1, -1)
    case _: ArrayType | _: MapType | _: StructType =>
      // indicates complex types serialized as json strings
      (StoredFormatIds.REF_TYPE_ID, -1, -1)

    // send across rest as objects that will be displayed as json strings
    case _ => (StoredFormatIds.REF_TYPE_ID, -1, -1)
  }
}

object SparkSQLPrepareImpl{
  def getTableNamesAndDatatype(
      output: Seq[expressions.Attribute]): (Array[String], Array[DataType]) =
    output.toArray.map(o => o.name -> o.dataType).unzip

  def addParamLiteral(position: Int, datatype: DataType, nullable: Boolean,
    result: mutable.HashSet[ParamLiteral]): Unit = if (!result.exists(_.pos == position)) {
    result += ParamLiteral(nullable, datatype, position, execId = -1, tokenized = true)
  }

  def handleCase(branches: Seq[(Expression, Expression)], elseValue: Option[Expression],
    datatype: DataType, nullable: Boolean, result: mutable.HashSet[ParamLiteral]): Unit = {
    branches.foreach {
      case (_, QuestionMark(pos)) =>
        addParamLiteral(pos, datatype, nullable, result)
    }
    elseValue match {
      case Some(QuestionMark(pos)) =>
        addParamLiteral(pos, datatype, nullable, result)
      case _ =>
    }
  }

  def allParamLiterals(plan: LogicalPlan, result: mutable.HashSet[ParamLiteral]): Unit = {
    val mapExpression: PartialFunction[Expression, Expression] = {
      case bl@BinaryComparison(left: Expression, QuestionMark(pos)) =>
        addParamLiteral(pos, left.dataType, left.nullable, result)
        bl
      case blc@BinaryComparison(left: Expression,
      Cast(QuestionMark(pos), _)) =>
        addParamLiteral(pos, left.dataType, left.nullable, result)
        blc
      case ble@BinaryComparison(left: Expression, CaseWhen(branches, elseValue)) =>
        handleCase(branches, elseValue, left.dataType, left.nullable, result)
        ble
      case blce@BinaryComparison(left: Expression, Cast(CaseWhen(branches, elseValue), _)) =>
        handleCase(branches, elseValue, left.dataType, left.nullable, result)
        blce
      case br@BinaryComparison(QuestionMark(pos), right: Expression) =>
        addParamLiteral(pos, right.dataType, right.nullable, result)
        br
      case brc@BinaryComparison(Cast(QuestionMark(pos), _),
      right: Expression) =>
        addParamLiteral(pos, right.dataType, right.nullable, result)
        brc
      case bre@BinaryComparison(CaseWhen(branches, elseValue), right: Expression) =>
        handleCase(branches, elseValue, right.dataType, right.nullable, result)
        bre
      case brce@BinaryComparison(Cast(CaseWhen(branches, elseValue), _), right: Expression) =>
        handleCase(branches, elseValue, right.dataType, right.nullable, result)
        brce
      case l@Like(left: Expression, QuestionMark(pos)) =>
        addParamLiteral(pos, left.dataType, left.nullable, result)
        l
      case lc@Like(left: Expression, Cast(QuestionMark(pos), _)) =>
        addParamLiteral(pos, left.dataType, left.nullable, result)
        lc
      case inlist@org.apache.spark.sql.catalyst.expressions.In(value: Expression,
      list: Seq[Expression]) =>
        list.map {
          case QuestionMark(pos) =>
            addParamLiteral(pos, value.dataType, value.nullable, result)
          case Cast(QuestionMark(pos), _) =>
            addParamLiteral(pos, value.dataType, value.nullable, result)
          case x => x
        }
        inlist
    }
    handleSubQuery(plan, mapExpression)
  }

  def remainingParamLiterals(plan: LogicalPlan, result: mutable.HashSet[ParamLiteral]): Unit = {
    val mapExpression: PartialFunction[Expression, Expression] = {
      case c@Cast(QuestionMark(pos), castType: DataType) =>
        addParamLiteral(pos, castType, nullable = false, result)
        c
      case cc@Cast(CaseWhen(branches, elseValue), castType: DataType) =>
        handleCase(branches, elseValue, castType, nullable = false, result)
        cc
    }
    handleSubQuery(plan, mapExpression)
  }

  def handleSubQuery(plan: LogicalPlan,
    f: PartialFunction[Expression, Expression]): LogicalPlan = plan transformAllExpressions {
    case e if f.isDefinedAt(e) => f(e)
    case sub: SubqueryExpression => sub match {
      case l@ListQuery(query, x) => l.copy(handleSubQuery(query, f), x)
      case e@Exists(query, x) => e.copy(handleSubQuery(query, f), x)
      case p@PredicateSubquery(query, x, y, z) => p.copy(handleSubQuery(query, f), x, y, z)
      case s@ScalarSubquery(query, x, y) => s.copy(handleSubQuery(query, f), x, y)
    }
  }
}

object QuestionMark {
  def unapply(p: ParamLiteral): Option[Int] = {
    if (p.pos == 0 && p.dataType == NullType) {
      p.value match {
        case r: Row => Some(r.getInt(0))
        case _ => None
      }
    } else None
  }
}
