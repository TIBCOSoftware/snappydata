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

package io.snappydata

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.jdbc.{ConnectionConf, ConnectionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CurrentDatabase, Expression, ExpressionDescription, ExpressionInfo, LeafExpression, Nondeterministic}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.policy.{CurrentUser, LdapGroupsOfCurrentUser}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.spark.sql.{SnappyContext, ThinClientConnectorMode}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This will contain all the functions specific to snappydata
 */
object SnappyDataFunctions {

  val stringArrayType: DataType = ArrayType(StringType)

  /**
   * List all the additional builtin functions here.
   */
  val builtin: Seq[(String, ExpressionInfo, FunctionBuilder)] = Seq(
    buildZeroArgExpression("dsid", classOf[DSID], DSID),
    // add current_schema() as an alias for current_database()
    buildZeroArgExpression("current_schema", classOf[CurrentDatabase], CurrentDatabase),
    buildZeroArgExpression("current_user", classOf[CurrentUser], CurrentUser),
    buildZeroArgExpression("current_user_ldap_groups", classOf[LdapGroupsOfCurrentUser],
      LdapGroupsOfCurrentUser)
  )

  def expressionInfo(name: String, fnClass: Class[_]): ExpressionInfo = {
    val df = fnClass.getAnnotation(classOf[ExpressionDescription])
    if (df ne null) {
      new ExpressionInfo(fnClass.getCanonicalName, null, name, df.usage(), df.extended())
    } else {
      new ExpressionInfo(fnClass.getCanonicalName, name)
    }
  }

  def buildZeroArgExpression(name: String, fnClass: Class[_],
      fn: () => Expression): (String, ExpressionInfo, FunctionBuilder) = {
    (name, expressionInfo(name, fnClass), e => {
      if (e.nonEmpty) {
        throw Utils.analysisException(s"Argument(s) passed for zero argument function $name")
      }
      fn()
    })
  }

  def buildOneArgExpression(name: String, fnClass: Class[_],
      fn: Expression => Expression): (String, ExpressionInfo, FunctionBuilder) = {
    (name, expressionInfo(name, fnClass), e => {
      if (e.length == 1) {
        fn(e.head)
      } else {
        throw Utils.analysisException("Invalid number of arguments for function " +
            s"$name: got ${e.length} but expected 1")
      }
    })
  }

  lazy val defaultConnectionProps: ConnectionProperties = SnappyContext.getClusterMode(
    SnappyContext.globalSparkContext) match {
    case _: ThinClientConnectorMode =>
      val session = Utils.getActiveSession
      ExternalStoreUtils.validateAndGetAllProps(session, ExternalStoreUtils.emptyCIMutableMap)
    case _ => null
  }

  def getDSID(connProps: ConnectionProperties): String = connProps match {
    case null => Misc.getMyId.getId
    case _ =>
      val conn = ConnectionUtil.getPooledConnection(SnappyExternalCatalog.SYS_SCHEMA,
        new ConnectionConf(connProps))
      try {
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery("values dsid()")
        assert(rs.next())
        val dsId = rs.getString(1)
        rs.close()
        stmt.close()
        dsId
      } finally {
        conn.close()
      }
  }
}

/**
 * Expression that returns the dsid of the server containing the row.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the unique distributed member ID of executor fetching current row.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       localhost(1831)<v2>:18165
  """)
case class DSID() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def prettyName: String = "DSID"

  private val connectionProps = SnappyDataFunctions.defaultConnectionProps

  @transient private[this] var result: UTF8String = _

  override protected def initializeInternal(partitionIndex: Int): Unit =
    result = UTF8String.fromString(SnappyDataFunctions.getDSID(connectionProps))

  override protected def evalInternal(input: InternalRow): UTF8String = result

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val connPropsRef = ctx.addReferenceObj("connProps", connectionProps,
      classOf[ConnectionProperties].getName)
    ctx.addMutableState("UTF8String", ev.value, s"${ev.value} = UTF8String" +
        s".fromString(io.snappydata.SnappyDataFunctions.getDSID($connPropsRef));")
    ev.copy(code = "", isNull = "false")
  }
}
