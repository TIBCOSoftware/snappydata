/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpressionDescription, ExpressionInfo, LeafExpression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This will contain all the functions specific to snappydata
 */
object SnappyDataFunctions {

  def registerSnappyFunctions(functionRegistry: FunctionRegistry): Unit = {
    var usageStr = ""
    var extendedStr = ""
    var info: ExpressionInfo = null

    // below are in-built operators additionally handled in snappydata over spark
    // which are listed so they can appear in describe function

    // --- BEGIN OPERATORS ---

    usageStr = "expr1 _FUNC_ expr2 - Bitwise left shift `expr1` by `expr2`."
    extendedStr = """
      Examples:
        > SELECT 15 _FUNC_ 2;
        60
      """
    info = new ExpressionInfo("", null, "<<", usageStr, extendedStr)

    usageStr = "expr1 _FUNC_ expr2 - Bitwise arithmetic right shift `expr1` by `expr2`."
    extendedStr = """
      Examples:
        > SELECT 15 _FUNC_ 2;
        3
        > SELECT -15 _FUNC_ 2;
        -4
      """
    info = new ExpressionInfo("", null, ">>", usageStr, extendedStr)

    usageStr = "expr1 _FUNC_ expr2 - Bitwise logical right shift `expr1` by `expr2`."
    extendedStr = """
      Examples:
        > SELECT 15 _FUNC_ 2;
        3
        > SELECT -15 _FUNC_ 2;
        1073741820
      """
    info = new ExpressionInfo("", null, ">>>", usageStr, extendedStr)

    usageStr = "str1 || str2 - Returns the concatenation of str1 and str2."
    extendedStr = """
      Examples:
        > SELECT 'Spark' _FUNC_ 'SQL';
        SparkSQL
      """
    info = new ExpressionInfo("", null, "||", usageStr, extendedStr)

    // --- END OPERATORS ---

    usageStr = "_FUNC_() - Returns the unique distributed member " +
        "ID of the server containing the current row being fetched."
    extendedStr = """
      Examples:
        > SELECT _FUNC_, ID FROM RANGE(1, 10);
        127.0.0.1(25167)<v2>:16171|1
        127.0.0.1(25167)<v2>:16171|2
        127.0.0.1(25167)<v2>:16171|3
        127.0.0.1(25167)<v2>:16171|4
        127.0.0.1(25078)<v1>:13152|5
        127.0.0.1(25078)<v1>:13152|6
        127.0.0.1(25078)<v1>:13152|7
        127.0.0.1(25078)<v1>:13152|8
        127.0.0.1(25167)<v2>:16171|9
      """
    info = new ExpressionInfo(DSID.getClass.getCanonicalName, null, "DSID", usageStr, extendedStr)
    functionRegistry.registerFunction("DSID", info, _ => DSID())
  }
}

/**
 * Expression that returns the dsid of the server containing the row.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the dsid of the server containing the row.")
case class DSID() extends LeafExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override val prettyName = "DSID"

  override def eval(input: InternalRow): UTF8String = {
    UTF8String.fromString(Misc.getMyId.getId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.addMutableState("UTF8String", ev.value, s"${ev.value} = UTF8String" +
        ".fromString(com.pivotal.gemfirexd.internal.engine.Misc.getMyId().getId());")
    ev.copy(code = "", isNull = "false")
  }
}
