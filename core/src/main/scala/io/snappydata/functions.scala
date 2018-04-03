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
import org.apache.spark.sql.catalyst.expressions.{ExpressionDescription, LeafExpression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This will contain all the functions specific to snappydata
 */
object SnappyDataFunctions {

  def registerSnappyFunctions(functionRegistry: FunctionRegistry): Unit = {
    functionRegistry.registerFunction("DSID", _ => DSID())
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
    ev.code = ""
    ev.isNull = "false"
    ev
  }
}
