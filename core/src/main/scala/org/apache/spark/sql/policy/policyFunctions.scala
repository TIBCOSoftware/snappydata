/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.policy


import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.{Constant, SnappyDataFunctions}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpressionDescription, LeafExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Get the current user that owns the session executing the function.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the name of the user that owns the session executing the " +
      "current SQL statement.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       USER1
  """)
case class CurrentUser() extends LeafExpression with CodegenFallback {

  override def foldable: Boolean = true

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    val snappySession = SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("SnappySession unavailable")).asInstanceOf[SnappySession]
    val owner = snappySession.conf.get(Attribute.USERNAME_ATTR, Constant.DEFAULT_SCHEMA)
    // normalize the name for string comparison
    UTF8String.fromString(IdUtil.getUserAuthorizationId(owner))
  }

  override def prettyName: String = "current_user"
}

/**
 * Get the LDAP groups of the current user executing the function.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns all the ldap groups as an ARRAY to which the user " +
      "who is executing the current SQL statement belongs.",
  extended = """
    Examples:
      > SELECT array_contains(_FUNC_(), 'GROUP1');
       true
  """)
case class LdapGroupsOfCurrentUser() extends LeafExpression with CodegenFallback {

  override def foldable: Boolean = true

  override def nullable: Boolean = false

  override def dataType: DataType = SnappyDataFunctions.stringArrayType

  override def eval(input: InternalRow): Any = {
    val snappySession = SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("SnappySession unavailable")).asInstanceOf[SnappySession]
    var owner = snappySession.conf.get(Attribute.USERNAME_ATTR, "")

    owner = IdUtil.getUserAuthorizationId(
      if (owner.isEmpty) Constant.DEFAULT_SCHEMA
      else snappySession.sessionCatalog.formatName(owner))

    val array = ExternalStoreUtils.getLdapGroupsForUser(owner).map(UTF8String.fromString)
    ArrayData.toArrayData(array)
  }

  override def prettyName: String = "current_user_ldap_groups"
}
