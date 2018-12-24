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
package org.apache.spark.sql.policy


import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import com.pivotal.gemfirexd.internal.impl.jdbc.authentication.LDAPAuthenticationSchemeImpl
import io.snappydata.Constant

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, StringType}
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

/**
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
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


case class LdapGroupsOfCurrentUser(includeParentGroups: Expression) extends LeafExpression
    with CodegenFallback {

  def this() = this(Literal.create(false, BooleanType))

  assert(includeParentGroups.dataType == BooleanType)
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = LdapGroupsOfCurrentUser.dataType

  override def eval(input: InternalRow): Any = {
    val snappySession = SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("SnappySession unavailable")).asInstanceOf[SnappySession]
    var owner = snappySession.conf.get(Attribute.USERNAME_ATTR, "")

    owner = IdUtil.getUserAuthorizationId(
      if (owner.isEmpty) Constant.DEFAULT_SCHEMA
      else snappySession.sessionState.catalog.formatDatabaseName(owner))


   val includeParents = includeParentGroups.eval().asInstanceOf[Boolean]
   val array = ExternalStoreUtils.getLdapGroupsForUser(owner, includeParents).
       map(UTF8String.fromString(_))
    ArrayData.toArrayData(array)
  }

  override def prettyName: String = "current_user_ldap_groups"
}

object LdapGroupsOfCurrentUser {
  val dataType: DataType = ArrayType(StringType)
}