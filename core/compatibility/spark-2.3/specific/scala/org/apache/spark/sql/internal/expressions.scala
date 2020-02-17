/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression}
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.types.{DataType, Metadata}

case class ErrorEstimateAttribute23(name: String, dataType: DataType, nullable: Boolean,
    override val metadata: Metadata, realExprId: ExprId)(override val exprId: ExprId,
    override val qualifier: Option[String]) extends ErrorEstimateAttribute {

  override def withQualifier(newQualifier: Option[String]): Attribute = {
    if (newQualifier == qualifier) {
      this
    } else {
      ErrorEstimateAttribute23(name, dataType, nullable, metadata, realExprId)(
        exprId, newQualifier)
    }
  }
}

case class ApproxColumnExtractor23(child: Expression, name: String,
    override val ordinal: Int, dataType: DataType, override val nullable: Boolean)(
    override val exprId: ExprId, override val qualifier: Option[String])
    extends ApproxColumnExtractor

case class TaggedAttribute23(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
    override val metadata: Metadata)(override val exprId: ExprId,
    override val qualifier: Option[String]) extends TaggedAttribute {

  /**
   * Returns a copy of this [[TaggedAttribute]] with new qualifier.
   */
  override def withQualifier(newQualifier: Option[String]): TaggedAttribute = {
    if (newQualifier == qualifier) {
      this
    } else {
      TaggedAttribute23(tag, name, dataType, nullable, metadata)(exprId, newQualifier)
    }
  }
}

case class TaggedAlias23(tag: TransformableTag, child: Expression, name: String)(
    override val exprId: ExprId, override val qualifier: Option[String]) extends TaggedAlias

case class ClosedFormColumnExtractor23(child: Expression, name: String, confidence: Double,
    confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
    behavior: HAC.Type, override val nullable: Boolean)(override val exprId: ExprId,
    override val qualifier: Option[String]) extends ClosedFormColumnExtractor
