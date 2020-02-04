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
package org.apache.spark.sql.execution.common

import io.snappydata.{Constant, Property}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, ParamLiteral}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

object HAC extends Enumeration {

  type Type = Value

  val DO_NOTHING: Type = Value(0)
  val SPECIAL_SYMBOL: Type = Value(1)
  val THROW_EXCEPTION: Type = Value(2)
  val REROUTE_TO_BASE: Type = Value(3)
  val PARTIAL_ROUTING: Type = Value(4)

  override def toString(): String = {
    s" 1)DO_NOTHING 2)LOCAL_OMIT 3)STRICT 4)RUN_ON_FULL_TABLE 5)PARTIAL_RUN_ON_BASE_TABLE"
  }

  def getBehavior(expr: Expression): HAC.Type = {
    expr match {
      case lp: ParamLiteral => getBehavior(lp.valueString)
      case _ => getBehavior(expr.simpleString)
    }
  }


  def getBehavior(name: String): HAC.Type = {
    Utils.toUpperCase(name) match {
      case Constant.BEHAVIOR_DO_NOTHING => DO_NOTHING
      case Constant.BEHAVIOR_LOCAL_OMIT => SPECIAL_SYMBOL
      case Constant.BEHAVIOR_STRICT => THROW_EXCEPTION
      case Constant.BEHAVIOR_RUN_ON_FULL_TABLE => REROUTE_TO_BASE
      case Constant.DEFAULT_BEHAVIOR => getDefaultBehavior()
      case Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE => PARTIAL_ROUTING

      case x@_ => throw new UnsupportedOperationException(
        s"Please specify valid HAC from below:\n$HAC\nGiven: $x")
    }
  }

  def getBehaviorAsString(value: HAC.Type): String = {
    value match {
      case DO_NOTHING => Constant.BEHAVIOR_DO_NOTHING
      case SPECIAL_SYMBOL => Constant.BEHAVIOR_LOCAL_OMIT
      case THROW_EXCEPTION => Constant.BEHAVIOR_STRICT
      case REROUTE_TO_BASE => Constant.BEHAVIOR_RUN_ON_FULL_TABLE
      case PARTIAL_ROUTING => Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE
      case _ => "INVALID"
    }
  }

  def getDefaultBehavior(conf: SQLConf = null): HAC.Type = {
    if (System.getProperty(Constant.defaultBehaviorAsDO_NOTHING, "false").toBoolean) {
      DO_NOTHING
    }
    else if (conf != null) {
      try {
        HAC.getBehavior(Literal.create(Property.Behavior.getOption(conf).getOrElse(
          Constant.BEHAVIOR_RUN_ON_FULL_TABLE),
          StringType))
      } catch {
        case e: UnsupportedOperationException => Property.Behavior.set(conf,
          Constant.BEHAVIOR_RUN_ON_FULL_TABLE)
          throw e
      }
    } else REROUTE_TO_BASE
  }
}
