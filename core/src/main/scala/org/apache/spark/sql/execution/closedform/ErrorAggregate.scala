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
package org.apache.spark.sql.execution.closedform

object ErrorAggregate extends Enumeration {

  type Type = Value

  val separator = '_'

  val Avg: Type = Value("Avg")
  val Sum: Type = Value("Sum")
  val Count: Type = Value("Count")

  val Sum_Lower: Type = Value(Sum.toString + separator + "Lower")
  val Avg_Lower: Type = Value(Avg.toString + separator + "Lower")
  val Count_Lower: Type = Value(Count.toString + separator + "Lower")

  val Sum_Upper: Type = Value(Sum.toString + separator + "Upper")
  val Avg_Upper: Type = Value(Avg.toString + separator + "Upper")
  val Count_Upper: Type = Value(Count.toString + separator + "Upper")

  // relative error
  val Sum_Relative: Type = Value(Sum.toString + separator + "Relative")
  val Avg_Relative: Type = Value(Avg.toString + separator + "Relative")
  val Count_Relative: Type = Value(Count.toString + separator + "Relative")

  // absolute error
  val Sum_Absolute: Type = Value(Sum.toString + separator + "Absolute")
  val Avg_Absolute: Type = Value(Avg.toString + separator + "Absolute")
  val Count_Absolute: Type = Value(Count.toString + separator + "Absolute")

  def getBaseAggregateType(param: ErrorAggregate.Type): ErrorAggregate.Type = {
    val name = param.toString
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      param
    } else {
      val baseName = name.substring(0, sepIndex)
      ErrorAggregate.withName(baseName)
    }
  }

  def getRelativeErrorTypeForBaseType(baseAggregateType: Type): Type = {
    val relErrorName = baseAggregateType.toString + separator + "Relative"
    ErrorAggregate.withName(relErrorName)
  }

  def isBaseAggType(aggType: Type): Boolean = {
    val name = aggType.toString
    val sepIndex = name.indexOf(separator)
    sepIndex == -1
  }

  private def getSuffix(name: String): Option[String] = {
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      None
    } else {
      Some(name.substring(sepIndex + 1))
    }
  }

  private def getPrefix(name: String): Option[String] = {
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      None
    } else {
      Some(name.substring(0, sepIndex))
    }
  }

  def checkFor(suffix: String, aggType: Type): Boolean = {
    getSuffix(aggType.toString) match {
      case Some(x) => x == suffix
      case None => false
    }
  }

  def checkFor(prefix: String, errorEstimateFuncName: String): Boolean = {
    getPrefix(errorEstimateFuncName) match {
      case Some(x) => x == prefix
      case None => false
    }
  }

  def isLowerAggType(aggType: Type): Boolean = checkFor("Lower", aggType)

  def isUpperAggType(aggType: Type): Boolean = checkFor("Upper", aggType)

  def isRelativeErrorAggType(aggType: Type): Boolean =
    checkFor("Relative", aggType)

  def isAbsoluteErrorAggType(aggType: Type): Boolean =
    checkFor("Absolute", aggType)

  def isLowerAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Lower", errorEstimateFuncName)

  def isUpperAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Upper", errorEstimateFuncName)

  def isRelativeErrorAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Relative", errorEstimateFuncName)

  def isAbsoluteErrorAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Absolute", errorEstimateFuncName)
}
