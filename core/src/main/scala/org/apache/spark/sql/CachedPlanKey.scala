/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.ParamLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

case class CachedPlanKey(val session: SnappySession, val parsedPlan: LogicalPlan, val sqlText: String) {
//  /**
//    * The hashcode calculated ignores the actual values of the literal.
//    * TODO: check for proper hashcode function. sbox? See whats being used
//    * in other parts of the code.
//    */
//  override def hashCode(): Int = {
//    var hash: Int = session.hashCode()
//    parsedPlan.foreach {
//      x => {
//        x match {
//          case pl: ParamLiteral => hash = hash ^ pl.pos ^ pl.dataType.hashCode()
//          case tNode: TreeNode[Any] => hash = hash ^ tNode.hashCode()
//        }
//      }
//    }
//    hash
//  }
//
//  val all: PartialFunction[LogicalPlan, TreeNode[Any]] = {
//    case x: TreeNode[Any] => x
//  }
//
//  /**
//    * The equals calculated ignores the actual values of the literals.
//    */
//  override def equals(obj: scala.Any): Boolean = {
//    if (session != obj) return false
//    obj match {
//      case otherCpk: CachedPlanKey => {
//        val otherSeq = otherCpk.parsedPlan.collect[TreeNode[Any]](all)
//        val thisSeq = parsedPlan.collect[TreeNode[Any]](all)
//        if (otherSeq == null || (otherSeq.size != thisSeq.size)) {
//          false
//        }
//        else {
//          val zippedSeq = thisSeq.zip(otherSeq)
//          for ((n1, n2) <- zippedSeq) {
//            n1 match {
//              case pl: ParamLiteral => {
//                n2 match {
//                  case pl2: ParamLiteral => {
//                    if (pl.dataType != pl2.dataType || pl.pos != pl2.pos) {
//                      return false
//                    }
//                  }
//                  case _ => return false
//                }
//              }
//              case _ => {
//                // TODO: Intention is to do node by node comparison
//                if (!n1.fastEquals(n2)) {
//                  return false
//                }
//              }
//            }
//          }
//          true
//        }
//      }
//      case _ => false
//    }
//  }
}
