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
package org.apache.spark.sql.execution

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.StringType

/**
 * Makes use of dictionary indexes for strings if any.
 * Depends only on the presence of dictionary per batch of rows (where the batch
 * must be substantially greater than its dictionary for optimization to help).
 *
 * For single column hash maps (groups or joins), it can be turned into a flat
 * indexed array instead of a map. Create an array of class objects as stored
 * in [[ObjectHashSet]] having the length same as dictionary so that dictionary
 * index can be used to directly lookup the array. Then for the first lookup
 * into the array for a dictionary index, lookup the actual [[ObjectHashSet]]
 * for the key to find the map entry object and insert into the array.
 * An alternative would be to pre-populate the array by making one pass through
 * the dictionary, but it may not be efficient if many of the entries in the
 * dictionary get filtered out by query predicates and never need to consult
 * the created array.
 *
 * For multiple column hash maps having one or more dictionary indexed columns,
 * there is slightly more work. Instead of an array as in single column case,
 * create a new hash map where the key columns values are substituted by
 * dictionary index value. However, the map entry will remain identical to the
 * original map so to save space add the additional index column to the full
 * map itself. As new values are inserted into this hash map, lookup the full
 * hash map to locate its map entry, then point to the same map entry in this
 * new hash map too. Thus for subsequent look-ups the new hash map can be used
 * completely based on integer dictionary indexes instead of strings.
 *
 * An alternative approach can be to just store the hash code arrays separately
 * for each of the dictionary columns indexed identical to dictionary. Use
 * this to lookup the main map which will also have additional columns for
 * dictionary indexes (that will be cleared at the start of a new batch).
 * One first lookup for key columns where dictionary indexes are missing in
 * the map, insert the dictionary index in those additional columns.
 * Then use those indexes for equality comparisons instead of string.
 *
 * The multiple column dictionary optimization will be useful for only string
 * dictionary types where cost of looking up a string in hash map is
 * substantially higher than integer lookup. The single column optimization
 * can improve performance for other dictionary types though its efficacy
 * for integer/long types will be reduced to avoiding hash code calculation.
 * Given this, the additional overhead of array maintenance may not be worth
 * the effort (and could possibly even reduce overall performance in some
 * cases), hence this optimization is currently only for string type.
 */
object DictionaryOptimizedMapAccessor {

  def checkSingleKeyCase(keyExpressions: Seq[Expression],
      keyVars: => Seq[ExprCode], ctx: CodegenContext,
      session: SnappySession): Option[ExprCodeEx] = {
    if (keyExpressions.length == 1 &&
        keyExpressions.head.dataType.isInstanceOf[StringType]) {
      session.getExCode(ctx, keyVars.head.value :: Nil, keyExpressions) match {
        case e@Some(ExprCodeEx(_, _, dictionary, _)) if dictionary.nonEmpty => e
        case _ => None
      }
    } else None
  }

  def dictionaryArrayGetOrInsert(ctx: CodegenContext, keyVar: ExprCode,
      keyVarEx: ExprCodeEx, arrayVar: String, resultVar: String,
      valueInit: String, accessor: ObjectHashMapAccessor): String = {
    val key = keyVar.value
    val keyIndex = keyVarEx.dictionaryIndex
    val keyNull = keyVar.isNull != "false"
    val keyExpr = ExprCode("", if (keyNull) s"($key == null)" else "false", key)
    val className = accessor.getClassName

    // for the case when there is no entry in map (hash join), insert a token
    // in the array to avoid looking up missing entries repeatedly
    val arrayAssignFragment = if (valueInit eq null) {
      s"""
         |  if ($resultVar != null) {
         |    $arrayVar[$keyIndex] = $resultVar;
         |  } else {
         |    $arrayVar[$keyIndex] = $className.EMPTY;
         |  }
         |} else if ($resultVar == $className.EMPTY) {
         |  $resultVar = null;
      """.stripMargin
    } else s"$arrayVar[$keyIndex] = $resultVar;"

    val hash = ctx.freshName("keyHash")
    val hashExprCode = if (keyNull) s"$key != null ? $key.hashCode() : -1"
    else s"$key.hashCode()"
    // if hash has already been calculated then use it
    val hashExpr = keyVarEx.hash match {
      case Some(h) =>
        s"""if ($h == 0) $h = $hashExprCode;
          final int $hash = $h;"""
      case None => s"final int $hash = $hashExprCode;"
    }

    // if keyVar code has been consumed, then dictionary index
    // has already been assigned and likewise the key itself
    val (dictionaryCode, keyAssign) = if (keyVar.code.isEmpty) ("", "")
    else (keyVarEx.dictionaryCode,
        s"final UTF8String $key = ${keyVarEx.dictionary}[$keyIndex];")
    s"""
       |$dictionaryCode
       |$resultVar = $arrayVar[$keyIndex];
       |if ($resultVar == null) {
       |  $keyAssign
       |  $hashExpr
       |  ${accessor.mapGetOrInsert(resultVar, hash, Seq(keyExpr), valueInit)}
       |  $arrayAssignFragment
       |}
    """.stripMargin
  }
}
