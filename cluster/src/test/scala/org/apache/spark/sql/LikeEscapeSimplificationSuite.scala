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
/*
 * Adapted from Spark's LikeSimplificationSuite having the license below.
q *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Contains, EndsWith, Length, StartsWith}
import org.apache.spark.sql.catalyst.optimizer.LikeSimplification
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.LikeEscapeSimplification

/**
 * Tests for checking of like simplification with escaped wildcard characters.
 */
class LikeEscapeSimplificationSuite extends PlanTest {

  private val testRelation = LocalRelation('a.string)

  test("simplify Like into StartsWith") {
    val originalQuery = testRelation.where(('a like "abc%") || ('a like "abc\\%"))

    val optimized = LikeEscapeSimplification(originalQuery.analyze)
    val optimized2 = LikeSimplification(originalQuery.analyze)
    val correctAnswer = testRelation
        .where(StartsWith('a, "abc") || ('a === "abc%")).analyze
    val correctAnswer2 = testRelation
        .where(StartsWith('a, "abc") || ('a like "abc\\%")).analyze

    comparePlans(optimized, correctAnswer)
    comparePlans(optimized2, correctAnswer2)
  }

  test("simplify Like into EndsWith") {
    val originalQuery = testRelation.where(('a like "%xyz") || ('a like "\\%xyz"))

    val optimized = LikeEscapeSimplification(originalQuery.analyze)
    val optimized2 = LikeSimplification(originalQuery.analyze)
    val correctAnswer = testRelation
        .where(EndsWith('a, "xyz") || ('a === "%xyz")).analyze
    val correctAnswer2 = testRelation
        .where(EndsWith('a, "xyz") || ('a like "\\%xyz")).analyze

    comparePlans(optimized, correctAnswer)
    comparePlans(optimized2, correctAnswer2)
  }

  test("simplify Like into startsWith and EndsWith") {
    val originalQuery = testRelation.where(('a like "abc\\%def") ||
        ('a like "abc%def") || ('a like "abc\\%%def"))

    // no optimization in LikeEscapeSimplification yet for this
    val optimized = LikeEscapeSimplification(originalQuery.analyze)
    val optimized2 = LikeSimplification(originalQuery.analyze)
    val correctAnswer = testRelation
        .where(('a === "abc%def") || ('a like "abc%def") || ('a like "abc\\%%def")).analyze
    val correctAnswer2 = testRelation
        .where(('a like "abc\\%def") ||
            (Length('a) >= 6 && (StartsWith('a, "abc") && EndsWith('a, "def"))) ||
            ('a like "abc\\%%def")).analyze

    comparePlans(optimized, correctAnswer)
    comparePlans(optimized2, correctAnswer2)
  }

  test("simplify Like into Contains") {
    val originalQuery = testRelation
        .where(('a like "%mn%") || ('a like "%mn\\%") ||
            ('a like "%\\%mn%") || ('a like "%mn\\%%") ||
            ('a like "%\\%mn\\%%") || ('a like "%\\_mn\\_%") ||
            ('a like "%%mn\\%%") || ('a like "%\\%_mn\\_%"))

    val optimized = LikeEscapeSimplification(originalQuery.analyze)
    val optimized2 = LikeSimplification(originalQuery.analyze)
    val correctAnswer = testRelation
        .where(Contains('a, "mn") || EndsWith('a, "mn%") ||
            Contains('a, "%mn") || Contains('a, "mn%") ||
            Contains('a, "%mn%") || Contains('a, "_mn_") ||
            ('a like "%%mn\\%%") || ('a like "%\\%_mn\\_%")).analyze
    val correctAnswer2 = testRelation
        .where(Contains('a, "mn") || ('a like "%mn\\%") ||
            ('a like "%\\%mn%") || ('a like "%mn\\%%") ||
            ('a like "%\\%mn\\%%") || ('a like "%\\_mn\\_%") ||
            ('a like "%%mn\\%%") || ('a like "%\\%_mn\\_%")).analyze

    comparePlans(optimized, correctAnswer)
    comparePlans(optimized2, correctAnswer2)
  }

  test("simplify Like into EqualTo") {
    val originalQuery = testRelation.where(('a like "") || ('a like "abc") ||
        ('a like "a\\%b\\_c") || ('a like "\\%abc\\_") || ('a like "\\%abc_"))

    val optimized = LikeEscapeSimplification(originalQuery.analyze)
    val optimized2 = LikeSimplification(originalQuery.analyze)
    val correctAnswer = testRelation
        .where(('a === "") || ('a === "abc") || ('a === "a%b_c") ||
            ('a === "%abc_") || ('a like "\\%abc_")).analyze
    val correctAnswer2 = testRelation
        .where(('a === "") || ('a === "abc") || ('a like "a\\%b\\_c") ||
            ('a like "\\%abc\\_") || ('a like "\\%abc_")).analyze

    comparePlans(optimized, correctAnswer)
    comparePlans(optimized2, correctAnswer2)
  }
}
