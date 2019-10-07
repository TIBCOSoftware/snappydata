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

package org.apache.spark.sql.store

import java.sql.{Date, Timestamp}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory
import com.pivotal.gemfirexd.internal.shared.common.reference.Limits
import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.random.XORShiftRandom

/**
 * Base class for common methods for column table tests.
 */
abstract class ColumnTablesTestBase extends SnappyFunSuite {

  protected def normalizeFloat(f: Float): Float = {
    if (java.lang.Float.isNaN(f) || java.lang.Float.isInfinite(f)) {
      0
    } else if (f < Limits.DB2_SMALLEST_REAL) {
      Limits.DB2_SMALLEST_REAL
    } else if (f > Limits.DB2_LARGEST_REAL) {
      Limits.DB2_LARGEST_REAL
    } else if (f > 0 && f < Limits.DB2_SMALLEST_POSITIVE_REAL) {
      Limits.DB2_SMALLEST_POSITIVE_REAL
    } else if (f < 0 && f > Limits.DB2_LARGEST_NEGATIVE_REAL) {
      Limits.DB2_LARGEST_NEGATIVE_REAL
    } else f
  }

  protected def normalizeDouble(d: Double): Double = {
    if (java.lang.Double.isNaN(d) || java.lang.Double.isInfinite(d)) {
      0
    } else if (d < Limits.DB2_SMALLEST_DOUBLE) {
      Limits.DB2_SMALLEST_DOUBLE
    } else if (d > Limits.DB2_LARGEST_DOUBLE) {
      Limits.DB2_LARGEST_DOUBLE
    } else if (d > 0 && d < Limits.DB2_SMALLEST_POSITIVE_DOUBLE) {
      Limits.DB2_SMALLEST_POSITIVE_DOUBLE
    } else if (d < 0 && d > Limits.DB2_LARGEST_NEGATIVE_DOUBLE) {
      Limits.DB2_LARGEST_NEGATIVE_DOUBLE
    } else d
  }

  protected def runAllTypesTest(session: SnappySession,
      numRowsLower: Int = 32000, numRowsUpper: Int = 64760,
      numIterations: Int = 3, process: IndexedSeq[AllTypes] => Unit = _ => ()): Unit = {
    import session.implicits._

    session.sql("CREATE TABLE TypesTable (Index Int not null, T1 Boolean, " +
        "T2 Byte, T3 Short, T4 Int, T5 Long, T6 FLOAT, T7 Double, T8 String, " +
        "T9 Decimal(10, 4), T10 Decimal(35, 15), T11 Date, T12 Timestamp, " +
        "T13 Binary) USING column options (buckets '8')")
    session.sql("CREATE TABLE TypesTable2 (index Int, T1 Boolean NOT NULL, " +
        "T2 Byte NOT NULL, T3 Short NOT NULL, T4 Int NOT NULL, " +
        "T5 Long not null, T6 FLOAT NOT NULL, T7 Double not null, " +
        "T8 String NOT NULL, T9 Decimal(10, 4) NOT NULL, " +
        "T10 Decimal(35, 15) NOT NULL, T11 Date not null, " +
        "T12 Timestamp not null, T13 Binary not null) " +
        "USING column options (buckets '8')")
    session.sql("CREATE TABLE TypesTable3 (Index Int not null, T1 Boolean, " +
        "T2 Integer, T3 smallint, T4 Int, T5 bigint, T6 REAL, T7 Double, T8 varchar(100), " +
        "T9 Decimal(10, 4), T10 Decimal(35, 15), T11 Date, T12 Timestamp, " +
        "T13 blob) USING row")

    val rnd = new XORShiftRandom
    var nonZeroRowBuffer = false
    var c = 1
    while (c <= numIterations || !nonZeroRowBuffer) {
      assert(c <= 100, s"failed to get any data in row buffer in $c tries")
      c += 1
      val numItems = rnd.nextInt(numRowsLower) + (numRowsUpper - numRowsLower)
      val items = (0 until numItems).map { index =>
        val t1 = rnd.nextInt(3) match {
          case 0 => java.lang.Boolean.FALSE
          case 1 => java.lang.Boolean.TRUE
          case 2 => null
        }

        val t2 = rnd.nextInt(150) match {
          case b if b < 128 => Byte.box(b.toByte)
          case _ => null
        }

        val t3 = rnd.nextInt(40000) match {
          case s if s < 32768 => Short.box(s.toShort)
          case _ => null
        }

        val t4 = rnd.nextInt() match {
          case i if i < 1500000000 => Int.box(i)
          case _ => null
        }

        val t5 = rnd.nextLong() match {
          case l if l < 7500000000000000000L => Long.box(l)
          case _ => null
        }

        val t6 = if (t4 ne null) {
          Float.box(normalizeFloat(java.lang.Float.intBitsToFloat(t4)))
        } else null

        val t7 = if (t5 ne null) {
          Double.box(normalizeDouble(java.lang.Double.longBitsToDouble(t5)))
        } else null

        val t8 = if (t7 ne null) t7.toString else null

        val t9 = if ((t3 ne null) && (t2 ne null)) {
          Decimal(math.abs(t3.toInt).toString + '.' + math.abs(t2.toInt).toString)
        } else null

        val t10 = if ((t5 ne null) && (t4 ne null)) {
          Decimal(math.abs(t5).toString + '.' + math.abs(t4).toString)
        } else null

        val t11 = if (t3 ne null) DateTimeUtils.toJavaDate(t3.toInt) else null

        val t12 = if (t4 ne null) DateTimeUtils.toJavaTimestamp(t4.toLong) else null

        val t13 = if (t8 ne null) t8.getBytes else null

        AllTypes(index, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13)
      }

      val ds = session.createDataset(items)
      ds.write.insertInto("TypesTable")
      ds.write.insertInto("TypesTable2")
      ds.write.insertInto("TypesTable3")

      val df = session.sql("select * from TypesTable order by index")
      assert(items === df.as[AllTypes].collect().toSeq)

      val df2 = session.sql("select * from TypesTable2 order by index")
      ColumnTablesTestBase.hasNulls = false
      try {
        assert(items === df2.as[AllTypes].collect().toSeq)
      } finally {
        ColumnTablesTestBase.hasNulls = true
      }

      val df3 = session.sql("select * from TypesTable3 order by index")
      assert(items === df3.as[AllTypes].collect().toSeq)

      process(items)

      if (!nonZeroRowBuffer) {
        nonZeroRowBuffer = Misc.getRegion("/APP/TYPESTABLE", true, false).size() > 0 &&
            Misc.getRegion("/APP/TYPESTABLE2", true, false).size() > 0
      }

      session.truncateTable("TypesTable")
      session.truncateTable("TypesTable2")
      session.truncateTable("TypesTable3")
    }

    session.dropTable("TypesTable")
    session.dropTable("TypesTable2")
    session.dropTable("TypesTable3")
  }
}

object ColumnTablesTestBase {
  var hasNulls = true
}

case class AllTypes(index: Int, t1: java.lang.Boolean, t2: java.lang.Byte,
    t3: java.lang.Short, t4: java.lang.Integer, t5: java.lang.Long,
    t6: java.lang.Float, t7: java.lang.Double, t8: String, t9: Decimal,
    t10: Decimal, t11: Date, t12: Timestamp, t13: Array[Byte]) {

  override def equals(obj: Any): Boolean = obj match {
    case a: AllTypes if ColumnTablesTestBase.hasNulls =>
      index == a.index && t1 == a.t1 && t2 == a.t2 && t3 == a.t3 &&
          t4 == a.t4 && t5 == a.t5 && t6 == a.t6 && t7 == a.t7 && t8 == a.t8 &&
          t9 == a.t9 && t10 == a.t10 && t11 == a.t11 && t12 == a.t12 &&
          java.util.Arrays.equals(t13, a.t13)
    case a: AllTypes =>
      // handle nulls on left side
      val st8 = if (t8 ne null) t8 else ""
      val at13 = if (t13 ne null) t13 else ReuseFactory.getZeroLenByteArray
      index == a.index && ((t1 eq null) || t1 == a.t1) &&
          ((t2 eq null) || t2 == a.t2) && ((t3 eq null) || t3 == a.t3) &&
          ((t4 eq null) || t4 == a.t4) && ((t5 eq null) || t5 == a.t5) &&
          ((t6 eq null) || t6 == a.t6) && ((t7 eq null) || t7 == a.t7) &&
          (st8 == a.t8) && ((t9 eq null) || t9 == a.t9) &&
          ((t10 eq null) || t10 == a.t10) && ((t11 eq null) || t11 == a.t11) &&
          ((t12 eq null) || t12 == a.t12) && java.util.Arrays.equals(at13, a.t13)
    case _ => false
  }
}
