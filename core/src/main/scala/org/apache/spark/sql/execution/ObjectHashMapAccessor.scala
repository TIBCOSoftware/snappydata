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

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, BindReferences, BoundReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods

/**
 * Provides helper methods for generated code to use ObjectHashSet with a
 * generated class (having key and value columns as corresponding java type
 * fields). This implementation saves the entire overhead of UnsafeRow
 * conversion for both key type (like in BytesToBytesMap) and value type
 * (like in BytesToBytesMap and VectorizedHashMapGenerator).
 * <p>
 * It has been carefully optimized to minimize memory reads/writes, with
 * minimalistic code to fit better in CPU instruction cache. Unlike the other
 * two maps used by HashAggregateExec, this has no limitations on the key or
 * value column types.
 * <p>
 * The basic idea being that all of the key and value columns will be
 * individual fields in a generated java class having corresponding java
 * types. Storage of a column value in the map is a simple matter of assignment
 * of incoming variable to the corresponding field of the class object and
 * access is likewise read from that field of class . Nullability information
 * is crammed in long bit-mask fields which are generated as many required
 * (instead of unnecessary overhead of something like a BitSet).
 * <p>
 * Hashcode and equals methods are generated for the key column fields.
 * Having both key and value fields in the same class object helps both in
 * cutting down of generated code as well as cache locality and reduces at
 * least one memory access for each row. In testing this alone has shown to
 * improve performance by ~25% in simple group by queries. Furthermore, this
 * class also provides for inline hashcode and equals methods so that incoming
 * register variables in generated code can be directly used (instead of
 * stuffing into a lookup key that will again read those fields inside). The
 * class hashcode method is supposed to be used only internally by rehashing
 * and that too is just a field cached in the class object that is filled in
 * during the initial insert (from the inline hashcode).
 * <p>
 * For memory management this uses a simple approach of starting with an
 * estimated size, then improving that estimate for future in a rehash where
 * the rehash will also collect the actual size of current entries.
 * If the rehash tells that no memory is available, then it will fallback
 * to dumping the current map into MemoryManager and creating a new one
 * with merge being done by an external sorter in a manner similar to
 * how UnsafeFixedWidthAggregationMap handles the situation. Caller can
 * instead decide to dump the entire map in that scenario like when using
 * for a HashJoin.
 * <p>
 * Overall this map is 5-10X faster than UnsafeFixedWidthAggregationMap
 * and 2-4X faster than VectorizedHashMapGenerator. It is generic enough
 * to be used for both group by aggregation as well as for HashJoins.
 */
final case class ObjectHashMapAccessor(session: SnappySession,
    ctx: CodegenContext, classPrefix: String, keyExpressions: Seq[Expression],
    hashMapTerm: String, dataTerm: String, maskTerm: String, multiMap: Boolean,
    mapSchema: StructType, consumer: CodegenSupport, cParent: CodegenSupport,
    override val output: Seq[Attribute], override val child: SparkPlan)
    extends UnaryExecNode with CodegenSupport {

  val valueIndex = keyExpressions.length

  private[this] val keyExprIds = keyExpressions.zipWithIndex.collect {
    case (ne: NamedExpression, index) => ne.exprId -> index
  }.toMap

  lazy val (integralKeys, integralKeysMinVars, integralKeysMaxVars) =
    keyExpressions.zipWithIndex.collect {
    case (expr, index) if isIntegralType(expr.dataType) =>
      (index, ctx.freshName("minValue"), ctx.freshName("maxValue"))
  }.unzip3

  private[this] val hashingClass = classOf[HashingUtil].getName
  private[this] val nullsMaskPrefix = "nullsMask"
  /**
   * Indicator value for "nullIndex" of a non-primitive nullable that can be
   * checked using its value rather than a separate bit mask.
   */
  private[this] val NULL_NON_PRIM = -2
  private[this] val multiValuesVar = "multiValues"
  private[this] val localValueVar = "value"

  private type ClassVarType = (DataType, String, ExprCode, Int)

  private[this] val (className, valueClassName, classVars, numNullVars) =
    initClass()

  private def initClass(): (String, String, IndexedSeq[ClassVarType], Int) = {

    // check for existing class with same schema
    val types = mapSchema.map(f => f.dataType -> f.nullable)
    val valueTypes = if (multiMap) types.drop(valueIndex) else Nil
    val (newClass, valueClass, exists) = session.getClass(ctx, types) match {
      case Some(r) =>
        val valClass =
          if (valueTypes.nonEmpty) session.getClass(ctx, valueTypes).get else ""
        (r, valClass, true)
      case None =>
        val entryClass = ctx.freshName(classPrefix)
        val valClass = if (valueTypes.nonEmpty) "Val_" + entryClass else ""
        (entryClass, valClass, false)
    }

    // local variable name for other object in equals
    val other = "other"

    // For the case of multi-map, key fields cannot be null. Create a
    // separate value class that the main class will extend. The main class
    // object will have value class as an array (possibly null) that holds
    // any additional values beyond the ones already inherited.
    val startNullIndex = if (multiMap) valueIndex else 0
    val (newClassVars, numNulls, nullDecls) = createClassVars(mapSchema,
      startNullIndex, exists)
    if (!exists) {
      // generate equals code for key columns only
      val keyVars = newClassVars.take(valueIndex)
      val classVars = if (multiMap) keyVars else newClassVars
      val valueVars = if (multiMap) newClassVars.drop(valueIndex) else Nil
      val equalsCode = keyVars.map {
        case (dataType, _, ExprCode(_, nullVar, varName), nullIndex) =>
          genEqualsCode("this", varName, nullVar, other,
            varName, nullVar, nullIndex, isPrimitiveType(dataType), dataType)
      }.mkString(" &&\n")
      val (valueClassCode, extendsCode, nulls, multiValues) =
        if (valueVars.nonEmpty) {
          (s"""
            public static class $valueClass {
              $nullDecls
              ${valueVars.map(e => s"${e._2} ${e._3.value};").mkString("\n")}
            }
          """, s" extends $valueClass", "",
              s"$valueClass[] $multiValuesVar;")
        } else ("", "", nullDecls, "")
      val classCode =
        s"""
          public static final class $newClass$extendsCode {
            $nulls
            ${classVars.map(e => s"${e._2} ${e._3.value};").mkString("\n")}
            $multiValues
            final int hash;

            public $newClass(int h) {
              this.hash = h;
            }

            public int hashCode() {
              return this.hash;
            }

            public boolean equals(Object o) {
              final $newClass $other = ($newClass)o;
              return $equalsCode;
            }
          };
        """
      // using addNewFunction to register the class since there is nothing
      // function specific in the addNewFunction method
      if (!valueClassCode.isEmpty) {
        ctx.addNewFunction(valueClass, valueClassCode)
        session.addClass(ctx, valueTypes, valueClass)
      }
      ctx.addNewFunction(newClass, classCode)
      session.addClass(ctx, types, newClass)
    }

    (newClass, valueClass, newClassVars, numNulls)
  }

  private def createClassVars(schema: StructType, startNullIndex: Int,
      exists: Boolean): (IndexedSeq[ClassVarType], Int, String) = {
    // collect the null field declarations
    val nullMaskDeclarations = new StringBuilder

    var numNulls = -1
    var currNullVar = ""

    val classVars = new mutable.ArrayBuffer[ClassVarType]()
    schema.indices.foreach { index =>
      getKeyRefForValue(index, onlyValue = false) match {
        case None =>
          val f = schema(index)
          val varName = s"field$index"
          val dataType = f.dataType
          val javaType = dataType match {
            // use raw byte arrays for strings to minimize overhead
            case StringType => "byte[]"
            case _ => ctx.javaType(dataType)
          }
          val (nullVar, nullIndex) = if (index >= startNullIndex && f.nullable) {
            if (isPrimitiveType(dataType)) {
              numNulls += 1
              // each long can hold bit mask for 64 nulls
              val nullIndex = numNulls % 64
              if (nullIndex == 0) {
                currNullVar = s"$nullsMaskPrefix${numNulls / 64}"
                if (!exists) {
                  nullMaskDeclarations.append(s"long $currNullVar;\n")
                }
              }
              (currNullVar, nullIndex)
            } else ("", NULL_NON_PRIM)
          } else ("", -1)
          classVars += ((dataType, javaType, ExprCode("", nullVar, varName),
              nullIndex))
        // if a value field is already a key column, then point to the same
        case Some(i) => classVars += classVars(i)
      }
    }
    val numNullVars = if (numNulls >= 0) (numNulls / 64) + 1 else 0
    (classVars, numNullVars, nullMaskDeclarations.toString())
  }

  private def getKeyRefForValue(index: Int, onlyValue: Boolean): Option[Int] = {
    if (child == null) None
    else if (onlyValue) keyExprIds.get(output(index + valueIndex).exprId)
    else if (index >= valueIndex) keyExprIds.get(output(index).exprId)
    else None
  }

  private def getKeyVars(input: Seq[ExprCode]): Seq[ExprCode] = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val vars = ctx.generateExpressions(keyExpressions.map(e =>
      BindReferences.bindReference[Expression](e, output)))
    ctx.currentVars = null
    vars
  }

  private def getValueVars(input: Seq[ExprCode]): Seq[ExprCode] = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val vars = ctx.generateExpressions(output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    })
    ctx.currentVars = null
    vars
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("unexpected call")

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq.empty

  override protected def doProduce(ctx: CodegenContext): String =
    throw new UnsupportedOperationException("unexpected invocation")

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    // consume the data and populate the map
    val entryVar = "mapEntry" // local variable
    val hashVar = ctx.freshName("hash")
    val posVar = ctx.freshName("pos")
    val deltaVar = ctx.freshName("delta")
    val keyVars = getKeyVars(input)
    val valueVars = getValueVars(input)
    // update min/max for primitive type columns
    val updateMinMax = integralKeys.map { index =>
      s"$hashMapTerm.updateLimits(${keyVars(index).value}, $index);"
    }.mkString("\n")
    val multiValuesUpdateCode = if (valueClassName.isEmpty) "// no value field"
    else {
      s"""
        // add to multiValues array
        final int valueIndex;
        $valueClassName[] values = $entryVar.$multiValuesVar;
        if (values != null) {
          valueIndex = values.length;
          $valueClassName[] newValues = new $valueClassName[valueIndex + 1];
          java.lang.System.arrayCopy(values, 0, newValues, 0, valueIndex);
          values = newValues;
        } else {
          valueIndex = 0;
          values = new $valueClassName[1];
        }

        final $valueClassName newValue = new $valueClassName();
        values[valueIndex] = newValue;
        $entryVar.$multiValuesVar = values;
        ${generateUpdate("newValue", Nil, valueVars, forKey = false)}

        // key is not unique
        $hashMapTerm.setKeyIsUnique(false);"""
    }
    s"""
      // evaluate the key and value expressions
      ${evaluateVariables(keyVars)}${evaluateVariables(valueVars)}
      // skip if any key is null
      if (${keyVars.map(_.isNull).mkString(" ||\n")}) continue;
      // generate hash code
      int $hashVar;
      ${generateHashCode(hashVar, keyVars, keyExpressions)}
      // lookup or insert the grouping key in map
      // using inline get call so that equals() is inline using
      // existing register variables instead of having to fill up
      // a lookup key fields and compare against those (thus saving
      //   on memory writes/reads vs just register reads)
      int $posVar = $hashVar & $maskTerm;
      int $deltaVar = 1;
      while (true) {
        $className $entryVar = $dataTerm[$posVar];
        if ($entryVar != null) {
          if (${generateEquals(entryVar, keyVars)}) {
            $multiValuesUpdateCode
            break;
          } else {
            // quadratic probing with position increase by 1, 2, 3, ...
            $posVar = ($posVar + $deltaVar) & $maskTerm;
            $deltaVar++;
          }
        } else {
          $entryVar = new $className($hashVar);
          // initialize the key fields
          ${generateUpdate(entryVar, Nil, keyVars, forKey = true)}
          // initialize the value fields
          ${generateUpdate(entryVar, Nil, valueVars, forKey = false)}
          // insert into the map and rehash if required
          $dataTerm[$posVar] = $entryVar;
          if ($hashMapTerm.handleNewInsert()) {
            // map was rehashed
            $maskTerm = $hashMapTerm.mask();
            $dataTerm = ($className[])$hashMapTerm.data();
          }
          $updateMinMax
          break;
        }
      }
    """
  }

  /** get the generated class name */
  def getClassName: String = className

  /**
   * Generate code to calculate the hash code for given column variables that
   * correspond to the key columns in this class.
   */
  def generateHashCode(hashVar: String, keyVars: Seq[ExprCode],
      keyExpressions: Seq[Expression]): String = {
    // check if hash has already been generated for keyExpressions
    val vars = keyVars.map(_.value)
    val (prefix, suffix) = session.getExCode(ctx, vars, keyExpressions) match {
      case Some(ExprCodeEx(Some(hash), _, _, _)) =>
        (s"if (($hashVar = $hash) == 0) {\n", "}\n")
      case _ => ("", "")
    }

    // register the hash variable for the key expressions
    session.addExCodeHash(ctx, vars, keyExpressions, hashVar)

    // optimize for first column to use fast hashing
    val expr = keyVars.head
    val colVar = expr.value
    val nullVar = expr.isNull
    val firstColumnHash = classVars(0)._1 match {
      case BooleanType =>
        hashSingleInt(s"($colVar) ? 1 : 0", nullVar, hashVar)
      case ByteType | ShortType | IntegerType | DateType =>
        hashSingleInt(colVar, nullVar, hashVar)
      case LongType | TimestampType =>
        hashSingleLong(colVar, nullVar, hashVar)
      case FloatType =>
        hashSingleInt(s"Float.floatToIntBits($colVar)", nullVar, hashVar)
      case DoubleType =>
        hashSingleLong(s"Double.doubleToLongBits($colVar)", nullVar, hashVar)
      case d: DecimalType =>
        hashSingleInt(s"$colVar.fastHashCode()", nullVar, hashVar)
      // single column types that use murmur hash already,
      // so no need to further apply mixing on top of it
      case _: StringType | _: ArrayType | _: StructType =>
        s"$hashVar = ${hashCodeSingleInt(s"$colVar.hashCode()", nullVar)};\n"
      case _ =>
        hashSingleInt(s"$colVar.hashCode()", nullVar, hashVar)
    }
    if (keyVars.length > 1) {
      classVars.tail.zip(keyVars.tail).map {
        case ((BooleanType, _, _, _), ev) =>
          addHashInt(s"${ev.value} ? 1 : 0", ev.isNull, hashVar)
        case ((ByteType | ShortType | IntegerType | DateType, _, _, _), ev) =>
          addHashInt(ev.value, ev.isNull, hashVar)
        case ((LongType | TimestampType, _, _, _), ev) =>
          addHashLong(ev.value, ev.isNull, hashVar)
        case ((FloatType, _, _, _), ev) =>
          addHashInt(s"Float.floatToIntBits(${ev.value})", ev.isNull, hashVar)
        case ((DoubleType, _, _, _), ev) =>
          addHashLong(s"Double.doubleToLongBits(${ev.value})", ev.isNull,
            hashVar)
        case ((d: DecimalType, _, _, _), ev) =>
          addHashInt(s"${ev.value}.fastHashCode()", ev.isNull, hashVar)
        case (_, ev) =>
          addHashInt(s"${ev.value}.hashCode()", ev.isNull, hashVar)
      }.mkString(prefix + firstColumnHash, "", suffix)
    } else prefix + firstColumnHash + suffix
  }

  /**
   * Generate code to compare equality of a given object (objVar) against
   * key column variables.
   */
  def generateEquals(objVar: String,
      keyVars: Seq[ExprCode]): String = classVars.zip(keyVars).map {
    case ((dataType, _, ExprCode(_, nullVar, varName), nullIndex), colVar) =>
      genEqualsCode("", colVar.value, colVar.isNull, objVar, varName,
        nullVar, nullIndex, isPrimitiveType(dataType), dataType)
  }.mkString(" &&\n")

  /**
   * Get the ExprCode for the key and/or value columns given a class object
   * variable. This also returns an initialization code that should be inserted
   * in generated code first.
   */
  def getColumnVars(objVar: String, onlyKeyVars: Boolean,
      onlyValueVars: Boolean,
      separateNullVars: Boolean = false): (String, Seq[ExprCode]) = {
    // Generate initial declarations for null masks to avoid reading those
    // repeatedly. Caller is supposed to insert the code at the start.
    val declarations = new StringBuilder
    val nullLocalVars = (0 until numNullVars).map { index =>
      val nullVar = s"$nullsMaskPrefix$index"
      val nullLocalVar = ctx.freshName("localNullsMask")
      declarations.append(s"final long $nullLocalVar = $objVar.$nullVar;\n")
      nullVar -> nullLocalVar
    }.toMap

    val vars = if (onlyKeyVars) classVars.take(valueIndex)
    else if (onlyValueVars) classVars.drop(valueIndex) else classVars

    val columnVars = new mutable.ArrayBuffer[ExprCode]()
    vars.indices.foreach { index =>
      getKeyRefForValue(index, onlyValueVars) match {
        case None =>
          // skip the variable if it is not in the final output
          val (dataType, javaType, ev, nullIndex) = vars(index)
          val (localVar, localDeclaration) = {
            if (ev.value.isEmpty) {
              // single column no-wrapper case
              (objVar, new StringBuilder)
            } else dataType match {
              case StringType =>
                // wrap the bytes in UTF8String
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(s"final UTF8String $lv = " +
                    s"UTF8String.fromBytes($objVar.${ev.value});"))
              case _ =>
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(
                  s"final $javaType $lv = $objVar.${ev.value};"))
            }
          }
          val nullExpr = nullLocalVars.get(ev.isNull)
              .map(genNullCode(_, nullIndex)).getOrElse(
            if (nullIndex == NULL_NON_PRIM) s"($localVar == null)"
            else "false")
          val nullVar = if (separateNullVars) {
            val nv = ctx.freshName("isNull")
            localDeclaration.append(s"\nboolean $nv = $nullExpr;")
            nv
          } else nullExpr
          columnVars += ExprCode(localDeclaration.toString, nullVar, localVar)
        // if a value field is already a key column, then point to the same
        case Some(i) => columnVars += columnVars(i).copy(code = "")
      }
    }
    (declarations.toString(), columnVars)
  }

  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(objVar: String, valueInitVars: Seq[ExprCode],
      valueInitCode: String, input: Seq[ExprCode]): String = {
    val hashVar = ctx.freshName("hash")
    val posVar = ctx.freshName("pos")
    val deltaVar = ctx.freshName("delta")
    val keyVars = getKeyVars(input)
    val valueInit = generateUpdate(objVar, Nil, valueInitVars, forKey = false,
      doCopy = false)
    s"""
      // evaluate the key expressions
      ${evaluateVariables(keyVars)}
      // evaluate the hash code of the lookup key
      int $hashVar;
      ${generateHashCode(hashVar, keyVars, keyExpressions)}
      // lookup or insert the grouping key in map
      // using inline get call so that equals() is inline using
      // existing register variables instead of having to fill up
      // a lookup key fields and compare against those (thus saving
      //   on memory writes/reads vs just register reads)
      $className $objVar;
      int $posVar = $hashVar & $maskTerm;
      int $deltaVar = 1;
      while (true) {
        final $className key = $dataTerm[$posVar];
        if (key != null) {
          $objVar = key;
          if (${generateEquals(objVar, keyVars)}) {
            break;
          } else {
            // quadratic probing with position increase by 1, 2, 3, ...
            $posVar = ($posVar + $deltaVar) & $maskTerm;
            $deltaVar++;
          }
        } else {
          $objVar = new $className($hashVar);
          // initialize the value fields to defaults
          $valueInitCode
          $valueInit
          // initialize the key fields
          ${generateUpdate(objVar, Nil, keyVars, forKey = true)}
          // insert into the map and rehash if required
          $dataTerm[$posVar] = $objVar;
          if ($hashMapTerm.handleNewInsert()) {
            // map was rehashed
            $maskTerm = $hashMapTerm.mask();
            $dataTerm = ($className[])$hashMapTerm.data();
          }

          break;
        }
      }
    """
  }

  def getMultiMapVars(entryVar: String,
      joinType: JoinType): (Seq[ExprCode], Seq[ExprCode], String) = {
    // for outer join use separate isNull variables that can be set if no match
    val separateNullVars = joinType match {
      case LeftOuter | RightOuter | FullOuter => true
      case _ => false
    }
    // keys can never be null for this case, hence skip null declarations
    val (_, keyVars) = getColumnVars(entryVar, onlyKeyVars = true,
      onlyValueVars = false, separateNullVars)
    val (valueInit, valueVars) = if (valueClassName.isEmpty) ("", Nil)
    else getColumnVars(localValueVar, onlyKeyVars = false,
      onlyValueVars = true, separateNullVars)
    (keyVars, valueVars, valueInit)
  }

  private def getConsumeResultCode(numRows: String,
      resultVars: Seq[ExprCode]): String =
    s"$numRows++;\n${consumer.consume(ctx, resultVars)}"

  // scalastyle:off
  def generateMapLookup(entryVar: String, keyIsUnique: String, numRows: String,
      valueInit: String, checkCondition: Option[ExprCode],
      streamKeys: Seq[Expression], streamKeyVars: Seq[ExprCode],
      buildVars: Seq[ExprCode], input: Seq[ExprCode],
      resultVars: Seq[ExprCode], joinType: JoinType): String = {
    // scalastyle:on

    val hashVar = ctx.freshName("hash")
    // these are all local variables inside private block, so no ctx.freshName
    val posVar = "pos"
    val deltaVar = "delta"

    // if consumer is a projection that will project away key columns,
    // then avoid materializing those
    val mapKeyVars = buildVars.take(valueIndex)
    val mapKeyCodes = cParent match {
      case ProjectExec(projection, _) =>
        mapKeyVars.zip(keyExpressions).collect {
          case (ev, ne: NamedExpression) if !ev.code.isEmpty &&
              projection.exists(_.exprId == ne.exprId) => ev.code
          case (ev, expr) if !ev.code.isEmpty &&
              projection.exists(_.semanticEquals(expr)) => ev.code
        }.mkString("\n")
      case _ => mapKeyVars.filter(!_.code.isEmpty).map(_.code)
          .mkString("\n")
    }

    // invoke generateHashCode before consume so that hash variables
    // can be re-used by consume if possible
    val streamHashCode = generateHashCode(hashVar, streamKeyVars, streamKeys)
    // if a stream-side key is null then skip (or null for outer join)
    val nullStreamKey = streamKeyVars.map(v => s"!${v.isNull}")
    // filter as per min/max if provided; the min/max variables will be
    // initialized by the caller outside the loop after creating the map
    val minMaxFilter = integralKeys.map { index =>
      val keyVar = streamKeyVars(index).value
      val minVar = integralKeysMinVars(index)
      val maxVar = integralKeysMaxVars(index)
      s"$keyVar >= $minVar && $keyVar <= $maxVar"
    }
    // generate the initial filter condition from above two
    val initFilters = nullStreamKey ++ minMaxFilter
    val initFilterCode = if (initFilters.isEmpty) ""
    else initFilters.mkString("if (", " &&\n", ")")

    // common multi-value iteration code fragments
    val declareLocalValueVars =
      s"""
        int valueIndex = -1;
        int numValues = 0;
        $valueClassName[] values = null;
        $valueClassName $localValueVar = $entryVar;"""
    val moveNextValue =
      s"""
        if (valueIndex != -1) {
          if (valueIndex < numValues) {
            $localValueVar = values[valueIndex++];
          } else {
            break;
          }
        } else {
          if ((values = $entryVar.$multiValuesVar) != null) {
            valueIndex = 1;
            numValues = values.length;
            $localValueVar = values[0];
          } else {
            break;
          }
        }"""
    // Code fragments for different join types.
    // This is to ensure only a single parent.consume() because the branches
    // can be taken alternately in the worst case so then it can lead to
    // large increase in instruction cache misses even though most of the code
    // will be the common parent's consume call.
    val (keyConsume, entryConsume) = joinType match {
      case Inner => genInnerJoinCodes(entryVar, mapKeyCodes, checkCondition,
        valueInit, numRows, getConsumeResultCode(numRows, resultVars),
        keyIsUnique, declareLocalValueVars, moveNextValue)

      case LeftOuter | RightOuter => genOuterJoinCodes(entryVar, buildVars,
        mapKeyCodes, checkCondition, valueInit, numRows,
        getConsumeResultCode(numRows, resultVars), keyIsUnique,
        declareLocalValueVars, moveNextValue)

      case LeftSemi => genSemiJoinCodes(entryVar, mapKeyCodes, checkCondition,
        valueInit, numRows, getConsumeResultCode(numRows, input),
        keyIsUnique, declareLocalValueVars, moveNextValue)

      case LeftAnti => genAntiJoinCodes(entryVar, mapKeyCodes, checkCondition,
        valueInit, numRows, getConsumeResultCode(numRows, input),
        keyIsUnique, declareLocalValueVars, moveNextValue)

      case _: ExistenceJoin =>
        // declare and add the exists variable to resultVars
        val existsVar = ctx.freshName("exists")
        genExistenceJoinCodes(entryVar, existsVar, mapKeyCodes,
          checkCondition, valueInit, numRows, getConsumeResultCode(numRows,
            input :+ ExprCode("", "false", existsVar)), keyIsUnique,
          declareLocalValueVars, moveNextValue)

      case _ => throw new IllegalArgumentException(
        s"LocalJoin should not take $joinType as the JoinType")
    }

    s"""
      $className $entryVar = null;
      int $hashVar = 0;
      // check if any join key is null or min/max for integral keys
      $initFilterCode {
        // generate hash code from stream side key columns
        $streamHashCode
        // Lookup the key in map and consume all values.
        // Using inline get call so that equals() is inline using
        // existing register variables instead of having to fill up
        // a lookup key fields and compare against those.
        // Start with the full class object then read the values array.
        int $posVar = $hashVar & $maskTerm;
        int $deltaVar = 1;
        while (true) {
          $entryVar = $dataTerm[$posVar];
          if ($entryVar != null) {
            if (${generateEquals(entryVar, streamKeyVars)}) {
              break;
            } else {
              // quadratic probing with position increase by 1, 2, 3, ...
              $posVar = ($posVar + $deltaVar) & $maskTerm;
              $deltaVar++;
            }
          } else {
            // key not found so filter out the row with entry as null
            break;
          }
        }
      }
      ${if (valueClassName.isEmpty) keyConsume else entryConsume}
    """
  }

  /**
   * Generate code to update a class object fields with given resultVars. If
   * accessors for fields have been generated (using <code>getColumnVars</code>)
   * then those can be passed for faster reads where required.
   *
   * @param objVar     the variable holding reference to the class object
   * @param columnVars accessors for object fields, if available
   * @param resultVars result values to be assigned to object fields
   * @param forKey     if true then update key fields else value fields
   * @param doCopy     if true then a copy of reference values is assigned
   *                   else only reference copy done
   * @param forInit    if true then this is for initialization of fields
   *                   after object creation so some checks can be skipped
   * @return code to assign objVar fields to given resultVars
   */
  def generateUpdate(objVar: String, columnVars: Seq[ExprCode],
      resultVars: Seq[ExprCode], forKey: Boolean,
      doCopy: Boolean = true, forInit: Boolean = true): String = {
    val fieldVars = if (forKey) classVars.take(valueIndex)
    else classVars.drop(valueIndex)

    val nullLocalVars = if (columnVars.isEmpty) {
      // get nullability from object fields
      fieldVars.map(e => genNullCode(s"$objVar.${e._3.isNull}", e._4))
    } else {
      // get nullability from already set local vars passed in columnVars
      columnVars.map(_.isNull)
    }

    fieldVars.zip(nullLocalVars).zip(resultVars).map { case (((dataType, _,
    fieldVar, nullIdx), nullLocalVar), resultVar) =>
      if (nullIdx == -1) {
        // if incoming variable is null, then default will get assigned
        // because the variable will be initialized with the default
        genVarAssignCode(objVar, resultVar, fieldVar.value, dataType, doCopy)
      } else if (nullIdx == NULL_NON_PRIM) {
        val varName = fieldVar.value
        s"""
          if (${resultVar.isNull}) {
            $objVar.$varName = null;
          } else {
            ${genVarAssignCode(objVar, resultVar, varName, dataType, doCopy)}
          }
        """
      } else {
        val nullVar = fieldVar.isNull
        // when initializing the object, no need to clear null mask
        val nullClear = if (forInit) ""
        else {
          s"""
            if ($nullLocalVar) {
              $objVar.$nullVar &= ~${genNullBitMask(nullIdx)};
            }
          """
        }
        s"""
          if (${resultVar.isNull}) {
            $objVar.$nullVar |= ${genNullBitMask(nullIdx)};
          } else {
            $nullClear
            ${genVarAssignCode(objVar, resultVar, fieldVar.value,
                dataType, doCopy)}
          }
        """
      }
    }.mkString("\n")
  }

  private def genInnerJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], valueInit: String,
      numRows: String, consumeResult: String, keyIsUnique: String,
      declareLocalValueVars: String,
      moveNextValue: String): (String, String) = {

    val keyCodes =
      s"""if ($entryVar == null) continue;
        $mapKeyCodes"""
    val consumeCode = checkCondition match {
      case None => consumeResult
      case Some(ev) =>
        s"""${ev.code}
          if (!${ev.isNull} && ${ev.value}) {
            $consumeResult
          }"""
    }
    // loop through all the matches with moveNextValue
    val multiConsumeCode =
      s"""$keyCodes

        $declareLocalValueVars
        while (true) {
          // values will be repeatedly reassigned in the loop (if any)
          // while keys will remain the same
          $valueInit
          $consumeCode

          if ($keyIsUnique) break;

          $moveNextValue
        }"""
    (s"$keyCodes\n$consumeCode", multiConsumeCode)
  }

  private def genOuterJoinCodes(entryVar: String, buildVars: Seq[ExprCode],
      mapKeyCodes: String, checkCondition: Option[ExprCode],
      valueInit: String, numRows: String, consumeResult: String,
      keyIsUnique: String, declareLocalValueVars: String,
      moveNextValue: String): (String, String) = {

    val keyCodes =
      s"""if ($entryVar != null) {
          $mapKeyCodes
        }"""

    val consumeCode = checkCondition match {
      case None =>
        s"""if ($entryVar != null) {
            $valueInit
          } else {
            // set null variables for outer join in failed match
            ${buildVars.map(ev => s"${ev.isNull} = true;").mkString("\n")}
          }
          $consumeResult"""

      case Some(ev) =>
        // assign null to entryVar if checkCondition fails so that it is
        // treated like an empty outer join match by subsequent code
        s"""if ($entryVar != null) {
            $valueInit
            ${ev.code}
            if (${ev.isNull} || !${ev.value}) $entryVar = null;
          }
          // set null variables for outer join in failed match
          if ($entryVar == null) {
            ${buildVars.map(ev => s"${ev.isNull} = true;").mkString("\n")}
          }
          $consumeResult"""
    }
    // loop through all the matches with moveNextValue
    val multiConsumeCode =
      s"""$keyCodes

        $declareLocalValueVars
        while (true) {
          // values will be repeatedly reassigned in the loop (if any)
          // while keys will remain the same
          $consumeCode

          if ($entryVar == null || $keyIsUnique) break;

          $moveNextValue
        }"""
    (s"$keyCodes\n$consumeCode", multiConsumeCode)
  }

  private def genSemiJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], valueInit: String,
      numRows: String, consumeResult: String, keyIsUnique: String,
      declareLocalValueVars: String,
      moveNextValue: String): (String, String) = checkCondition match {

    case None =>
      // no key/value assignments required
      val keyConsumeCode = s"if ($entryVar == null) continue;\n$consumeResult"
      (keyConsumeCode, keyConsumeCode)

    case Some(ev) =>
      // need the key/value assignments for condition evaluation
      val keyCodes =
        s"""if ($entryVar == null) continue;
          $mapKeyCodes"""
      val keyConsumeCode =
        s"""$keyCodes
          ${ev.code}
          if (!${ev.isNull} && ${ev.value}) {
            $consumeResult
          }"""
      // loop through all the matches with moveNextValue
      val multiConsumeCode =
        s"""$keyCodes

          $declareLocalValueVars
          while (true) {
            // values will be repeatedly reassigned in the loop (if any)
            // while keys will remain the same
            $valueInit
            ${ev.code}
            // consume only one result
            if (!${ev.isNull} && ${ev.value}) {
              $consumeResult
              break;
            }
            if ($keyIsUnique) break;

            $moveNextValue
          }"""
      (keyConsumeCode, multiConsumeCode)
  }

  private def genAntiJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], valueInit: String,
      numRows: String, consumeResult: String, keyIsUnique: String,
      declareLocalValueVars: String,
      moveNextValue: String): (String, String) = checkCondition match {

    case None =>
      // success if no match for an anti-join (no value iteration)
      val keyConsumeCode = s"if ($entryVar != null) continue;\n$consumeResult"
      (keyConsumeCode, keyConsumeCode)

    case Some(ev) =>
      // need to check all failures for the condition outside the value
      // iteration loop, hence code layout is bit different from other joins
      val keyConsumeCode =
        s"""
          if ($entryVar != null) {
            $mapKeyCodes
            // fail if condition matches for the row
            ${ev.code}
            if (!${ev.isNull} && ${ev.value}) continue;
          }
          $consumeResult"""
      val matched = ctx.freshName("matched")
      // need the key/value assignments for condition evaluation
      val multiConsumeCode =
        s"""
          boolean $matched = false;
          if ($entryVar != null) {
            $mapKeyCodes

            $declareLocalValueVars
            while (true) {
              // values will be repeatedly reassigned in the loop
              // while keys will remain the same
              $valueInit
              // fail if condition matches for any row
              ${ev.code}
              if (!${ev.isNull} && ${ev.value}) {
                $matched = true;
                break;
              }
              if ($keyIsUnique) break;

              $moveNextValue
            }
          }
          // anti-join failure if there is any match
          if ($matched) continue;

          $consumeResult"""
      (keyConsumeCode, multiConsumeCode)
  }

  private def genExistenceJoinCodes(entryVar: String, existsVar: String,
      mapKeyCodes: String, checkCondition: Option[ExprCode],
      valueInit: String, numRows: String, consumeResult: String,
      keyIsUnique: String, declareLocalValueVars: String,
      moveNextValue: String): (String, String) = checkCondition match {

    case None =>
      // only one match needed, so no value iteration
      val keyConsumeCode =
        s"""final boolean $existsVar = ($entryVar == null);
          $consumeResult"""
      (keyConsumeCode, keyConsumeCode)

    case Some(ev) =>
      // need the key/value assignments for condition evaluation
      val keyConsumeCode =
        s"""boolean $existsVar = false;
          if ($entryVar != null) {
            $mapKeyCodes
            ${ev.code}
            $existsVar = !${ev.isNull} && ${ev.value};
          }
          $consumeResult"""
      // need the key/value assignments for condition evaluation
      val multiConsumeCode =
      s"""boolean $existsVar = false;
        if ($entryVar != null) {
          $mapKeyCodes

          $declareLocalValueVars
          while (true) {
            // values will be repeatedly reassigned in the loop (if any)
            // while keys will remain the same
            $valueInit
            ${ev.code}
            if (!${ev.isNull} && ${ev.value}) {
              // consume only one result
              $existsVar = true;
              break;
            }
            if ($keyIsUnique) break;

            $moveNextValue
          }
        }
        $consumeResult"""
      (keyConsumeCode, multiConsumeCode)
  }

  private def isIntegralType(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType |
         TimestampType | DateType => true
    case _ => false
  }

  private def isPrimitiveType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType |
         LongType | FloatType | DoubleType | TimestampType | DateType => true
    case _ => false
  }

  private def genVarAssignCode(objVar: String, resultVar: ExprCode,
      varName: String, dataType: DataType, doCopy: Boolean): String = {
    // check for single column no-wrapper case
    val colVar = if (varName.isEmpty) objVar
    else s"$objVar.$varName"
    genVarAssignCode(colVar, resultVar, dataType, doCopy)
  }

  private def genVarAssignCode(colVar: String, resultVar: ExprCode,
      dataType: DataType, doCopy: Boolean): String = dataType match {
    // if doCopy is true, then create a copy of some non-primitives that just
    // hold a reference to UnsafeRow bytes (and can change under the hood)
    case StringType if doCopy =>
      s"$colVar = ${resultVar.value}.getBytes();"
    case StringType =>
      // try to copy just reference of the byte[] if possible
      val stringVar = resultVar.value
      val platformClass = classOf[Platform].getName
      val bytes = ctx.freshName("stringBytes")
      val obj = bytes + "Obj"
      s"""Object $obj; byte[] $bytes;
        if ($stringVar.getBaseOffset() == $platformClass.BYTE_ARRAY_OFFSET
            && ($obj = $stringVar.getBaseObject()) instanceof byte[]
            && ($bytes = (byte[])$obj).length == $stringVar.numBytes()) {
          $colVar = $bytes;
        } else {
          $colVar = $stringVar.getBytes();
        }"""
    case (_: ArrayType | _: MapType | _: StructType) if doCopy =>
      s"$colVar = ${resultVar.value}.copy();"
    case _: BinaryType if doCopy =>
      s"$colVar = ${resultVar.value}.clone();"
    case _ =>
      s"$colVar = ${resultVar.value};"
  }

  private def genNullBitMask(nullIdx: Int): String =
    if (nullIdx > 0) s"(1L << $nullIdx)" else "1L"

  private def genNullCode(colVar: String, nullIndex: Int): String = {
    if (nullIndex > 0) {
      s"(($colVar & (1L << $nullIndex)) != 0L)"
    } else if (nullIndex == 0) {
      s"(($colVar & 1L) == 1L)"
    } else if (nullIndex == NULL_NON_PRIM) {
      s"($colVar == null)"
    } else "false"
  }

  private def genNotNullCode(colVar: String, nullIndex: Int): String = {
    if (nullIndex > 0) {
      s"(($colVar & (1L << $nullIndex)) == 0L)"
    } else if (nullIndex == 0) {
      s"(($colVar & 1L) == 0L)"
    } else if (nullIndex == NULL_NON_PRIM) {
      s"($colVar != null)"
    } else "true"
  }

  private def hashSingleInt(colVar: String, nullVar: String,
      hashVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") {
      s"$hashVar = $hashingClass.hashInt($colVar);\n"
    } else {
      s"$hashVar = ($nullVar) ? -1 : $hashingClass.hashInt($colVar);\n"
    }
  }

  private def hashCodeSingleInt(hashExpr: String, nullVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") hashExpr
    else s"($nullVar) ? -1 : $hashExpr"
  }

  private def hashSingleLong(colVar: String, nullVar: String,
      hashVar: String): String = {
    val longVar = ctx.freshName("longVar")
    if (nullVar.isEmpty || nullVar == "false") {
      s"""
        final long $longVar = $colVar;
        $hashVar = $hashingClass.hashInt(
          (int)($longVar ^ ($longVar >>> 32)));
      """
    } else {
      s"""
        final long $longVar;
        $hashVar = ($nullVar) ? -1 : $hashingClass.hashInt(
          (int)(($longVar = ($colVar)) ^ ($longVar >>> 32)));
      """
    }
  }

  private def addHashInt(hashExpr: String, nullVar: String,
      hashVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") {
      s"""
        $hashVar = ($hashVar ^ 0x9e3779b9) + ($hashExpr) +
            ($hashVar << 6) + ($hashVar >>> 2);
      """
    } else {
      s"""
        $hashVar = ($hashVar ^ 0x9e3779b9) + (($nullVar) ? -1 : ($hashExpr)) +
            ($hashVar << 6) + ($hashVar >>> 2);
      """
    }
  }

  private def addHashLong(hashExpr: String, nullVar: String,
      hashVar: String): String = {
    val longVar = ctx.freshName("longVar")
    if (nullVar.isEmpty || nullVar == "false") {
      s"""
        final long $longVar = $hashExpr;
        $hashVar = ($hashVar ^ 0x9e3779b9) + (int)($longVar ^ ($longVar >>> 32)) +
            ($hashVar << 6) + ($hashVar >>> 2);
      """
    } else {
      s"""
        final long $longVar;
        $hashVar = ($hashVar ^ 0x9e3779b9) + (($nullVar) ? -1
            : (int)(($longVar = ($hashExpr)) ^ ($longVar >>> 32))) +
            ($hashVar << 6) + ($hashVar >>> 2);
      """
    }
  }

  private def genEqualsCode(
      thisVar: String, thisColVar: String, thisNullVar: String,
      otherVar: String, otherColVar: String, otherNullVar: String,
      nullIndex: Int, isPrimitive: Boolean, dataType: DataType): String = {
    // check for single column no-wrapper case
    val otherCol = if (otherColVar.isEmpty) otherVar
    else s"$otherVar.$otherColVar"
    val otherColNull = if (otherColVar.isEmpty) otherNullVar
    else s"$otherVar.$otherNullVar"
    val equalsCode = if (isPrimitive) s"($thisColVar == $otherCol)"
    else dataType match {
      // strings are stored as raw byte arrays
      case StringType =>
        val byteMethodsClass = classOf[ByteArrayMethods].getName
        val platformClass = classOf[Platform].getName
        if (thisVar.isEmpty) {
          // left side is a UTF8String while right side is byte array
          s"""$thisColVar.numBytes() == $otherCol.length
            && $byteMethodsClass.arrayEquals($thisColVar.getBaseObject(),
               $thisColVar.getBaseOffset(), $otherCol,
               $platformClass.BYTE_ARRAY_OFFSET, $otherCol.length)"""
        } else {
          // both sides are raw byte arrays
          s"""$thisColVar.length == $otherCol.length
            && $byteMethodsClass.arrayEquals($thisColVar,
               $platformClass.BYTE_ARRAY_OFFSET, $otherCol,
               $platformClass.BYTE_ARRAY_OFFSET, $otherCol.length)"""
        }
      case _ => s"$thisColVar.equals($otherCol)"
    }
    if (nullIndex == -1) equalsCode
    else if (nullIndex == NULL_NON_PRIM) {
      s"""($thisColVar != null ? ($otherCol != null && $equalsCode)
           : ($otherCol) == null)"""
    } else {
      val notNullCode = if (thisVar.isEmpty) s"!$thisNullVar"
      else genNotNullCode(thisNullVar, nullIndex)
      val otherNotNullCode = genNotNullCode(otherColNull, nullIndex)
      s"""($notNullCode ? ($otherNotNullCode && $equalsCode)
           : !$otherNotNullCode)"""
    }
  }
}
