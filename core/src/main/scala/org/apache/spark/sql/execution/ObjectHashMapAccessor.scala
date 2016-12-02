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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression,
NamedExpression}
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
final case class ObjectHashMapAccessor(@transient session: SnappySession,
    @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
    @transient valueExprs: Seq[Expression], classPrefix: String,
    hashMapTerm: String, dataTerm: String, maskTerm: String,
    multiMap: Boolean, @transient consumer: CodegenSupport,
    @transient cParent: CodegenSupport, override val child: SparkPlan)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  private[this] val keyExpressions = keyExprs.map(_.canonicalized)
  private[this] val valueExpressions = valueExprs.map(_.canonicalized)

  private[this] val valueIndex = keyExpressions.length

  // Key expressions are the key fields in the map, while value expressions
  // are the output values expected to be read from the map which may include
  // some or all or none of the keys. The maps used below are to find the common
  // expressions for value fields and re-use key variables if possible.
  // End output is Seq of (expression, index) where index will be negative if
  // found in the key list i.e. value expression index will be negative of that
  // in the key map if found there else its own running index for unique ones.
  @transient private[this] val (keyExprIndexes, valueExprIndexes) = {
    val keyExprIndexMap = keyExpressions.zipWithIndex.toMap
    var index = -1
    val valueExprIndexes = valueExpressions.map(e =>
      e -> keyExprIndexMap.get(e).map(-_ - 1).getOrElse {
        index += 1;
        index
      })
    (keyExprIndexMap.toSeq, valueExprIndexes)
  }

  @transient lazy val (integralKeys, integralKeysMinVars, integralKeysMaxVars) =
    keyExprIndexes.collect {
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
  private[this] val lastKeyIndexVar = "lastKeyIndex"
  private[this] val multiValuesVar = "multiValues"

  private type ClassVar = (DataType, String, ExprCode, Int)

  @transient private[this] val (className, valueClassName, classVars,
  numNullVars) = initClass()

  private def initClass(): (String, String, IndexedSeq[ClassVar], Int) = {

    // Key columns will be first in the class.
    // Eliminate common expressions and re-use variables.
    // For multi-map case, the full entry class will extend value class
    // so common expressions will be in the entry key fields which will
    // be same for all values when the multi-value array is filled.
    val keyTypes = keyExpressions.map(e => e.dataType -> e.nullable)
    val valueTypes = valueExprIndexes.collect {
      case (e, i) if i >= 0 => e.dataType -> e.nullable
    }
    val entryTypes = if (multiMap) keyTypes else keyTypes ++ valueTypes
    // whether a separate value class is required or not
    val valClassTypes = if (multiMap) valueTypes else Nil
    // check for existing class with same schema
    val (valueClass, entryClass, exists) = session.getClass(ctx,
      valClassTypes, entryTypes) match {
      case Some((v, e)) => (v, e, true)
      case None =>
        val entryClass = ctx.freshName(classPrefix)
        val valClass = if (valClassTypes.nonEmpty) "Val_" + entryClass else ""
        (valClass, entryClass, false)
    }

    // local variable name for other object in equals
    val other = "other"

    // For the case of multi-map, key fields cannot be null. Create a
    // separate value class that the main class will extend. The main class
    // object will have value class objects as an array (possibly null) that
    // holds any additional values beyond the one set already inherited.
    val (entryVars, valClassVars, numNulls, nullDecls) = createClassVars(
      entryTypes, valClassTypes)
    if (!exists) {
      // Generate equals code for key columns only.
      val keyVars = entryVars.take(valueIndex)
      val equalsCode = keyVars.map {
        case (dataType, _, ExprCode(_, nullVar, varName), nullIndex) =>
          genEqualsCode("this", varName, nullVar, other,
            varName, nullVar, nullIndex, isPrimitiveType(dataType), dataType)
      }.mkString(" &&\n")
      val (valueClassCode, extendsCode, nulls, multiValues) =
        if (valClassVars.nonEmpty) {
          (
              s"""
            public static class $valueClass {
              $nullDecls
              ${valClassVars.map(e => s"${e._2} ${e._3.value};").mkString("\n")}
            }
          """, s" extends $valueClass", "",
              s"$valueClass[] $multiValuesVar;")
        } else if (multiMap) {
          ("", "", nullDecls, s"int $lastKeyIndexVar;")
        } else ("", "", nullDecls, "")
      val classCode =
        s"""
          public static final class $entryClass$extendsCode {
            $nulls
            ${entryVars.map(e => s"${e._2} ${e._3.value};").mkString("\n")}
            $multiValues
            final int hash;

            public $entryClass(int h) {
              this.hash = h;
            }

            public int hashCode() {
              return this.hash;
            }

            public boolean equals(Object o) {
              final $entryClass $other = ($entryClass)o;
              return $equalsCode;
            }
          };
        """
      // using addNewFunction to register the class since there is nothing
      // function specific in the addNewFunction method
      if (!valueClassCode.isEmpty) {
        ctx.addNewFunction(valueClass, valueClassCode)
      }
      ctx.addNewFunction(entryClass, classCode)
      session.addClass(ctx, valClassTypes, entryTypes, valueClass, entryClass)
    }

    (entryClass, valueClass, entryVars ++ valClassVars, numNulls)
  }

  private def createClassVars(entryTypes: Seq[(DataType, Boolean)],
      valClassTypes: Seq[(DataType, Boolean)]): (IndexedSeq[ClassVar],
      IndexedSeq[ClassVar], Int, String) = {
    // collect the null field declarations (will be in baseClass if present)
    val nullMaskDeclarations = new StringBuilder

    var numNulls = -1
    var currNullVar = ""

    val numEntryVars = entryTypes.length
    val entryVars = new mutable.ArrayBuffer[ClassVar](numEntryVars)
    val valClassVars = new mutable.ArrayBuffer[ClassVar](valClassTypes.length)
    val allTypes = entryTypes ++ valClassTypes
    allTypes.indices.foreach { index =>
      val p = allTypes(index)
      val varName = s"field$index"
      val dataType = p._1
      val nullable = p._2
      val javaType = dataType match {
        // use raw byte arrays for strings to minimize overhead
        case StringType => "byte[]"
        case _ => ctx.javaType(dataType)
      }
      val (nullVar, nullIndex) = if (nullable) {
        if (isPrimitiveType(dataType)) {
          // nullability will be stored in separate long bitmask fields
          numNulls += 1
          // each long can hold bit mask for 64 nulls
          val nullIndex = numNulls % 64
          if (nullIndex == 0) {
            currNullVar = s"$nullsMaskPrefix${numNulls / 64}"
            nullMaskDeclarations.append(s"long $currNullVar;\n")
          }
          (currNullVar, nullIndex)
        } else ("", NULL_NON_PRIM) // field itself is nullable
      } else ("", -1)
      if (index < numEntryVars) {
        entryVars += ((dataType, javaType, ExprCode("", nullVar, varName),
            nullIndex))
      } else {
        valClassVars += ((dataType, javaType, ExprCode("", nullVar, varName),
            nullIndex))
      }
    }
    val numNullVars = if (numNulls >= 0) (numNulls / 64) + 1 else 0
    (entryVars, valClassVars, numNullVars, nullMaskDeclarations.toString())
  }

  private def getExpressionVars(expressions: Seq[Expression],
      input: Seq[ExprCode]): Seq[ExprCode] = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val vars = ctx.generateExpressions(expressions.map(e =>
      BindReferences.bindReference[Expression](e, child.output)))
    ctx.currentVars = null
    vars
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("unexpected invocation")

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq.empty

  override protected def doProduce(ctx: CodegenContext): String =
    throw new UnsupportedOperationException("unexpected invocation")

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    // consume the data and populate the map
    val entryVar = "mapEntry"
    // local variable
    val hashVar = ctx.freshName("hash")
    val posVar = ctx.freshName("pos")
    val deltaVar = ctx.freshName("delta")
    val keyVars = getExpressionVars(keyExpressions, input)
    // skip expressions already in key variables (that are also skipped
    //   in the value class fields in class generation)
    val valueVars = getExpressionVars(
      valueExprIndexes.filter(_._2 >= 0).map(_._1), input)
    // Update min/max code for primitive type columns. Avoiding additional
    // index mapping here for mix of integral and non-integral keys
    // rather using key index since overhead of blanks will be negligible.
    val updateMinMax = integralKeys.map { index =>
      s"$hashMapTerm.updateLimits(${keyVars(index).value}, $index);"
    }.mkString("\n")
    val multiValuesUpdateCode = if (valueClassName.isEmpty) {
      s"""
        // no value field, only count
        final int lastKeyIndex = $entryVar.$lastKeyIndexVar;
        $entryVar.$lastKeyIndexVar = lastKeyIndex + 1;
        // mark map as not unique on second insert for same key
        if (lastKeyIndex == 0) $hashMapTerm.setKeyIsUnique(false);"""
    } else {
      s"""
        // add to multiValues array
        final int valueIndex;
        $valueClassName[] values = $entryVar.$multiValuesVar;
        if (values != null) {
          valueIndex = values.length;
          $valueClassName[] newValues = new $valueClassName[valueIndex + 1];
          System.arraycopy(values, 0, newValues, 0, valueIndex);
          values = newValues;
        } else {
          valueIndex = 0;
          values = new $valueClassName[1];
        }

        final $valueClassName newValue = new $valueClassName();
        values[valueIndex] = newValue;
        $entryVar.$multiValuesVar = values;
        ${generateUpdate("newValue", Nil, valueVars, forKey = false)}

        // mark map as not unique on second insert for same key
        if (valueIndex == 0) $hashMapTerm.setKeyIsUnique(false);"""
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

  // TODO: generate local variables for key fields for equals call and use
  // those variables for any consumers later
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
   * in generated code first. The last element in the result tuple is the names
   * of null mask variables.
   */
  def getColumnVars(keyObjVar: String, localValObjVar: String,
      onlyKeyVars: Boolean, onlyValueVars: Boolean,
      checkNullObj: Boolean = false): (String, Seq[ExprCode], Array[String]) = {
    // no local value if no separate value class
    val valObjVar = if (valueClassName.isEmpty) keyObjVar else localValObjVar
    // Generate initial declarations for null masks to avoid reading those
    // repeatedly. Caller is supposed to insert the code at the start.
    val declarations = new StringBuilder
    val nullValMaskVars = new Array[String](numNullVars)
    val nullMaskVarMap = (0 until numNullVars).map { index =>
      val nullVar = s"$nullsMaskPrefix$index"
      // separate final variable for nulls mask of key columns because
      // value can change for multi-map case whose nullsMask may not
      // be properly set for key fields
      val nullMaskVar = ctx.freshName("localKeyNullsMask")
      val nullValMaskVar = ctx.freshName("localNullsMask")
      if (checkNullObj) {
        // for outer joins, check for null entry and set all bits to 1
        declarations.append(s"final long $nullMaskVar = " +
            s"$keyObjVar != null ? $keyObjVar.$nullVar : -1L;\n")
      } else {
        declarations.append(s"final long $nullMaskVar = $keyObjVar.$nullVar;")
      }
      declarations.append(s"long $nullValMaskVar = $nullMaskVar;")
      nullValMaskVars(index) = nullValMaskVar
      nullVar -> (nullMaskVar, nullValMaskVar)
    }.toMap

    val vars = if (onlyKeyVars) classVars.take(valueIndex)
    else {
      // for value variables that are part of common expressions with keys,
      // indicate the same as a null ExprCode with "nullIndex" pointing to
      // the index of actual key variable to use in classVars
      val valueVars = valueExprIndexes.collect {
        case (e, i) if i >= 0 => classVars(i + valueIndex)
        case (e, i) => (null, null, null, -i - 1) // i < 0
      }
      if (onlyValueVars) valueVars else classVars.take(valueIndex) ++ valueVars
    }

    // lookup common expressions in key for values if accumulated by
    // back to back calls to getColumnVars for keys, then columns
    val columnVars = new mutable.ArrayBuffer[ExprCode]
    vars.indices.foreach { index =>
      val (dataType, javaType, ev, nullIndex) = vars(index)
      val isKeyVar = index < valueIndex
      val objVar = if (isKeyVar) keyObjVar else valObjVar
      ev match {
        // nullIndex contains index of referenced key variable in this case
        case null if !onlyValueVars => columnVars += columnVars(nullIndex)
        case _ =>
          val (localVar, localDeclaration) = {
            dataType match {
              case StringType =>
                // wrap the bytes in UTF8String
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(s"final UTF8String $lv = ").append(
                  if (checkNullObj) {
                    s" ( $objVar != null ? UTF8String.fromBytes($objVar.${ev.value}) " +
                        s" : null ) ; "
                  }
                  else {
                    s"UTF8String.fromBytes($objVar.${ev.value});"
                  }))
              case _ =>
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(s"final $javaType $lv = ").append(
                  if (checkNullObj) {
                    s" ( $objVar != null ? $objVar.${ev.value} " +
                        s" : ${ctx.defaultValue(dataType)} ) ; "
                  }
                  else {
                    s"$objVar.${ev.value};"
                  }))
            }
          }
          val nullExpr = nullMaskVarMap.get(ev.isNull)
              .map(p => if (isKeyVar) genNullCode(p._1, nullIndex)
              else genNullCode(p._2, nullIndex)).getOrElse(
            if (nullIndex == NULL_NON_PRIM) s"($localVar == null)"
            else "false")
          val nullVar = ctx.freshName("isNull")
          localDeclaration.append(s"\nboolean $nullVar = $nullExpr;")
          columnVars += ExprCode(localDeclaration.toString, nullVar, localVar)
      }
    }
    (declarations.toString(), columnVars, nullValMaskVars)
  }

  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(objVar: String, valueInitVars: Seq[ExprCode],
      valueInitCode: String, input: Seq[ExprCode]): String = {
    val hashVar = ctx.freshName("hash")
    val posVar = ctx.freshName("pos")
    val deltaVar = ctx.freshName("delta")
    val keyVars = getExpressionVars(keyExpressions, input)
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

  private def getConsumeResultCode(numRows: String,
      resultVars: Seq[ExprCode]): String =
    s"$numRows++;\n${consumer.consume(ctx, resultVars)}"

  // scalastyle:off
  def generateMapLookup(entryVar: String, localValueVar: String,
      keyIsUnique: String, numRows: String, nullMaskVars: Array[String],
      initCode: String, checkCondition: Option[ExprCode],
      streamKeys: Seq[Expression], streamKeyVars: Seq[ExprCode],
      buildKeyVars: Seq[ExprCode], buildVars: Seq[ExprCode], input: Seq[ExprCode],
      resultVars: Seq[ExprCode], joinType: JoinType): String = {
    // scalastyle:on

    val hashVar = ctx.freshName("hash")
    // these are all local variables inside private block, so no ctx.freshName
    val posVar = "pos"
    val deltaVar = "delta"

    // if consumer is a projection that will project away key columns,
    // then avoid materializing those
    // TODO: SW: should be removed once keyVars are handled in generateEquals
    // Also remove mapKeyCodes in join types.
    val mapKeyVars = cParent match {
      case ProjectExec(projection, _) =>
        buildKeyVars.zip(keyExpressions).collect {
          case (ev, ne: NamedExpression)
            if projection.exists(_.exprId == ne.exprId) => ev
          case (ev, expr)
            if projection.exists(_.semanticEquals(expr)) => ev
        }
      case _ => buildKeyVars
    }
    val mapKeyCodes = s"$initCode\n${evaluateVariables(mapKeyVars)}"

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
    val entryIndexVar = ctx.freshName("entryIndex")
    val numEntriesVar = ctx.freshName("numEntries")
    val valuesVar = ctx.freshName("values")
    val declareLocalVars = if (valueClassName.isEmpty) {
      // one consume done when moveNextValue is hit, so initialize with 1
      s"""
        // key count iteration
        int $entryIndexVar = 0;
        int $numEntriesVar = $entryVar.$lastKeyIndexVar + 1;
      """
    } else {
      s"""
        int $entryIndexVar = -2;
        int $numEntriesVar = -2;
        $valueClassName[] $valuesVar = null;
        // for first iteration, entry object itself has value fields
        $valueClassName $localValueVar = $entryVar;"""
    }
    val moveNextValue = if (valueClassName.isEmpty) {
      s"""
        if ($entryIndexVar < $numEntriesVar) {
          $entryIndexVar++;
        } else {
          break;
        }"""
    } else {
      // code to update the null masks
      val nullsUpdate = (0 until numNullVars).map { index =>
        val nullVar = s"$nullsMaskPrefix$index"
        s"${nullMaskVars(index)} = $localValueVar.$nullVar;"
      }.mkString("\n")
      s"""
        if ($entryIndexVar == -2) {
          // first iteration where entry is value
          $entryIndexVar = -1;
        } else if ($entryIndexVar < $numEntriesVar) {
          $localValueVar = $valuesVar[$entryIndexVar++];
        } else if ($entryIndexVar == -1) {
          // multi-values array hit first time
          if (($valuesVar = $entryVar.$multiValuesVar) != null) {
            $entryIndexVar = 1;
            $numEntriesVar = $valuesVar.length;
            $localValueVar = $valuesVar[0];
            $nullsUpdate
          } else {
            break;
          }
        } else {
          break;
        }"""
    }
    // Code fragments for different join types.
    // This is to ensure only a single parent.consume() because the branches
    // can be taken alternately in the worst case so then it can lead to
    // large increase in instruction cache misses even though most of the code
    // will be the common parent's consume call.
    val entryConsume = joinType match {
      case Inner => genInnerJoinCodes(entryVar, mapKeyCodes, checkCondition,
        numRows, getConsumeResultCode(numRows, resultVars),
        keyIsUnique, declareLocalVars, moveNextValue)

      case LeftOuter | RightOuter =>
        // instantiate code for buildVars before calling getConsumeResultCode
        val buildInitCode = evaluateVariables(buildVars)
        genOuterJoinCodes(entryVar, buildVars, buildInitCode, mapKeyCodes,
          checkCondition, numRows, getConsumeResultCode(numRows, resultVars),
          keyIsUnique, declareLocalVars, moveNextValue)

      case LeftSemi => genSemiJoinCodes(entryVar, mapKeyCodes, checkCondition,
        numRows, getConsumeResultCode(numRows, input),
        keyIsUnique, declareLocalVars, moveNextValue)

      case LeftAnti => genAntiJoinCodes(entryVar, mapKeyCodes, checkCondition,
        numRows, getConsumeResultCode(numRows, input),
        keyIsUnique, declareLocalVars, moveNextValue)

      case _: ExistenceJoin =>
        // declare and add the exists variable to resultVars
        val existsVar = ctx.freshName("exists")
        genExistenceJoinCodes(entryVar, existsVar, mapKeyCodes,
          checkCondition, numRows, getConsumeResultCode(numRows,
            input :+ ExprCode("", "false", existsVar)), keyIsUnique,
          declareLocalVars, moveNextValue)

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
      $entryConsume
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
            $objVar.$nullVar|= ${genNullBitMask(nullIdx)};
          } else {
            $nullClear
            ${
          genVarAssignCode(objVar, resultVar, fieldVar.value,
            dataType, doCopy)
        }
          }
        """
      }
    }.mkString("\n")
  }

  private def genInnerJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], numRows: String,
      consumeResult: String, keyIsUnique: String,
      declareLocalVars: String, moveNextValue: String): String = {

    val consumeCode = checkCondition match {
      case None => consumeResult
      case Some(ev) =>
        s"""${ev.code}
          if (!${ev.isNull} && ${ev.value}) {
            $consumeResult
          }"""
    }
    // loop through all the matches with moveNextValue
    s"""if ($entryVar == null) continue;

      $declareLocalVars

      $mapKeyCodes
      while (true) {
        // values will be repeatedly reassigned in the loop (if any)
        // while keys will remain the same
        $moveNextValue

        $consumeCode

        if ($keyIsUnique) break;
      }"""
  }

  private def genOuterJoinCodes(entryVar: String, buildVars: Seq[ExprCode],
      buildInitCode: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], numRows: String,
      consumeResult: String, keyIsUnique: String, declareLocalVars: String,
      moveNextValue: String): String = {

    val consumeCode = checkCondition match {
      case None =>
        s"""$buildInitCode
          if ($entryVar == null) {
            // set null variables for outer join in failed match
            ${buildVars.map(ev => s"${ev.isNull} = true;").mkString("\n")}
          }
          $consumeResult"""

      case Some(ev) =>
        // assign null to entryVar if checkCondition fails so that it is
        // treated like an empty outer join match by subsequent code
        s"""if ($entryVar != null) {
            ${ev.code}
            if (${ev.isNull}|| !${ev.value}) $entryVar = null;
          }
          $buildInitCode
          if ($entryVar == null) {
            // set null variables for outer join in failed match
            ${buildVars.map(ev => s"${ev.isNull} = true;").mkString("\n")}
          }
          $consumeResult"""
    }
    // loop through all the matches with moveNextValue
    // null check for entryVar is already done inside mapKeyCodes
    s"""$declareLocalVars

      $mapKeyCodes
      while (true) {
        // values will be repeatedly reassigned in the loop (if any)
        // while keys will remain the same
        $moveNextValue

        $consumeCode

        if ($entryVar == null || $keyIsUnique) break;
      }"""
  }

  private def genSemiJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], numRows: String,
      consumeResult: String, keyIsUnique: String, declareLocalVars: String,
      moveNextValue: String): String = checkCondition match {

    case None =>
      // no key/value assignments required
      s"if ($entryVar == null) continue;\n$consumeResult"

    case Some(ev) =>
      // need the key/value assignments for condition evaluation
      // loop through all the matches with moveNextValue
      s"""if ($entryVar == null) continue;

        $declareLocalVars

        $mapKeyCodes
        while (true) {
          // values will be repeatedly reassigned in the loop (if any)
          // while keys will remain the same
          $moveNextValue

          ${ev.code}
          // consume only one result
          if (!${ev.isNull} && ${ev.value}) {
            $consumeResult
            break;
          }
          if ($keyIsUnique) break;
        }"""
  }

  private def genAntiJoinCodes(entryVar: String, mapKeyCodes: String,
      checkCondition: Option[ExprCode], numRows: String,
      consumeResult: String, keyIsUnique: String, declareLocalVars: String,
      moveNextValue: String): String = checkCondition match {

    case None =>
      // success if no match for an anti-join (no value iteration)
      s"if ($entryVar != null) continue;\n$consumeResult"

    case Some(ev) =>
      // need to check all failures for the condition outside the value
      // iteration loop, hence code layout is bit different from other joins
      val matched = ctx.freshName("matched")
      // need the key/value assignments for condition evaluation
      s"""
        boolean $matched = false;
        if ($entryVar != null) {
          $declareLocalVars

          $mapKeyCodes
          while (true) {
            // values will be repeatedly reassigned in the loop
            // while keys will remain the same
            $moveNextValue

            // fail if condition matches for any row
            ${ev.code}
            if (!${ev.isNull} && ${ev.value}) {
              $matched = true;
              break;
            }
            if ($keyIsUnique) break;
          }
        }
        // anti-join failure if there is any match
        if ($matched) continue;

        $consumeResult"""
  }

  private def genExistenceJoinCodes(entryVar: String, existsVar: String,
      mapKeyCodes: String, checkCondition: Option[ExprCode], numRows: String,
      consumeResult: String, keyIsUnique: String, declareLocalVars: String,
      moveNextValue: String): String = checkCondition match {

    case None =>
      // only one match needed, so no value iteration
      s"""final boolean $existsVar = ($entryVar == null);
        $consumeResult"""

    case Some(ev) =>
      // need the key/value assignments for condition evaluation
      s"""boolean $existsVar = false;
        if ($entryVar != null) {
          $declareLocalVars

          $mapKeyCodes
          while (true) {
            // values will be repeatedly reassigned in the loop (if any)
            // while keys will remain the same
            $moveNextValue

            ${ev.code}
            if (!${ev.isNull} && ${ev.value}) {
              // consume only one result
              $existsVar = true;
              break;
            }
            if ($keyIsUnique) break;
          }
        }
        $consumeResult"""
  }

  /**
   * Max, min for only integer types is supported. For non-primitives, the cost
   * of comparison will be significant so better to do a hash lookup.
   * Approximate types like float/double are not useful as hash key columns
   * and will be very rare, if at all, so ignored.
   */
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
    // check for object field or local variable
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
    // check for object field or local variable
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
    if (nullIndex == -1 || thisNullVar.isEmpty || thisNullVar == "false") {
      equalsCode
    } else if (nullIndex == NULL_NON_PRIM) {
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
