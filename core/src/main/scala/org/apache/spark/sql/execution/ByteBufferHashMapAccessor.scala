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
package org.apache.spark.sql.execution

import java.nio.ByteBuffer

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.{BufferAllocator, ClientResolverUtils}
import io.snappydata.collection.ObjectHashSet
import scala.reflect.runtime.universe._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.columnar.encoding.StringDictionary
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoinExec}
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods

case class ByteBufferHashMapAccessor(@transient session: SnappySession,
  @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
  @transient valueExprs: Seq[Expression], classPrefix: String,
  hashMapTerm: String, dataTerm: String, @transient consumer: CodegenSupport,
  @transient cParent: CodegenSupport, override val child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  private[this] val hashingClass = classOf[ClientResolverUtils].getName
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
        declarations.append(s"final long $nullMaskVar = $keyObjVar.$nullVar;\n")
      }
      declarations.append(s"long $nullValMaskVar = $nullMaskVar;\n")
      nullValMaskVars(index) = nullValMaskVar
      nullVar -> (nullMaskVar, nullValMaskVar)
    }.toMap

    val vars = if (onlyKeyVars) classVars.take(valueIndex)
    else {
      // for value variables that are part of common expressions with keys,
      // indicate the same as a null ExprCode with "nullIndex" pointing to
      // the index of actual key variable to use in classVars
      val valueVars = valueExprIndexes.collect {
        case (_, i) if i >= 0 => classVars(i + valueIndex)
        case (_, i) => (null, null, null, -i - 1) // i < 0
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
              case StringType if !multiMap =>
                // wrap the bytes in UTF8String
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(s"final UTF8String $lv = ").append(
                  if (checkNullObj) {
                    s"($objVar != null ? UTF8String.fromBytes(" +
                      s"$objVar.${ev.value}) : null);"
                  } else {
                    s"UTF8String.fromBytes($objVar.${ev.value});"
                  }))
              case _ =>
                val lv = ctx.freshName("localField")
                (lv, new StringBuilder().append(s"final $javaType $lv = ").append(
                  if (checkNullObj) {
                    s"($objVar != null ? $objVar.${ev.value} " +
                      s" : ${ctx.defaultValue(dataType)});"
                  } else {
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
  def generateMapGetOrInsert(aggregateBufferVars: Seq[String], valueInitVars: Seq[ExprCode],
    valueInitCode: String, input: Seq[ExprCode], keyVars: Seq[ExprCode],
    keysDataType: Seq[DataType], aggregateDataTypes: Seq[DataType]): String = {
    val hashVar = Array(ctx.freshName("hash"))
    val numKeyBytes = (ctx.freshName("numKeyBytes"))
    val keyBytesHolder = (ctx.freshName("keyBytesHolder"))
    val numValueBytes = (ctx.freshName("numValueBytes"))

    val valueInit = valueInitCode + '\n'
    val numAggBytes = aggregateDataTypes.foldLeft(0)( _ + _.defaultSize)
    /* generateUpdate(objVar, Nil,
      valueInitVars, forKey = false, doCopy = false) */


        val inputEvals = evaluateVariables(input)

        s"""
          // evaluate the key expressions
          $inputEvals
          ${evaluateVariables(keyVars)}
          // evaluate hash code of the lookup key
          ${generateHashCode(hashVar, keyVars, this.keyExprs, keysDataType)}
          ${generateKeySizeCode(keyVars, keysDataType, numKeyBytes)}
          int ${numValueBytes} = ${numAggBytes};

          $className $objVar;
          ${mapLookupCode(keyVars)}
         """

  }

  def generateKeyBytesHolderCode(numKeyBytesVar: String, numValueBytesVar: String, keyBytesHolderVar: String,
    keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType]): String = {
    val allocator = ctx.freshName("allocator")
    val byteBufferClass = classOf[ByteBuffer].getName
    val gfeCacheImplClass = classOf[GemFireCacheImpl].getName
    val allocatorClass = classOf[BufferAllocator].getName
    val plaformClass = classOf[Platform].getName
    val offset = ctx.freshName("offset")
    val baseObject = ctx.freshName("baseObject")
    val writer = ByteBufferHashMapAccessor.getPartialFunctionForWriting(baseObject, offset)
    s"""
        ${allocatorClass} ${allocator} = ${gfeCacheImplClass}.
        getCurrentBufferAllocator();

        ${byteBufferClass} ${keyBytesHolderVar} =  ${allocator}.
        allocate($numKeyBytesVar + $numValueBytesVar, "SHA");
        Object $baseObject = $allocator.baseObject($keyBytesHolderVar)
        long $offset = $allocator.baseOffset($keyBytesHolderVar);
        ${keysDataType.zip(keyVars.map(_.value)).foldLeft(""){
      case (code, (dt, variable)) => {
        var codex = code
        codex = codex  + (dt match {
          case x: AtomicType => typeOf(x.tag) match {
            case t if t =:= typeOf[Byte] => s"\n ${plaformClass}.putByte($baseObject," +
              s" $offset, $variable); " +
              s"\n $offset += 1; \n"
            case t if t =:= typeOf[Short] => s"\n ${Platform}.putShort($baseObject," +
              s"$offset, $variable); " +
              s"\n $offset += 2; \n"
            case t if t =:= typeOf[Int] => s"\n ${Platform}.putInt($baseObject, " +
              s"$offset, $variable); " +
              s"\n $offset += 4; \n"
            case t if t =:= typeOf[Long] => s"\n ${Platform}.putLong($baseObject," +
              s" $offset, $variable); " +
              s"\n $offset += 8; \n"
            case t if t =:= typeOf[Float] => s"\n ${Platform}.putFloat($baseObject," +
              s"$offset, $variable); " +
              s"\n $offset += 4; \n"
            case t if t =:= typeOf[Double] => s"\n ${Platform}.putDouble(" +
              s"$baseObject, $offset, $variable); " +
              s"\n $offset += 8; \n"
            case t if t =:= typeOf[Decimal] =>
              throw new UnsupportedOperationException("implement decimal")
            case _ => throw new UnsupportedOperationException("unknown type" + dt)
          }
          case StringType =>  s"\n ${Platform}.putInt($baseObject, $offset, " +
            s"$variable.numBytes());" +
            s"\n $offset += 4; \n   $variable.writeToMemory($baseObject, $offset);" +
            s"\n $offset += $variable.numBytes(); \n"
        })
        codex
    }}}
       // now add values
       ${aggregatesDataType.foldLeft("")((code, dt) => {
      var codex = code
      codex = codex  + (dt match {
        case x: AtomicType => typeOf(x.tag) match {
          case t if t =:= typeOf[Byte] => s"\n ${plaformClass}.putByte($baseObject," +
            s" $offset, 0); " +
            s"\n $offset += 1; \n"
          case t if t =:= typeOf[Short] => s"\n ${Platform}.putShort($baseObject," +
            s"$offset, 0); " +
            s"\n $offset += 2; \n"
          case t if t =:= typeOf[Int] => s"\n ${Platform}.putInt($baseObject, " +
            s"$offset, 0); " +
            s"\n $offset += 4; \n"
          case t if t =:= typeOf[Long] => s"\n ${Platform}.putLong($baseObject," +
            s" $offset, 0l); " +
            s"\n $offset += 8; \n"
          case t if t =:= typeOf[Float] => s"\n ${Platform}.putFloat($baseObject," +
            s"$offset, 0f); " +
            s"\n $offset += 4; \n"
          case t if t =:= typeOf[Double] => s"\n ${Platform}.putDouble(" +
            s"$baseObject, $offset, 0); " +
            s"\n $offset += 8; \n"
          case t if t =:= typeOf[Decimal] =>
            throw new UnsupportedOperationException("implement decimal")
          case _ => throw new UnsupportedOperationException("unknown type" + dt)
        }
    })
    codex
    })}

         """
  }

  def generateKeySizeCode(keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    numKeyBytesVar: String): String = {

    s"""
         ${keysDataType.zipWithIndex.foldLeft("${numKeyBytesVar} = " ){case (expr, (dt, i)) => {
      var rs = expr
      if (i > 0) {
        rs = rs + " + "
      }
      if (TypeUtilities.isFixedWidth(dt)) {
        rs = rs + dt.defaultSize.toString
      } else {
        rs = rs + s"${keyVars(i).value}.numBytes() + 4"
      }
      rs
    }


    }}

         """
  }
  /**
   * Generate code to calculate the hash code for given column variables that
   * correspond to the key columns in this class.
   */
  def generateHashCode(hashVar: Array[String], keyVars: Seq[ExprCode],
    keyExpressions: Seq[Expression], keysDataType: Seq[DataType], skipDeclaration: Boolean = false,
    register: Boolean = true): String = {
    var hash = hashVar(0)
    val hashDeclaration = if (skipDeclaration) "" else s"int $hash;\n"
    // check if hash has already been generated for keyExpressions
    var doRegister = register
    val vars = keyVars.map(_.value)
    val (prefix, suffix) = session.getHashVar(ctx, vars) match {
      case Some(h) =>
        hashVar(0) = h
        hash = h
        doRegister = false
        (s"if ($hash == 0) {\n", "}\n")
      case _ => (hashDeclaration, "")
    }

    // register the hash variable for the key expressions
    if (doRegister) {
      session.addHashVar(ctx, vars, hash)
    }

    // optimize for first column to use fast hashing
    val expr = keyVars.head
    val colVar = expr.value
    val nullVar = expr.isNull
    val firstColumnHash = keysDataType(0) match {
      case BooleanType =>
        hashSingleInt(s"($colVar) ? 1 : 0", nullVar, hash)
      case ByteType | ShortType | IntegerType | DateType =>
        hashSingleInt(colVar, nullVar, hash)
      case LongType | TimestampType =>
        hashSingleLong(colVar, nullVar, hash)
      case FloatType =>
        hashSingleInt(s"Float.floatToIntBits($colVar)", nullVar, hash)
      case DoubleType =>
        hashSingleLong(s"Double.doubleToLongBits($colVar)", nullVar, hash)
      case _: DecimalType =>
        hashSingleInt(s"$colVar.fastHashCode()", nullVar, hash)
      // single column types that use murmur hash already,
      // so no need to further apply mixing on top of it
      case _: StringType | _: ArrayType | _: StructType =>
        s"$hash = ${hashCodeSingleInt(s"$colVar.hashCode()", nullVar)};\n"
      case _ =>
        hashSingleInt(s"$colVar.hashCode()", nullVar, hash)
    }
    if (keyVars.length > 1) {
      keysDataType.tail.zip(keyVars.tail).map {
        case (BooleanType, ev) =>
          addHashInt(s"${ev.value} ? 1 : 0", ev.isNull, hash)
        case ((ByteType | ShortType | IntegerType | DateType), ev) =>
          addHashInt(ev.value, ev.isNull, hash)
        case ((LongType | TimestampType), ev) =>
          addHashLong(ev.value, ev.isNull, hash)
        case (FloatType, ev) =>
          addHashInt(s"Float.floatToIntBits(${ev.value})", ev.isNull, hash)
        case (DoubleType, ev) =>
          addHashLong(s"Double.doubleToLongBits(${ev.value})", ev.isNull,
            hash)
        case (_: DecimalType, ev) =>
          addHashInt(s"${ev.value}.fastHashCode()", ev.isNull, hash)
        case (_, ev) =>
          addHashInt(s"${ev.value}.hashCode()", ev.isNull, hash)
      }.mkString(prefix + firstColumnHash, "", suffix)
    } else prefix + firstColumnHash + suffix
  }


  private def hashSingleInt(colVar: String, nullVar: String,
    hashVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") {
      s"$hashVar = $hashingClass.fastHashInt($colVar);\n"
    } else {
      s"$hashVar = ($nullVar) ? -1 : $hashingClass.fastHashInt($colVar);\n"
    }
  }

  private def hashCodeSingleInt(hashExpr: String, nullVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") hashExpr
    else s"($nullVar) ? -1 : $hashExpr"
  }

  private def hashSingleLong(colVar: String, nullVar: String,
    hashVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") {
      s"$hashVar = $hashingClass.fastHashLong($colVar);\n"
    } else {
      s"$hashVar = ($nullVar) ? -1 : $hashingClass.fastHashLong($colVar);\n"
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
}

object ByteBufferHashMapAccessor {

  def getPartialFunctionForWriting(baseObjectVar: String, offsetVar: String):
  PartialFunction[(DataType, String), String] = {
    val platformClass = classOf[Platform].getName
    val writer: PartialFunction[(DataType, String), String] = {
      case (x: AtomicType, valueVar) => typeOf(x.tag) match {
        case t if t =:= typeOf[Byte] => s"\n ${platformClass}.putByte($baseObjectVar," +
          s" $offsetVar, $valueVar); " +
          s"\n $offsetVar += 1; \n"
        case t if t =:= typeOf[Short] => s"\n ${platformClass}.putShort($baseObjectVar," +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 2; \n"
        case t if t =:= typeOf[Int] => s"\n ${platformClass}.putInt($baseObjectVar, " +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 4; \n"
        case t if t =:= typeOf[Long] => s"\n ${platformClass}.putLong($baseObjectVar," +
          s" $offsetVar, $valueVar); " +
          s"\n $offsetVar += 8; \n"
        case t if t =:= typeOf[Float] => s"\n ${platformClass}.putFloat($baseObjectVar," +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 4; \n"
        case t if t =:= typeOf[Double] => s"\n ${platformClass}.putDouble(" +
          s"$baseObjectVar, $offsetVar, $valueVar); " +
          s"\n $offsetVar += 8; \n"
        case t if t =:= typeOf[Decimal] =>
          throw new UnsupportedOperationException("implement decimal")
        case _ => throw new UnsupportedOperationException("unknown type" + x)
      }
      case (StringType, valueVar) => s"\n ${platformClass}.putInt($baseObjectVar, $offsetVar, " +
        s"$valueVar.numBytes());" +
        s"\n $offsetVar += 4; \n   $valueVar.writeToMemory($baseObjectVar, $offsetVar);" +
        s"\n $offsetVar += $valueVar.numBytes(); \n"
    }
    writer
  }
}