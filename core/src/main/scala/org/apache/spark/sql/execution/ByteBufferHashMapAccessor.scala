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
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoding, StringDictionary}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoinExec}
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

case class ByteBufferHashMapAccessor(@transient session: SnappySession,
  @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
  @transient valueExprs: Seq[Expression], classPrefix: String,
  hashMapTerm: String, dataTerm: String, @transient consumer: CodegenSupport,
  @transient cParent: CodegenSupport, override val child: SparkPlan,
  valueOffsetTerm: String, numKeyBytesTerm: String,
  currentOffSetForMapLookupUpdt: String, valueDataTerm: String,
  vdBaseObjectTerm: String, vdBaseOffsetTerm: String)
  extends UnaryExecNode with CodegenSupport {

  private[this] val hashingClass = classOf[ClientResolverUtils].getName

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("unexpected invocation")

  override def inputRDDs(): Seq[RDD[InternalRow]] = Nil

  override protected def doProduce(ctx: CodegenContext): String =
    throw new UnsupportedOperationException("unexpected invocation")

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
    row: ExprCode): String = {
    throw new UnsupportedOperationException("unexpected invocation")
  }
  def getBufferVars(dataTypes: Seq[DataType], varNames: Seq[String],
    currentValueOffsetTerm: String, isKey: Boolean):
  Seq[ExprCode] = {
    val plaformClass = classOf[Platform].getName
    dataTypes.zip(varNames).map { case (dt, varName) => {
      val nullVar = ctx.freshName("isNull")
      dt match {
        case StringType => {
          val allocator = ctx.freshName("allocator")
          val holder = ctx.freshName("holder")
          val holderBaseObject = ctx.freshName("holderBaseObject")
          val holderBaseOffset = ctx.freshName("holderBaseOffset")
          val allocatorClass = classOf[BufferAllocator].getName
          val gfeCacheImplClass = classOf[GemFireCacheImpl].getName
          val byteBufferClass = classOf[ByteBuffer].getName
          val plaformClass = classOf[Platform].getName
          val len = ctx.freshName("len")
          ExprCode(
            s"""
            int $len = $plaformClass.getInt($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);
          ${allocatorClass} ${allocator} = ${gfeCacheImplClass}.
            getCurrentBufferAllocator();
          ${byteBufferClass} ${holder} =  ${allocator}.
            allocate($len, "SHA");
          Object $holderBaseObject = $allocator.baseObject($holder);
          long $holderBaseOffset = $allocator.baseOffset($holder);
           $currentValueOffsetTerm += 4;
           $plaformClass.copyMemory($vdBaseObjectTerm,
            $vdBaseOffsetTerm + $currentValueOffsetTerm,
              $holderBaseObject, $holderBaseOffset , $len);
           $varName = ${classOf[UTF8String].getName}.
           fromAddress($holderBaseObject, $holderBaseOffset, $len);
             $currentValueOffsetTerm += $len;
           boolean $nullVar = false;
          """.stripMargin, nullVar, varName)
        }
        case x =>
          ExprCode(s"$varName = ${
            x match {
              case ByteType => s"$plaformClass.getByte($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm); "
              case ShortType => s"$plaformClass.getShort($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm);"
              case IntegerType => s"$plaformClass.getInt($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm); "
              case LongType => s"$plaformClass.getLong($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm); "
              case FloatType => s"$plaformClass.getFloat($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm); "
              case DoubleType => s"$plaformClass.getDouble($vdBaseObjectTerm, " +
                s"$vdBaseOffsetTerm + $currentValueOffsetTerm); "
            }
          }; " +
            s"""
                \n $currentValueOffsetTerm += ${dt.defaultSize}; \n boolean $nullVar = false;
            // System.out.println(${if (isKey) "\"key = \"" else "\"value = \""} + $varName);\n
             """,
            nullVar, varName)

      }
    }

    }
  }


  def initKeyOrBufferVal(aggregateDataTypes: Seq[DataType], aggVarNames: Seq[String]):
  String = {
    aggregateDataTypes.zip(aggVarNames).map { case (dt, varName) => {
      dt match {
        case ByteType => s"byte ${varName} = 0;"
        case ShortType => s"short ${varName} = 0;"
        case IntegerType => s"int ${varName} = 0;"
        case LongType => s"long ${varName} = 0;"
        case FloatType => s"float ${varName} = 0;"
        case DoubleType => s"double ${varName} = 0;"
        case StringType => s"${classOf[UTF8String].getName} ${varName} = null;"
      }
    }
    }.mkString("\n")
  }


  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(aggregateBufferVars: Seq[String], valueInitVars: Seq[ExprCode],
    valueInitCode: String, input: Seq[ExprCode], keyVars: Seq[ExprCode],
    keysDataType: Seq[DataType], aggregateDataTypes: Seq[DataType]): String = {
    val hashVar = Array(ctx.freshName("hash"))

    val keyBytesHolder = (ctx.freshName("keyBytesHolder"))
    val numValueBytes = (ctx.freshName("numValueBytes"))
    val allocator = ctx.freshName("allocator")
    val allocatorClass = classOf[BufferAllocator].getName
    val gfeCacheImplClass = classOf[GemFireCacheImpl].getName
    val baseKeyoffset = ctx.freshName("baseKeyoffset")
    val baseObject = ctx.freshName("baseObject")

    val valueInit = valueInitCode + '\n'
    val numAggBytes = aggregateDataTypes.foldLeft(0)( _ + _.defaultSize)
    /* generateUpdate(objVar, Nil,
      valueInitVars, forKey = false, doCopy = false) */


        val inputEvals = evaluateVariables(input)

        s"""
           ${allocatorClass} ${allocator} = ${gfeCacheImplClass}.
                   getCurrentBufferAllocator();
          // evaluate the key expressions
          $inputEvals
          ${evaluateVariables(keyVars)}
          // evaluate hash code of the lookup key
          ${generateHashCode(hashVar, keyVars, this.keyExprs, keysDataType)}
        //  System.out.println("hash code for key = " +${hashVar(0)});
          int ${numKeyBytesTerm} = 0;
          ${generateKeySizeCode(keyVars, keysDataType, numKeyBytesTerm)}
          int ${numValueBytes} = ${numAggBytes};
          ${generateKeyBytesHolderCode(numKeyBytesTerm, numValueBytes, keyBytesHolder, keyVars,
      keysDataType, aggregateDataTypes, allocator, baseObject, baseKeyoffset)}
           // insert or lookup
          int $valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($baseObject, $baseKeyoffset,
      $numKeyBytesTerm, $numValueBytes + $numKeyBytesTerm, ${hashVar(0)});
          // read key bytes length & position the offset to start of aggregate value
          $valueOffsetTerm += $numKeyBytesTerm;
       // ${classOf[ColumnEncoding].getName}.getInt($vdBaseObjectTerm, $vdBaseOffsetTerm + $valueOffsetTerm - 4) ;
          long $currentOffSetForMapLookupUpdt = $valueOffsetTerm;

         """

  }


  def generateUpdate(columnVars: Seq[ExprCode], aggBufferDataType: Seq[DataType],
    resultVars: Seq[ExprCode]): String = {
    val plaformClass = classOf[Platform].getName

     resultVars.zip(aggBufferDataType).
      map{case(expr, dt) => {
      (dt match {
        case ByteType => s"$plaformClass.putByte($vdBaseObjectTerm," +
          s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value});"
        case ShortType => s"$plaformClass.putShort($vdBaseObjectTerm," +
          s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value});"
        case IntegerType => s"$plaformClass.putInt($vdBaseObjectTerm, " +
          s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
        case LongType => s"$plaformClass.putLong($vdBaseObjectTerm, " +
          s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
        case FloatType => s"$plaformClass.putFloat($vdBaseObjectTerm, " +
          s"$vdBaseOffsetTerm+ $currentOffSetForMapLookupUpdt, ${expr.value}); "
        case DoubleType => s"$plaformClass.putDouble($vdBaseObjectTerm, " +
          s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
      })  + s"\n $currentOffSetForMapLookupUpdt += ${dt.defaultSize}; \n "
    }}.mkString("")
    /*
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
    }.mkString("\n") */
  }

  def generateKeyBytesHolderCode(numKeyBytesVar: String, numValueBytesVar: String, keyBytesHolderVar: String,
    keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType], allocatorTerm: String, baseObjectTerm: String, baseKeyoffsetTerm: String): String = {

    val byteBufferClass = classOf[ByteBuffer].getName
    val currentOffset = (ctx.freshName("currentOffset"))
    val plaformClass = classOf[Platform].getName
   val writer = ByteBufferHashMapAccessor.getPartialFunctionForWriting(baseObjectTerm, currentOffset)
    s"""


        ${byteBufferClass} ${keyBytesHolderVar} =  ${allocatorTerm}.
        allocate($numKeyBytesVar + $numValueBytesVar, "SHA");
        Object $baseObjectTerm = $allocatorTerm.baseObject($keyBytesHolderVar);
        long $baseKeyoffsetTerm = $allocatorTerm.baseOffset($keyBytesHolderVar);
        long $currentOffset = $baseKeyoffsetTerm;
        ${keysDataType.zip(keyVars.map(_.value)).foldLeft(""){
      case (code, (dt, variable)) => {
        var codex = code
        codex = codex  + (dt match {
          case x: AtomicType => typeOf(x.tag) match {
            case t if t =:= typeOf[Byte] => s"\n ${plaformClass}.putByte($baseObjectTerm," +
              s" $currentOffset, $variable); " +
              s"\n $currentOffset += 1; \n"
            case t if t =:= typeOf[Short] => s"\n ${plaformClass}.putShort($baseObjectTerm," +
              s"$currentOffset, $variable); " +
              s"\n $currentOffset += 2; \n"
            case t if t =:= typeOf[Int] => s"\n ${plaformClass}.putInt($baseObjectTerm, " +
              s"$currentOffset, $variable); " +
              s"\n $currentOffset += 4; \n"
            case t if t =:= typeOf[Long] => s"\n ${plaformClass}.putLong($baseObjectTerm," +
              s" $currentOffset, $variable); " +
              s"\n $currentOffset += 8; \n"
            case t if t =:= typeOf[Float] => s"\n ${plaformClass}.putFloat($baseObjectTerm," +
              s"$currentOffset, $variable); " +
              s"\n $currentOffset += 4; \n"
            case t if t =:= typeOf[Double] => s"\n ${plaformClass}.putDouble(" +
              s"$baseObjectTerm, $currentOffset, $variable); " +
              s"\n $currentOffset += 8; \n"
            case t if t =:= typeOf[Decimal] =>
              throw new UnsupportedOperationException("implement decimal")
            case t if t =:= typeOf[UTF8String]  =>  s"\n ${plaformClass}.putInt($baseObjectTerm, $currentOffset, " +
              s"$variable.numBytes());" +
              s"\n $currentOffset += 4; \n   $variable.writeToMemory($baseObjectTerm, $currentOffset);" +
              s"\n $currentOffset += $variable.numBytes(); \n"
            case _ => throw new UnsupportedOperationException("unknown type " + dt)
          }
          case _ => throw new UnsupportedOperationException("unknown type " + dt)

        })
        codex
    }}.mkString("")}
       // now add values
       ${aggregatesDataType.foldLeft("")((code, dt) => {
      var codex = code
      codex = codex  + (dt match {
        case x: AtomicType => typeOf(x.tag) match {
          case t if t =:= typeOf[Byte] => s"\n ${plaformClass}.putByte($baseObjectTerm," +
            s" $currentOffset, 0); " +
            s"\n $currentOffset += 1; \n"
          case t if t =:= typeOf[Short] => s"\n ${plaformClass}.putShort($baseObjectTerm," +
            s"$currentOffset, 0); " +
            s"\n $currentOffset += 2; \n"
          case t if t =:= typeOf[Int] => s"\n ${plaformClass}.putInt($baseObjectTerm, " +
            s"$currentOffset, 0); " +
            s"\n $currentOffset += 4; \n"
          case t if t =:= typeOf[Long] => s"\n ${plaformClass}.putLong($baseObjectTerm," +
            s" $currentOffset, 0l); " +
            s"\n $currentOffset += 8; \n"
          case t if t =:= typeOf[Float] => s"\n ${plaformClass}.putFloat($baseObjectTerm," +
            s"$currentOffset, 0f); " +
            s"\n $currentOffset += 4; \n"
          case t if t =:= typeOf[Double] => s"\n ${plaformClass}.putDouble(" +
            s"$baseObjectTerm, $currentOffset, 0); " +
            s"\n $currentOffset += 8; \n"
          case t if t =:= typeOf[Decimal] =>
            throw new UnsupportedOperationException("implement decimal")
          case _ => throw new UnsupportedOperationException("unknown type" + dt)
        }
    })
    codex
    }).mkString("")}

         """
  }

  def generateKeySizeCode(keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    numKeyBytesVar: String): String = {

    s"""
         ${keysDataType.zipWithIndex.foldLeft(s"${numKeyBytesVar} = " ){case (expr, (dt, i)) => {
      var rs = expr
      if (i > 0) {
        rs = rs + " + "
      }
      if (TypeUtilities.isFixedWidth(dt)) {
        rs = rs + dt.defaultSize.toString + ";"
      } else {
        rs = rs + s"${keyVars(i).value}.numBytes() + 4;"
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