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

import scala.reflect.runtime.universe._

import com.gemstone.gemfire.internal.shared.ClientResolverUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

case class SHAMapAccessor(@transient session: SnappySession,
  @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
  @transient valueExprs: Seq[Expression], classPrefix: String,
  hashMapTerm: String, @transient consumer: CodegenSupport,
  @transient cParent: CodegenSupport, override val child: SparkPlan,
  valueOffsetTerm: String, numKeyBytesTerm: String,
  currentOffSetForMapLookupUpdt: String, valueDataTerm: String,
  vdBaseObjectTerm: String, vdBaseOffsetTerm: String,
  nullKeysBitsetTerm: String, numBytesForNullKeyBits: Int, allocatorTerm: String)
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

    val byteBufferClass = classOf[ByteBuffer].getName

    dataTypes.zip(varNames).zipWithIndex.map { case ((dt, varName), i) =>
      val nullVar = ctx.freshName("isNull")
      val nullVarCode = if (isKey) {
        val castTerm = getKeyNullBitsCastTerm
        if (numBytesForNullKeyBits <= 8) {
          s"""
            boolean $nullVar = ($nullKeysBitsetTerm & ( (($castTerm)0x01) << $i)) == 0;
        """.stripMargin
        } else {
          val remainder = i % 8
          val index = i / 8
          s"""
            boolean $nullVar = ($nullKeysBitsetTerm[$index] & (0x01 << $remainder)) == 0;\n
        """.stripMargin
        }
      } else {
        s"boolean $nullVar = false;"
      }

      val evaluationCode = dt match {
        case StringType =>
          val holder = ctx.freshName("holder")
          val holderBaseObject = ctx.freshName("holderBaseObject")
          val holderBaseOffset = ctx.freshName("holderBaseOffset")


          val len = ctx.freshName("len")

          s"""
            int $len = $plaformClass.getInt($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);

          $byteBufferClass $holder =  $allocatorTerm.
            allocate($len, "SHA");
          Object $holderBaseObject = $allocatorTerm.baseObject($holder);
          long $holderBaseOffset = $allocatorTerm.baseOffset($holder);
           $currentValueOffsetTerm += 4;
           $plaformClass.copyMemory($vdBaseObjectTerm,
            $vdBaseOffsetTerm + $currentValueOffsetTerm,
              $holderBaseObject, $holderBaseOffset , $len);
           $varName = ${classOf[UTF8String].getName}.
           fromAddress($holderBaseObject, $holderBaseOffset, $len);
             $currentValueOffsetTerm += $len;
          """.stripMargin
        case x =>
          s"$varName = ${
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
                \n $currentValueOffsetTerm += ${dt.defaultSize}; \n
            // System.out.println(${if (isKey) "\"key = \"" else "\"value = \""} + $varName);\n
             """
      }
      val exprCode =
        s"""
            $nullVarCode
           if (!$nullVar) {
             $evaluationCode
           }
         """.stripMargin
      ExprCode(exprCode, nullVar, varName)
    }
  }

  def readNullKeyBitsCode(currentValueOffsetTerm: String): String = {
    val plaformClass = classOf[Platform].getName
    if (numBytesForNullKeyBits == 1) {
      s"""
          byte $nullKeysBitsetTerm = $plaformClass.getByte($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 1; \n
          """.stripMargin
    } else if (numBytesForNullKeyBits == 2) {
      s"""
          short $nullKeysBitsetTerm = $plaformClass.getShort($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 2; \n
          """.stripMargin
    } else if (numBytesForNullKeyBits <= 4) {
      s"""
          int $nullKeysBitsetTerm = $plaformClass.getInt($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 4; \n
          """.stripMargin
    } else if (numBytesForNullKeyBits <= 8) {
      s"""
          long $nullKeysBitsetTerm = $plaformClass.getLong($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 8; \n
          """.stripMargin
    } else {
      s"""
           $plaformClass.copyMemory($vdBaseObjectTerm,
          $vdBaseOffsetTerm + $currentValueOffsetTerm, $nullKeysBitsetTerm,
           ${Platform.BYTE_ARRAY_OFFSET}, $numBytesForNullKeyBits); \n
           $currentValueOffsetTerm += $numBytesForNullKeyBits; \n
         """.stripMargin
    }
  }


  def initKeyOrBufferVal(aggregateDataTypes: Seq[DataType], aggVarNames: Seq[String]):
  String = {
    aggregateDataTypes.zip(aggVarNames).map { case (dt, varName) =>
      dt match {
        case ByteType => s"byte $varName = 0;"
        case ShortType => s"short $varName = 0;"
        case IntegerType => s"int $varName = 0;"
        case LongType => s"long $varName = 0;"
        case FloatType => s"float $varName = 0;"
        case DoubleType => s"double $varName = 0;"
        case StringType => s"${classOf[UTF8String].getName} $varName = null;"
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

    val keyBytesHolder = ctx.freshName("keyBytesHolder")
    val numValueBytes = ctx.freshName("numValueBytes")

    val baseKeyoffset = ctx.freshName("baseKeyoffset")
    val baseKeyObject = ctx.freshName("baseKeyObject")

    // val valueInit = valueInitCode + '\n'
    val numAggBytes = aggregateDataTypes.foldLeft(0)(_ + _.defaultSize)
    /* generateUpdate(objVar, Nil,
      valueInitVars, forKey = false, doCopy = false) */


    val inputEvals = evaluateVariables(input)

    s"""
           $initNullKeyBitsetCode

          // evaluate input row vars
          $inputEvals

           // evaluate key vars
          ${evaluateVariables(keyVars)}

           // evaluate null key bits
          ${evaluateNullKeyBits(keyVars)}

          // evaluate hash code of the lookup key
          ${generateHashCode(hashVar, keyVars, this.keyExprs, keysDataType)}

          //  System.out.println("hash code for key = " +${hashVar(0)});
          // get key size code
          int $numKeyBytesTerm = 0;
          ${generateKeySizeCode(keyVars, keysDataType, numKeyBytesTerm)}

          int $numValueBytes = $numAggBytes;
          ${
      generateKeyBytesHolderCode(numKeyBytesTerm, numValueBytes, keyBytesHolder, keyVars,
        keysDataType, aggregateDataTypes, baseKeyObject, baseKeyoffset)
    }
           // insert or lookup
          int $valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($baseKeyObject, $baseKeyoffset,
      $numKeyBytesTerm, $numValueBytes + $numKeyBytesTerm, ${hashVar(0)});
          // position the offset to start of aggregate value
          $valueOffsetTerm += $numKeyBytesTerm;
          long $currentOffSetForMapLookupUpdt = $valueOffsetTerm;
         """

  }

  def initNullKeyBitsetCode: String = if (numBytesForNullKeyBits == 1) {
    s"byte $nullKeysBitsetTerm = 0;"
  } else if (numBytesForNullKeyBits == 2) {
    s"short $nullKeysBitsetTerm = 0;"
  } else if (numBytesForNullKeyBits <= 4) {
    s"int $nullKeysBitsetTerm = 0;"
  } else if (numBytesForNullKeyBits <= 8) {
    s"long $nullKeysBitsetTerm = 0l;"
  } else {
    s"""
       for( int i = 0 ; i < $numBytesForNullKeyBits; ++i) {
         $nullKeysBitsetTerm[i] = 0;
       }
     """.stripMargin
  }


  def evaluateNullKeyBits(keyVars: Seq[ExprCode]): String = {
    val castTerm = getKeyNullBitsCastTerm
    keyVars.zipWithIndex.map {
      case (expr, i) =>
        val nullVar = expr.isNull
        if (numBytesForNullKeyBits > 8) {
          val remainder = i % 8
          val index = i / 8

          if (nullVar.isEmpty || nullVar == "false") {
            s"""
            $nullKeysBitsetTerm[$index] |= (byte)((0x01 << $remainder));
          """.stripMargin
          } else {
            s"""
            if (!$nullVar) {
              $nullKeysBitsetTerm[$index] |= (byte)((0x01 << $remainder));
            }
          """.stripMargin
          }
        }
        else {

          if (nullVar.isEmpty || nullVar == "false") {
            s"""
            $nullKeysBitsetTerm |= ($castTerm)(( (($castTerm)0x01) << $i));
          """.stripMargin
          } else {
            s"""
            if (!$nullVar) {
              $nullKeysBitsetTerm |= ($castTerm)(( (($castTerm)0x01) << $i));
            }
          """.stripMargin
          }
        }
    }.mkString("\n")
  }


  private def getKeyNullBitsCastTerm = if (numBytesForNullKeyBits == 1) {
      "byte"
    } else if (numBytesForNullKeyBits == 2) {
      "short"
    } else if (numBytesForNullKeyBits <= 4) {
      "int"
    } else {
      "long"
    }


  def generateUpdate(columnVars: Seq[ExprCode], aggBufferDataType: Seq[DataType],
    resultVars: Seq[ExprCode]): String = {
    val plaformClass = classOf[Platform].getName

    resultVars.zip(aggBufferDataType).
      map { case (expr, dt) =>
        (dt match {
          case ByteType => s"$plaformClass.putByte($vdBaseObjectTerm," +
            s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value});"
          case ShortType => s"$plaformClass.putShort($vdBaseObjectTerm," +
            s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value});"
          case IntegerType => s"$plaformClass.putInt($vdBaseObjectTerm, " +
            s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
          case LongType => s" $plaformClass.putLong($vdBaseObjectTerm," +
            s" $vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value});"
          case FloatType => s"$plaformClass.putFloat($vdBaseObjectTerm, " +
            s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
          case DoubleType => s"$plaformClass.putDouble($vdBaseObjectTerm, " +
            s"$vdBaseOffsetTerm + $currentOffSetForMapLookupUpdt, ${expr.value}); "
        }) + s"\n $currentOffSetForMapLookupUpdt += ${dt.defaultSize}; \n "
      }.mkString("")
  }

  def generateKeyBytesHolderCode(numKeyBytesVar: String, numValueBytesVar: String,
    keyBytesHolderVar: String, keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType], baseKeyObject: String, baseKeyoffsetTerm: String): String = {

    val byteBufferClass = classOf[ByteBuffer].getName
    val currentOffset = ctx.freshName("currentOffset")
    val plaformClass = classOf[Platform].getName
    /*
    val writer = ByteBufferHashMapAccessor.getPartialFunctionForWriting(
      baseObjectTerm, currentOffset)
      */
    s"""
        $byteBufferClass $keyBytesHolderVar =  $allocatorTerm.
        allocate($numKeyBytesVar + $numValueBytesVar, "SHA");
        Object $baseKeyObject = $allocatorTerm.baseObject($keyBytesHolderVar);
        long $baseKeyoffsetTerm = $allocatorTerm.baseOffset($keyBytesHolderVar);
        long $currentOffset = $baseKeyoffsetTerm;
        ${writeNullKeyBits(baseKeyObject, currentOffset)}

        ${
      keysDataType.zip(keyVars).foldLeft("") {
        case (code, (dt, expr)) =>
          val variable = expr.value
          val isNull = expr.isNull
          var codex = code
          codex += (dt match {
            case x: AtomicType =>
              val snippet = typeOf(x.tag) match {
                case t if t =:= typeOf[Byte] =>
                  s"""
                   $plaformClass.putByte($baseKeyObject, $currentOffset,
                   $variable); \n
                   $currentOffset += 1; \n
                """.stripMargin
                case t if t =:= typeOf[Short] =>
                  s"""
              $plaformClass.putShort($baseKeyObject,
              $currentOffset, $variable);\n
              $currentOffset += 2;\n
              """.stripMargin
                case t if t =:= typeOf[Int] =>
                  s"""
                  $plaformClass.putInt($baseKeyObject, $currentOffset, $variable);\n
                 $currentOffset += 4; \n
                 """.stripMargin
                case t if t =:= typeOf[Long] => s"""
               $plaformClass.putLong($baseKeyObject, $currentOffset, $variable);\n
               $currentOffset += 8; \n
               """
                case t if t =:= typeOf[Float] => s"""
              $plaformClass.putFloat($baseKeyObject, $currentOffset, $variable);\n
               $currentOffset += 4; \n
               """.stripMargin
                case t if t =:= typeOf[Double] => s"""
              $plaformClass.putDouble($baseKeyObject, $currentOffset, $variable);\n
               $currentOffset += 8; \n
               """.stripMargin
                case t if t =:= typeOf[Decimal] =>
                  throw new UnsupportedOperationException("implement decimal")
                case t if t =:= typeOf[UTF8String] => s"""
              $plaformClass.putInt($baseKeyObject, $currentOffset, $variable.numBytes());\n
               $currentOffset += 4; \n
               $variable.writeToMemory($baseKeyObject, $currentOffset);\n
               $currentOffset += $variable.numBytes();\n
               """
                case _ => throw new UnsupportedOperationException("unknown type " + dt)
              }
              if (isNull.isEmpty || isNull == "false") {
                snippet
              } else {
                s"""
               if (!$isNull) {
                 $snippet
               }
             """.stripMargin
              }
            case _ => throw new UnsupportedOperationException("unknown type " + dt)
          })
          codex
      }.mkString("")
    }
       // now add values
       ${
      aggregatesDataType.foldLeft("")((code, dt) => {
        var codex = code
        codex += (dt match {
          case x: AtomicType => typeOf(x.tag) match {
            case t if t =:= typeOf[Byte] => s"\n $plaformClass.putByte($baseKeyObject," +
              s" $currentOffset, 0); " +
              s"\n $currentOffset += 1; \n"
            case t if t =:= typeOf[Short] => s"\n $plaformClass.putShort($baseKeyObject," +
              s"$currentOffset, 0); " +
              s"\n $currentOffset += 2; \n"
            case t if t =:= typeOf[Int] => s"\n $plaformClass.putInt($baseKeyObject, " +
              s"$currentOffset, 0); " +
              s"\n $currentOffset += 4; \n"
            case t if t =:= typeOf[Long] => s"\n $plaformClass.putLong($baseKeyObject," +
              s" $currentOffset, 0l); " +
              s"\n $currentOffset += 8; \n"
            case t if t =:= typeOf[Float] => s"\n $plaformClass.putFloat($baseKeyObject," +
              s"$currentOffset, 0f); " +
              s"\n $currentOffset += 4; \n"
            case t if t =:= typeOf[Double] => s"\n $plaformClass.putDouble(" +
              s"$baseKeyObject, $currentOffset, 0d); " +
              s"\n $currentOffset += 8; \n"
            case t if t =:= typeOf[Decimal] =>
              throw new UnsupportedOperationException("implement decimal")
            case _ => throw new UnsupportedOperationException("unknown type" + dt)
          }
        })
        codex
      }).mkString("")
    }

         """
  }

  def writeNullKeyBits(baseObjectTerm: String, currentOffsetTerm: String): String = {
    val plaformClass = classOf[Platform].getName
    if (numBytesForNullKeyBits == 1) {
      s"\n $plaformClass.putByte($baseObjectTerm," +
        s" $currentOffsetTerm, $nullKeysBitsetTerm); " +
        s"\n $currentOffsetTerm += 1; \n"
    } else if (numBytesForNullKeyBits == 2) {
      s"\n $plaformClass.putShort($baseObjectTerm," +
        s" $currentOffsetTerm, $nullKeysBitsetTerm); " +
        s"\n $currentOffsetTerm += 2; \n"
    } else if (numBytesForNullKeyBits <= 4) {
      s"\n $plaformClass.putInt($baseObjectTerm," +
        s" $currentOffsetTerm, $nullKeysBitsetTerm); " +
        s"\n $currentOffsetTerm += 4; \n"
    } else if (numBytesForNullKeyBits <= 8) {
      s"$plaformClass.putLong($baseObjectTerm, $currentOffsetTerm, " +
        s"$nullKeysBitsetTerm);\n" +
        s"$currentOffsetTerm += 8; \n"
    } else {
      s"\n $plaformClass.copyMemory($nullKeysBitsetTerm, ${Platform.BYTE_ARRAY_OFFSET}," +
        s" $baseObjectTerm, $currentOffsetTerm, $numBytesForNullKeyBits);" +
        s"\n $currentOffsetTerm += $numBytesForNullKeyBits; \n"
    }
  }

  def generateKeySizeCode(keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    numKeyBytesVar: String): String = {
    s"$numKeyBytesVar = " +
      keysDataType.zip(keyVars).zipWithIndex.map { case ((dt, expr), i) =>
        val nullVar = expr.isNull
        val notNullSizeExpr = if (TypeUtilities.isFixedWidth(dt)) {
          dt.defaultSize.toString
        } else {
          s" (${keyVars(i).value}.numBytes() + 4) "
        }
        if (nullVar.isEmpty || nullVar == "false") {
          notNullSizeExpr
        } else {
          s" ($nullVar? 0 : $notNullSizeExpr)"
        }
      }.mkString(" + ") + (
      if (numBytesForNullKeyBits < 3 || numBytesForNullKeyBits > 8) {
        s" + $numBytesForNullKeyBits; \n"
      } else if (numBytesForNullKeyBits <= 4) {
        s" + 4; \n"
      } else {
        s" + 8; \n"
      })
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
    val firstColumnHash = keysDataType.head match {
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
        case (ByteType | ShortType | IntegerType | DateType, ev) =>
          addHashInt(ev.value, ev.isNull, hash)
        case (LongType | TimestampType, ev) =>
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

object SHAMapAccessor {

  def getPartialFunctionForWriting(baseObjectVar: String, offsetVar: String):
  PartialFunction[(DataType, String), String] = {
    val platformClass = classOf[Platform].getName
    val writer: PartialFunction[(DataType, String), String] = {
      case (x: AtomicType, valueVar) => typeOf(x.tag) match {
        case t if t =:= typeOf[Byte] => s"\n $platformClass.putByte($baseObjectVar," +
          s" $offsetVar, $valueVar); " +
          s"\n $offsetVar += 1; \n"
        case t if t =:= typeOf[Short] => s"\n $platformClass.putShort($baseObjectVar," +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 2; \n"
        case t if t =:= typeOf[Int] => s"\n $platformClass.putInt($baseObjectVar, " +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 4; \n"
        case t if t =:= typeOf[Long] => s"\n $platformClass.putLong($baseObjectVar," +
          s" $offsetVar, $valueVar); " +
          s"\n $offsetVar += 8; \n"
        case t if t =:= typeOf[Float] => s"\n $platformClass.putFloat($baseObjectVar," +
          s"$offsetVar, $valueVar); " +
          s"\n $offsetVar += 4; \n"
        case t if t =:= typeOf[Double] => s"\n $platformClass.putDouble(" +
          s"$baseObjectVar, $offsetVar, $valueVar); " +
          s"\n $offsetVar += 8; \n"
        case t if t =:= typeOf[Decimal] =>
          throw new UnsupportedOperationException("implement decimal")
        case _ => throw new UnsupportedOperationException("unknown type" + x)
      }
      case (StringType, valueVar) => s"\n $platformClass.putInt($baseObjectVar, $offsetVar, " +
        s"$valueVar.numBytes());" +
        s"\n $offsetVar += 4; \n   $valueVar.writeToMemory($baseObjectVar, $offsetVar);" +
        s"\n $offsetVar += $valueVar.numBytes(); \n"
    }
    writer
  }
}