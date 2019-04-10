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
  nullKeysBitsetTerm: String, numBytesForNullKeyBits: Int,
  allocatorTerm: String, numBytesForNullAggBits: Int,
  nullAggsBitsetTerm: String)
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
    val nullBitTerm = if (isKey) nullKeysBitsetTerm else nullAggsBitsetTerm
    val numBytesForNullBits = if (isKey) numBytesForNullKeyBits else numBytesForNullAggBits
    val castTerm = getNullBitsCastTerm(numBytesForNullBits)
    dataTypes.zip(varNames).zipWithIndex.map { case ((dt, varName), i) =>
      val nullVar = ctx.freshName("isNull")
      val nullVarCode =
        if (numBytesForNullBits <= 8) {
          s"""
            boolean $nullVar = ($nullBitTerm & ( (($castTerm)0x01) << $i)) == 0;\n
        """.stripMargin
        } else {
          val remainder = i % 8
          val index = i / 8
          s"""
            boolean $nullVar = ($nullBitTerm[$index] & (0x01 << $remainder)) == 0;\n
        """.stripMargin
        }


      val evaluationCode = dt match {
        case StringType =>
          val holder = ctx.freshName("holder")
          val holderBaseObject = ctx.freshName("holderBaseObject")
          val holderBaseOffset = ctx.freshName("holderBaseOffset")


          val len = ctx.freshName("len")

          s"""
            int $len = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);

          $byteBufferClass $holder =  $allocatorTerm.
            allocate($len, "SHA");
          Object $holderBaseObject = $allocatorTerm.baseObject($holder);
          long $holderBaseOffset = $allocatorTerm.baseOffset($holder);
           $currentValueOffsetTerm += 4;
           $plaformClass.copyMemory($vdBaseObjectTerm,
            $currentValueOffsetTerm, $holderBaseObject, $holderBaseOffset , $len);
           $varName = ${classOf[UTF8String].getName}.
           fromAddress($holderBaseObject, $holderBaseOffset, $len);
             $currentValueOffsetTerm += $len;
          """.stripMargin
        case x =>
          s"$varName = ${
            x match {
              case ByteType => s"$plaformClass.getByte($vdBaseObjectTerm, " +
                s"$currentValueOffsetTerm); "
              case ShortType => s"$plaformClass.getShort($vdBaseObjectTerm, " +
                s"$currentValueOffsetTerm);"
              case IntegerType => s"$plaformClass.getInt($vdBaseObjectTerm, " +
                s"$currentValueOffsetTerm); "
              case LongType => s"$plaformClass.getLong($vdBaseObjectTerm, " +
                s" $currentValueOffsetTerm); "
              case FloatType => s"$plaformClass.getFloat($vdBaseObjectTerm, " +
                s" $currentValueOffsetTerm); "
              case DoubleType => s"$plaformClass.getDouble($vdBaseObjectTerm, " +
                s" $currentValueOffsetTerm); "
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
           }${ if (!isKey) {
          s"""
             else {
               $currentValueOffsetTerm += ${dt.defaultSize};\n
             }
           """.stripMargin
        } else {
          ""
        }
        }
         """.stripMargin
      ExprCode(exprCode, nullVar, varName)
    }
  }

  def readNullBitsCode(currentValueOffsetTerm: String, isKey: Boolean): String = {
    val plaformClass = classOf[Platform].getName
    val (nullBitsetTerm, numBytesForNullBits) = if (isKey) {
      (nullKeysBitsetTerm, numBytesForNullKeyBits)
    } else {
      (nullAggsBitsetTerm, numBytesForNullAggBits)
    }

    if (numBytesForNullBits == 1) {
      s"""
          $nullBitsetTerm = $plaformClass.getByte($vdBaseObjectTerm, $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 1; \n
          """.stripMargin
    } else if (numBytesForNullBits == 2) {
      s"""
          $nullBitsetTerm = $plaformClass.getShort($vdBaseObjectTerm, $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 2; \n
          """.stripMargin
    } else if (numBytesForNullBits <= 4) {
      s"""
          $nullBitsetTerm = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 4; \n
          """.stripMargin
    } else if (numBytesForNullBits <= 8) {
      s"""
          $nullBitsetTerm = $plaformClass.getLong($vdBaseObjectTerm, $currentValueOffsetTerm);\n
          $currentValueOffsetTerm += 8; \n
          """.stripMargin
    } else {
      s"""
           $plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm, $nullBitsetTerm,
           ${Platform.BYTE_ARRAY_OFFSET}, $numBytesForNullBits); \n
           $currentValueOffsetTerm += $numBytesForNullBits; \n
         """.stripMargin
    }
  }


  def initKeyOrBufferVal(dataTypes: Seq[DataType], varNames: Seq[String]):
  String = dataTypes.zip(varNames).map { case (dt, varName) =>
      s"${ctx.javaType(dt)} $varName = ${ctx.defaultValue(dt)};"
  }.mkString("\n")



  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(valueInitVars: Seq[ExprCode],
    valueInitCode: String, input: Seq[ExprCode], keyVars: Seq[ExprCode],
    keysDataType: Seq[DataType], aggregateDataTypes: Seq[DataType]): String = {
    val hashVar = Array(ctx.freshName("hash"))

    val keyBytesHolder = ctx.freshName("keyBytesHolder")
    val numValueBytes = ctx.freshName("numValueBytes")

    val baseKeyoffset = ctx.freshName("baseKeyoffset")
    val baseKeyObject = ctx.freshName("baseKeyObject")

    // val valueInit = valueInitCode + '\n'
    val numAggBytes = aggregateDataTypes.foldLeft(0)(_ + _.defaultSize) +
      sizeForNullBits(numBytesForNullAggBits)
    /* generateUpdate(objVar, Nil,
      valueInitVars, forKey = false, doCopy = false) */


    val inputEvals = evaluateVariables(input)

    s"""
           $valueInitCode
           ${initNullBitsetCode (nullKeysBitsetTerm, numBytesForNullKeyBits)}
           ${initNullBitsetCode (nullAggsBitsetTerm, numBytesForNullAggBits)}
          // evaluate input row vars
          $inputEvals

           // evaluate key vars
          ${evaluateVariables(keyVars)}

          // evaluate hash code of the lookup key
          ${generateHashCode(hashVar, keyVars, this.keyExprs, keysDataType)}

          //  System.out.println("hash code for key = " +${hashVar(0)});
          // get key size code
          int $numKeyBytesTerm = 0;
          ${generateKeySizeCode(keyVars, keysDataType, numKeyBytesTerm)}

          int $numValueBytes = $numAggBytes;
          ${
      generateKeyBytesHolderCode(numKeyBytesTerm, numValueBytes, keyBytesHolder, keyVars,
        keysDataType, aggregateDataTypes, valueInitVars, baseKeyObject, baseKeyoffset)
    }
           // insert or lookup
          int $valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($baseKeyObject, $baseKeyoffset,
      $numKeyBytesTerm, $numValueBytes + $numKeyBytesTerm, ${hashVar(0)});
          // position the offset to start of aggregate value
          $valueOffsetTerm += $numKeyBytesTerm + $vdBaseOffsetTerm;
          long $currentOffSetForMapLookupUpdt = $valueOffsetTerm;
         """

  }

  def initNullBitsetCode(nullBitsetTerm: String,
    numBytesForNullBits: Int): String = if (numBytesForNullBits == 1) {
    s"byte $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits == 2) {
    s"short $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits <= 4) {
    s"int $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits <= 8) {
    s"long $nullBitsetTerm = 0l;"
  } else {
    s"""
       for( int i = 0 ; i < $numBytesForNullBits; ++i) {
         $nullBitsetTerm[i] = 0;
       }
     """.stripMargin
  }

  def resetNullBitsetCode(nullBitsetTerm: String,
    numBytesForNullBits: Int): String = if (numBytesForNullBits <= 8) {
    s"$nullBitsetTerm = 0; \n"
  } else {
    s"""
       for( int i = 0 ; i < $numBytesForNullBits; ++i) {
         $nullBitsetTerm[i] = 0;
       }

     """.stripMargin
  }





  private def getNullBitsCastTerm(numBytesForNullBits: Int) = if (numBytesForNullBits == 1) {
      "byte"
    } else if (numBytesForNullBits == 2) {
      "short"
    } else if (numBytesForNullBits <= 4) {
      "int"
    } else {
      "long"
    }


  def generateUpdate(bufferVars: Seq[ExprCode], aggBufferDataType: Seq[DataType]): String = {
    val plaformClass = classOf[Platform].getName
    s"""
      ${resetNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggBits)}
      ${writeKeyOrValue(vdBaseObjectTerm, currentOffSetForMapLookupUpdt,
            aggBufferDataType, bufferVars, nullAggsBitsetTerm, numBytesForNullAggBits,
            false)}
    """.stripMargin

  }


  def writeKeyOrValue(baseObjectTerm: String, offsetTerm: String,
    dataTypes: Seq[DataType], varsToWrite: Seq[ExprCode], nullBitsTerm: String,
    numBytesForNullBits: Int, isKey: Boolean): String = {
    // Move the offset at the end of num Null Bytes space, we will fill that space later
    // store the starting value of offset
    val startingOffsetTerm = ctx.freshName("startingOffset")
    val plaformClass = classOf[Platform].getName
    s"""
       long $startingOffsetTerm = $offsetTerm;
       // move current offset to end of null bits
       $offsetTerm += ${sizeForNullBits(numBytesForNullBits)};
       ${
      dataTypes.zip(varsToWrite).zipWithIndex.map {
      case ((dt, expr), i) =>
        val variable = expr.value
        val writingCode = dt match {
          case x: AtomicType =>
            val snippet = typeOf(x.tag) match {
              case t if t =:= typeOf[Byte] => s"""
                   $plaformClass.putByte($baseObjectTerm, $offsetTerm,
                   $variable); \n
                   $offsetTerm += ${dt.defaultSize};\n
                """.stripMargin
              case t if t =:= typeOf[Short] =>
                s"""
              $plaformClass.putShort($baseObjectTerm,
              $offsetTerm, $variable);\n
              $offsetTerm += ${dt.defaultSize};\n
              """.stripMargin
              case t if t =:= typeOf[Int] =>
                s"""
                  $plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable);\n
                 $offsetTerm += ${dt.defaultSize}; \n
                 """.stripMargin
              case t if t =:= typeOf[Long] => s"""
               $plaformClass.putLong($baseObjectTerm, $offsetTerm, $variable);\n
               $offsetTerm += ${dt.defaultSize};\n
               """
              case t if t =:= typeOf[Float] => s"""
              $plaformClass.putFloat($baseObjectTerm, $offsetTerm, $variable);\n
               $offsetTerm += ${dt.defaultSize};\n
               """.stripMargin
              case t if t =:= typeOf[Double] => s"""
              $plaformClass.putDouble($baseObjectTerm, $offsetTerm, $variable);\n
               $offsetTerm += ${dt.defaultSize};\n
               """.stripMargin
              case t if t =:= typeOf[Decimal] =>
                throw new UnsupportedOperationException("implement decimal")
              case t if t =:= typeOf[UTF8String] => s"""
              $plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable.numBytes());\n
               $offsetTerm += 4;\n
               $variable.writeToMemory($baseObjectTerm, $offsetTerm);\n
               $offsetTerm += $variable.numBytes();\n
               """
              case _ => throw new UnsupportedOperationException("unknown type " + dt)
            }
            snippet

          case _ => throw new UnsupportedOperationException("unknown type " + dt)
        }

        // Now do the actual writing based on whether the variable is null or not
        val castTerm = getNullBitsCastTerm(numBytesForNullBits)
        val nullVar = expr.isNull
        if (numBytesForNullBits > 8) {
              val remainder = i % 8
              val index = i / 8

              if (nullVar.isEmpty || nullVar == "false") {
                s"""
                  $nullBitsTerm[$index] |= (byte)((0x01 << $remainder)); \n
                  $writingCode \n
                """.stripMargin
              } else {
                s"""
                  if (!$nullVar) {
                    $nullBitsTerm[$index] |= (byte)((0x01 << $remainder)); \n
                    $writingCode \n
                  }${ if (!isKey) {
                    s"""
                       else {
                         $offsetTerm += ${dt.defaultSize};\n
                       }
                    """.stripMargin
                  } else ""
                }
                """.stripMargin
              }
            }
            else {

              if (nullVar.isEmpty || nullVar == "false") {
                s"""
                  $nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $i)); \n
                  $writingCode \n
                """.stripMargin
              } else {
                s"""
                  if (!$nullVar) {
                    $nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $i)); \n
                    $writingCode \n
                  }${
                  if (!isKey) {
                    s"""
                       else {
                         $offsetTerm += ${dt.defaultSize};\n
                       }
                    """.stripMargin
                  } else ""
                }
                """.stripMargin
              }
          }

    }.mkString("\n")}
     // now write the nullBitsTerm
     ${writeNullBitsAt(baseObjectTerm, startingOffsetTerm, nullBitsTerm,
      numBytesForNullBits)}
     """.stripMargin

  }

  def generateKeyBytesHolderCode(numKeyBytesVar: String, numValueBytesVar: String,
    keyBytesHolderVar: String, keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType], valueInitVars: Seq[ExprCode],
    baseKeyObject: String, baseKeyoffsetTerm: String): String = {

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
        // first write key data
        ${writeKeyOrValue(baseKeyObject, currentOffset, keysDataType, keyVars,
      nullKeysBitsetTerm, numBytesForNullKeyBits, true)}
        //write value data
       ${ writeKeyOrValue(baseKeyObject, currentOffset, aggregatesDataType, valueInitVars,
      nullAggsBitsetTerm, numBytesForNullAggBits, false)}
    """
  }

  def writeNullBitsAt(baseObjectTerm: String, offsetToWriteTerm: String,
    nullBitsTerm: String, numBytesForNullBits: Int): String = {
    val plaformClass = classOf[Platform].getName
    if (numBytesForNullBits == 1) {
      s"\n $plaformClass.putByte($baseObjectTerm," +
        s" $offsetToWriteTerm, $nullBitsTerm);\n"
    } else if (numBytesForNullBits == 2) {
      s"\n $plaformClass.putShort($baseObjectTerm," +
        s" $offsetToWriteTerm, $nullBitsTerm);\n"
    } else if (numBytesForNullBits <= 4) {
      s"\n $plaformClass.putInt($baseObjectTerm," +
        s" $offsetToWriteTerm, $nullBitsTerm);\n"
    } else if (numBytesForNullBits <= 8) {
      s"$plaformClass.putLong($baseObjectTerm, $offsetToWriteTerm," +
        s"$nullBitsTerm);\n"
    } else {
      s"\n $plaformClass.copyMemory($nullBitsTerm, ${Platform.BYTE_ARRAY_OFFSET}," +
        s" $baseObjectTerm, $offsetToWriteTerm, $numBytesForNullBits);\n"
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
      }.mkString(" + ") + s" + ${sizeForNullBits(numBytesForNullKeyBits)}; \n"
  }

  def sizeForNullBits(numBytesForNullBits: Int): Int =
    if (numBytesForNullBits < 3 || numBytesForNullBits > 8) {
      numBytesForNullBits
    } else if (numBytesForNullBits <= 4) {
      4
    } else {
      8
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