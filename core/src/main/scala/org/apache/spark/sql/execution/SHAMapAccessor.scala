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
package org.apache.spark.sql.execution

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._

import com.gemstone.gemfire.internal.shared.{BufferSizeLimitExceededException, ClientResolverUtils}
import io.snappydata.Property
import io.snappydata.collection.{ByteBufferData, ByteBufferHashMap, SHAMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoding, StringDictionary}
import org.apache.spark.sql.types.{BinaryType, StringType, _}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

case class SHAMapAccessor(@transient session: SnappySession,
  @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
  @transient valueExprs: Seq[Expression], classPrefix: String,
  hashMapTerm: String, overflowHashMapsTerm: String, keyValSize: Int,
  valueOffsetTerm: String, numKeyBytesTerm: String, numValueBytes: Int,
  currentOffSetForMapLookupUpdt: String, valueDataTerm: String,
  vdBaseObjectTerm: String, vdBaseOffsetTerm: String,
  nullKeysBitsetTerm: String, numBytesForNullKeyBits: Int,
  allocatorTerm: String, numBytesForNullAggBits: Int,
  nullAggsBitsetTerm: String, sizeAndNumNotNullFuncForStringArr: String,
  keyBytesHolderVarTerm: String, baseKeyObject: String,
  baseKeyHolderOffset: String, keyExistedTerm: String,
  skipLenForAttribIndex: Int, codeForLenOfSkippedTerm: String,
  valueDataCapacityTerm: String, storedAggNullBitsTerm: Option[String],
  storedKeyNullBitsTerm: Option[String],
  aggregateBufferVars: Seq[String], keyHolderCapacityTerm: String,
  shaMapClassName: String, useCustomHashMap: Boolean,
  previousSingleKey_Position_LenTerm: Option[(String, String, String)],
  codeSplitFuncParamsSize: Int, codeSplitThresholdSize: Int, splitAggCode: Boolean,
  splitGroupByKeyCode: Boolean)
  extends CodegenSupport {
  val unsafeArrayClass = classOf[UnsafeArrayData].getName
  val unsafeRowClass = classOf[UnsafeRow].getName
  val platformClass = classOf[Platform].getName
  val decimalClass = classOf[Decimal].getName
  val bigDecimalObjectClass = s"$decimalClass$$.MODULE$$"
  val typeUtiltiesObjectClass =
    s"${org.apache.spark.sql.types.TypeUtilities.getClass.getName}.MODULE$$"
  val bigDecimalClass = classOf[java.math.BigDecimal].getName
  val bigIntegerClass = classOf[java.math.BigInteger].getName
  val byteBufferClass = classOf[ByteBuffer].getName
  val unsafeClass = classOf[UnsafeRow].getName
  val bbDataClass = classOf[ByteBufferData].getName
  val byteArrayEqualsClass = classOf[ByteArrayMethods].getName

   private def getBufferVars(dataTypes: Seq[DataType], varNames: Seq[String],
    currentValueOffsetTerm: String, isKey: Boolean, nullBitTerm: String,
    numBytesForNullBits: Int, skipNullBitsCode: Boolean, nestingLevel: Int = 0):
  Seq[ExprCode] = {
    val castTerm = SHAMapAccessor.getNullBitsCastTerm(numBytesForNullBits)
    dataTypes.zip(varNames).zipWithIndex.map { case ((dt, varName), i) =>
      val nullVar = if (isKey) {
        if (nestingLevel == 0 && skipNullBitsCode) {
          "false"
        } else {
          ctx.freshName("isNull")
        }
      } else s"$varName${SHAMapAccessor.nullVarSuffix}"
      // if it is aggregate value buffer do not declare null var as
      // they are already declared at start
      // also aggregate vars cannot be nested in any case.
      val booleanStr = if (isKey) "boolean" else ""
      val nullVarCode = if (skipNullBitsCode) {
        ""
      } else {
        s"""$booleanStr $nullVar = ${SHAMapAccessor.getExpressionForNullEvalFromMask(i,
          numBytesForNullBits, nullBitTerm)};""".stripMargin
      }

      val evaluationCode = readVarPartialFunction(currentValueOffsetTerm, nestingLevel, i,
        varName, dt).trim

      val exprCode = if (skipNullBitsCode) {
        evaluationCode
      } else {
        s"""$nullVarCode
           if (!$nullVar) {
             $evaluationCode
           }${
          if (!isKey) {
            s"""
             else {
               ${SHAMapAccessor.getOffsetIncrementCodeForNullAgg(currentValueOffsetTerm, dt)}
             } """.stripMargin
          } else {
            ""
          }
        }""".stripMargin
      }
      ExprCode(exprCode, nullVar, varName)
    }
  }


  def readVarsFromBBMap(dataTypes: Seq[DataType], varNames: Seq[String],
    currentValueOffsetTerm: String, isKey: Boolean, nullBitTerm: String,
    numBytesForNullBits: Int, skipNullBitsCode: Boolean,
    splitCode: Boolean, useTheseNullVarsForAggBuffer: Option[Seq[String]],
    castVarToDataType: Boolean = false):
  (Option[String], Seq[ExprCode]) = {
    if (splitCode) {
      val nullBitsCastTerm = if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullBits)) {
        "byte[]"
      } else SHAMapAccessor.getNullBitsCastTerm(numBytesForNullBits)

      val baseMethod = "readFromBBMap"
      val stateArray = ctx.freshName("stateArray")
      val localVar = ctx.freshName("tempVar")
      val localIndex = ctx.freshName("index")
      val localNullBool = ctx.freshName("isNull")
      val remainder = ctx.freshName("remainder")
      val pos = ctx.freshName("pos")
      val nullBoolEvalCode = if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullBits)) {
        s"""
           |int $remainder = $localIndex % 8;
           |int $pos = $localIndex / 8;
           |$localNullBool = ($nullBitTerm[$pos] & (0x01 << $remainder)) != 0;
         """.stripMargin
      } else {
        s"""$localNullBool = ($nullBitTerm & ((($nullBitsCastTerm)0x01) << $localIndex)) != 0;"""
      }
      val nullCode = if (skipNullBitsCode) "" else nullBoolEvalCode
      // this can handle only those aggs & keys whose index is not equal to skip length index
      val funcMapping = dataTypes.distinct.map(dt => {
        val methodName = ctx.freshName(baseMethod)

        val partMethodBody = readVarPartialFunction(currentValueOffsetTerm, 0, -1,
          localVar, dt).trim
        val varDataType = ctx.javaType(dt);
        ctx.addNewFunction(methodName,
          s"""
             |private Object $methodName(Object[] $stateArray,
             | ${ if (skipNullBitsCode) "" else s"$nullBitsCastTerm $nullBitTerm,"}
             | int $localIndex, Object $vdBaseObjectTerm) {
             | long $currentValueOffsetTerm = (Long) $stateArray[0];
             | $varDataType $localVar = ${ctx.defaultValue(dt)};
             | boolean $localNullBool = false;
             | $nullCode
             | if ($localNullBool) {
             | ${if (isKey) "" else SHAMapAccessor.getOffsetIncrementCodeForNullAgg(currentValueOffsetTerm, dt)}
             | } else {
             |   $partMethodBody
             | }
             | $stateArray[0] = $currentValueOffsetTerm;
             | $stateArray[1] = $localNullBool;
             | return $localVar;
             |}
         """.stripMargin)
        dt -> methodName
      }).toMap


      val exprCodes = dataTypes.zip(varNames).zipWithIndex.map {
        case ((dt, varName), index) => {
          val nullVarName = if (skipNullBitsCode) {
            "false"
          } else if (isKey) {
            ctx.freshName("isNullKey")
          } else {
            useTheseNullVarsForAggBuffer.map(nullVars => nullVars(index)).
              getOrElse(s"$varName${SHAMapAccessor.nullVarSuffix}")
          }

          if (isKey && ((index == this.skipLenForAttribIndex) || dt.isInstanceOf[StructType])) {
            val code =
              s"""
                 |boolean $localNullBool = false;
                 |int $localIndex = $index;
                 |$nullCode
                 |if (!$localNullBool) {
                 |  $currentValueOffsetTerm = (Long)$stateArray[0];
                 |  ${readVarPartialFunction(currentValueOffsetTerm, 0, index, varName, dt).trim}
                 |  $stateArray[0] = $currentValueOffsetTerm;
                 |}
                 |${if (skipNullBitsCode) "" else s"boolean $nullVarName = $localNullBool;"}
               """.stripMargin
            ExprCode(code, nullVarName, varName)
          } else {
            val varDataType = ctx.javaType(dt);
            val funcName = funcMapping.get(dt).get


            val nullDeclare = if (isKey) "boolean" else ""

            val nullVarCode = if (skipNullBitsCode) ""
            else s"$nullDeclare $nullVarName = (Boolean)$stateArray[1];"

            val code = s"""
              |$varName = (${SHAMapAccessor.getObjectTypeForPrimitiveType(varDataType)})$funcName(
              |$stateArray, ${if (skipNullBitsCode) "" else s"$nullBitTerm, "} $index,
              |$vdBaseObjectTerm);
              |$nullVarCode
            """.stripMargin
            val processedVarName = if (castVarToDataType) {
              s"((${SHAMapAccessor.getObjectTypeForPrimitiveType(varDataType)})$varName)"
            } else varName
            ExprCode(code, nullVarName, processedVarName)
          }
        }
      }

      ctx.addMutableState("Object[]", stateArray, s"$stateArray = new Object[2];")
      Some(stateArray) -> exprCodes

    } else {
      (None, getBufferVars(dataTypes, varNames, currentValueOffsetTerm, isKey, nullBitTerm,
        numBytesForNullBits, skipNullBitsCode, 0))
    }
  }

  def readNullBitsCode(currentValueOffsetTerm: String, nullBitsetTerm: String,
    numBytesForNullBits: Int): String = {
    val plaformClass = classOf[Platform].getName
    if (numBytesForNullBits == 0) {
      ""
    } else if (numBytesForNullBits == 1) {
      s"""$nullBitsetTerm = $plaformClass.getByte($vdBaseObjectTerm, $currentValueOffsetTerm);
         |$currentValueOffsetTerm += 1;""".stripMargin
    } else if (numBytesForNullBits == 2) {
      s"""$nullBitsetTerm = $plaformClass.getShort($vdBaseObjectTerm, $currentValueOffsetTerm);
         |$currentValueOffsetTerm += 2;""".stripMargin
    } else if (numBytesForNullBits <= 4) {
      s"""|$nullBitsetTerm = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
          |$currentValueOffsetTerm += 4;""".stripMargin
    } else if (numBytesForNullBits <= 8) {
      s"""$nullBitsetTerm = $plaformClass.getLong($vdBaseObjectTerm, $currentValueOffsetTerm);
         |$currentValueOffsetTerm += 8;""".stripMargin
    } else {
      s"""$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm, $nullBitsetTerm,
         |${Platform.BYTE_ARRAY_OFFSET}, $numBytesForNullBits);
         |$currentValueOffsetTerm += $numBytesForNullBits;""".stripMargin
    }
  }

  private val writeVarPartialFunction: PartialFunction[(String, String, String, Int,
    Boolean, DataType), String] = {
    case (baseObjectTerm, offsetTerm, variable, nestingLevel, skipLength, dt: AtomicType) =>
      val snippet = typeOf(dt.tag) match {
        case t if t =:= typeOf[Boolean] =>
          s"""$platformClass.putBoolean($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                """.stripMargin
        case t if t =:= typeOf[Byte] =>
          s"""$platformClass.putByte($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                """.stripMargin
        case t if t =:= typeOf[Array[Byte]] =>
          s"""$platformClass.putInt($baseObjectTerm, $offsetTerm, $variable.length);
             |$offsetTerm += 4;
             |$platformClass.copyMemory($variable, ${Platform.BYTE_ARRAY_OFFSET},
             |$baseObjectTerm, $offsetTerm, $variable.length);
             |$offsetTerm += $variable.length;
                """.stripMargin
        case t if t =:= typeOf[Short] =>
          s"""$platformClass.putShort($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
        case t if t =:= typeOf[Int] =>
          s"""
                  $platformClass.putInt($baseObjectTerm, $offsetTerm, $variable);
                 $offsetTerm += ${dt.defaultSize};
                 """.stripMargin
        case t if t =:= typeOf[Long] =>
          s"""$platformClass.putLong($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
        case t if t =:= typeOf[Float] =>
          s"""$platformClass.putFloat($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
        case t if t =:= typeOf[Double] =>
          s"""$platformClass.putDouble($baseObjectTerm, $offsetTerm, $variable);
             |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
        case t if t =:= typeOf[Decimal] =>
          val tempBigDecArrayTerm = ctx.freshName("tempBigDecArray")
          s"""
             |if (${dt.asInstanceOf[DecimalType].precision} != $variable.precision() ||
             | ${dt.asInstanceOf[DecimalType].scale} != $variable.scale()) {
             |  if (!$variable.changePrecision(${dt.asInstanceOf[DecimalType].precision},
             |  ${dt.asInstanceOf[DecimalType].scale})) {
             |    throw new java.lang.IllegalStateException("unable to change precision");
             |  }
             |}
                   """.stripMargin +
            (if (dt.asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS) {
              s"""$platformClass.putLong($baseObjectTerm, $offsetTerm,
                 | $variable.toUnscaledLong());
                     """.stripMargin
            } else {
              s"""byte[] $tempBigDecArrayTerm = $variable.toJavaBigDecimal().
                 |unscaledValue().toByteArray();
                 |assert ($tempBigDecArrayTerm.length <= 16);
                 |$platformClass.putLong($baseObjectTerm, $offsetTerm,0);
                 |$platformClass.putLong($baseObjectTerm, $offsetTerm + 8,0);
                 |$platformClass.copyMemory($tempBigDecArrayTerm,
                 |$platformClass.BYTE_ARRAY_OFFSET, $baseObjectTerm, $offsetTerm +
                 |${dt.asInstanceOf[DecimalType].defaultSize} - $tempBigDecArrayTerm.length ,
                 | $tempBigDecArrayTerm.length);
                    """.stripMargin
            }) +
            s"""
               |$offsetTerm += ${dt.defaultSize};
                     """.stripMargin
        case t if t =:= typeOf[UTF8String] =>
          val tempLenTerm = ctx.freshName("tempLen")

          val lengthWritingPart = if (!skipLength) {
            s"""$platformClass.putInt($baseObjectTerm, $offsetTerm, $tempLenTerm);
               |$offsetTerm += 4;""".stripMargin
          } else ""

          s"""int $tempLenTerm = $variable.numBytes();
             |$lengthWritingPart
             |$variable.writeToMemory($baseObjectTerm, $offsetTerm);
             |$offsetTerm += $tempLenTerm;
               """.stripMargin
        case _ => throw new UnsupportedOperationException("unknown type " + dt)
      }
      snippet
    case (baseObjectTerm, offsetTerm, variable, nestingLevel, i, st: StructType) =>
      val (childExprCodes, childDataTypes) = getExplodedExprCodeAndDataTypeForStruct(variable,
        st, nestingLevel)
      val newNullBitTerm = SHAMapAccessor.generateNullKeysBitTermForStruct(variable)
      val newNumBytesForNullBits = SHAMapAccessor.
        calculateNumberOfBytesForNullBits(st.length)
      val explodeStructSnipet =
        s"""$platformClass.putBoolean($baseObjectTerm, $offsetTerm, true);
           |$offsetTerm += 1;
           |${
          writeKeyOrValue(baseObjectTerm, offsetTerm, childDataTypes, childExprCodes,
            newNullBitTerm, newNumBytesForNullBits, true, false,
            nestingLevel + 1)
        }
                 """.stripMargin
      val unexplodedStructSnippet =
        s"""|$platformClass.putBoolean($baseObjectTerm, $offsetTerm, false);
            |$offsetTerm += 1;
            |$platformClass.putInt($baseObjectTerm, $offsetTerm,
            |(($unsafeRowClass)$variable).getSizeInBytes());
            |$offsetTerm += 4;
            |(($unsafeRowClass)$variable).writeToMemory($baseObjectTerm, $offsetTerm);
            |$offsetTerm += (($unsafeRowClass)$variable).getSizeInBytes();
                 """.stripMargin
      if (alwaysExplode) {
        explodeStructSnipet
      } else {
        s"""if (!($variable instanceof $unsafeRowClass)) {
                  $explodeStructSnipet
                } else {
                  $unexplodedStructSnippet
                }
               """.stripMargin
      }


    case (baseObjectTerm, offsetTerm, variable, nestingLevel, skipLength,
    at@ArrayType(elementType, containsNull)) =>
      val varWidthNullBitStartPos = ctx.freshName("nullBitBeginPos")
      val varWidthNumNullBytes = ctx.freshName("numNullBytes")
      val varWidthNullBits = ctx.freshName("nullBits")
      val arrElement = ctx.freshName("arrElement")
      val tempObj = ctx.freshName("temp")
      val array = ctx.freshName("array")
      val counter = ctx.freshName("counter")
      val remainder = ctx.freshName("remainder")
      val arrIndex = ctx.freshName("arrIndex")
      val dataTypeAsJson = elementType.json
      val strippedQuotesJson = dataTypeAsJson.substring(1, dataTypeAsJson.length - 1)
      val dataType = ctx.freshName("dataType")
      val dataTypeClass = classOf[DataType].getName
      val elementWitingCode = writeKeyOrValue(baseObjectTerm, offsetTerm, Seq(elementType),
        Seq(ExprCode("", "false", arrElement)), "", -1,
        true, true, nestingLevel)
      val explodeArraySnippet =
        s"""|$platformClass.putBoolean($baseObjectTerm, $offsetTerm, true);
            |$offsetTerm += 1;
            |$platformClass.putInt($baseObjectTerm, $offsetTerm, $variable.numElements());
            |$offsetTerm += 4;
            |long $varWidthNullBitStartPos = $offsetTerm;
            |int $varWidthNumNullBytes = $variable.numElements() / 8 +
            |($variable.numElements() % 8 > 0 ? 1 : 0);
            |byte[] $varWidthNullBits = null;
            |${ if (containsNull) {
          s"""
             |$varWidthNullBits = new byte[$varWidthNumNullBytes];
             |$offsetTerm += $varWidthNumNullBytes;
                          """.stripMargin
        } else ""
        }

            |$dataTypeClass $dataType = $dataTypeClass$$.MODULE$$.
            |fromJson("\\"$strippedQuotesJson\\"");
            |for( int $counter = 0; $counter < $variable.numElements(); ++$counter) {
            |int $remainder = $counter % 8;
            |int $arrIndex = $counter / 8;
            |if ($variable.isNullAt($counter)) {
            |if ($containsNull) {
            |$varWidthNullBits[$arrIndex] |= (byte)((0x01 << $remainder));
            |} else {
            |  throw new IllegalStateException("Not null Array element contains null");
            |}
            |} else {
            |${ctx.javaType(elementType)} $arrElement =
            |(${ctx.boxedType(elementType)}) $variable.get($counter, $dataType);
            |$elementWitingCode
            |}
            |}
            |${ if (containsNull ) {
          s"""
             |$platformClass.copyMemory($varWidthNullBits,
             |${Platform.BYTE_ARRAY_OFFSET},
             |$baseObjectTerm, $varWidthNullBitStartPos, $varWidthNumNullBytes);
                         """.stripMargin
        } else ""
        }
                """.stripMargin
      val unexplodedArraySnippet =
        s"""$platformClass.putBoolean($baseObjectTerm, $offsetTerm, false);
           |$offsetTerm += 1;
           |$platformClass.putInt($baseObjectTerm, $offsetTerm,
           |(($unsafeArrayClass)$variable).getSizeInBytes());
           |$offsetTerm += 4;
           |(($unsafeArrayClass)$variable).writeToMemory($baseObjectTerm, $offsetTerm);
           |$offsetTerm += (($unsafeArrayClass)$variable).getSizeInBytes();
                 """.stripMargin

      if (alwaysExplode) {
        explodeArraySnippet
      } else {
        s"""if (!($variable instanceof $unsafeArrayClass)) {
                       $explodeArraySnippet
           |} else {
                        $unexplodedArraySnippet
           |}
               """.stripMargin
      }
    case (baseObjectTerm, offsetTerm, variable, nestingLevel, skipLength, dt) =>
      throw new UnsupportedOperationException("unknown type " + dt)
  }

  private val readVarPartialFunction: PartialFunction[(String, Int, Int, String,
    DataType), String] = {
    case (currentValueOffsetTerm: String, nestingLevel: Int, i: Int, varName: String,
    StringType) =>
    val len = ctx.freshName("len")
    val readLenCode = if (nestingLevel == 0 && i == skipLenForAttribIndex) {
      s"int $len = $codeForLenOfSkippedTerm"
    } else {
      s"""int $len = $platformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
         |$currentValueOffsetTerm += 4;
               """
    }
    s"""$readLenCode
       | $varName = ${classOf[UTF8String].getName}.fromAddress($vdBaseObjectTerm,
       | $currentValueOffsetTerm, $len);
       |$currentValueOffsetTerm += $len;
          """.stripMargin
    case (currentValueOffsetTerm: String, nestingLevel: Int, i: Int, varName: String, BinaryType) =>
      s"""$varName = new byte[$platformClass.getInt($vdBaseObjectTerm,
         | $currentValueOffsetTerm)];
         |$currentValueOffsetTerm += 4;
         |$platformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
         | $varName, ${Platform.BYTE_ARRAY_OFFSET}, $varName.length);
         | $currentValueOffsetTerm += $varName.length;
               """.stripMargin
    case (currentValueOffsetTerm: String, nestingLevel: Int, i: Int, varName: String,
    x: AtomicType) => {
      (typeOf(x.tag) match {
      case t if t =:= typeOf[Boolean] =>
      s"""$varName = $platformClass.getBoolean($vdBaseObjectTerm, $currentValueOffsetTerm);
              """
      case t if t =:= typeOf[Byte] =>
      s"""$varName = $platformClass.getByte($vdBaseObjectTerm, $currentValueOffsetTerm);
               """
      case t if t =:= typeOf[Short] =>
      s"""$varName = $platformClass.getShort($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
      case t if t =:= typeOf[Int] =>
      s"""$varName = $platformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
               """.stripMargin
      case t if t =:= typeOf[Long] =>
      s"""$varName = $platformClass.getLong($vdBaseObjectTerm,$currentValueOffsetTerm);
              """.stripMargin
      case t if t =:= typeOf[Float] =>
      s"""$varName = $platformClass.getFloat($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
      case t if t =:= typeOf[Double] =>
      s"""$varName = $platformClass.getDouble($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
      case t if t =:= typeOf[Decimal] =>
      if (x.asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS) {
      s"""$varName = $bigDecimalObjectClass.apply(
                            |$platformClass.getLong($vdBaseObjectTerm, $currentValueOffsetTerm),
                   ${x.asInstanceOf[DecimalType].precision},
                   ${x.asInstanceOf[DecimalType].scale});""".stripMargin
    } else {
      val tempByteArrayTerm = ctx.freshName("tempByteArray")

      val len = ctx.freshName("len")
      s"""
                            |byte[] $tempByteArrayTerm = new byte[${x.asInstanceOf[DecimalType].
      defaultSize}];
                            |$platformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
                            |$tempByteArrayTerm, ${Platform.BYTE_ARRAY_OFFSET} ,
                            | $tempByteArrayTerm.length);
                            |$varName = $bigDecimalObjectClass.apply(new $bigDecimalClass(
                            |new $bigIntegerClass($tempByteArrayTerm),
                            |${x.asInstanceOf[DecimalType].scale},
                            | $typeUtiltiesObjectClass.mathContextCache()[${
        x.asInstanceOf[DecimalType].precision - 1}]));
                   """.stripMargin
    }

      case _ => throw new UnsupportedOperationException("unknown type " + x)
    }) +
      s"""$currentValueOffsetTerm += ${x.defaultSize};"""
    }
    case  (currentValueOffsetTerm: String, nestingLevel: Int, i: Int, varName: String,
    ArrayType(elementType, containsNull)) =>
    val isExploded = ctx.freshName("isExplodedArray")
    val arraySize = ctx.freshName("arraySize")
    val holder = ctx.freshName("holder")
    val byteBufferClass = classOf[ByteBuffer].getName
    val unsafeArrayDataClass = classOf[UnsafeArrayData].getName
    val genericArrayDataClass = classOf[GenericArrayData].getName
    val objectArray = ctx.freshName("objArray")
    val objectClass = classOf[Object].getName
    val counter = ctx.freshName("counter")
    val readingCodeExprs = getBufferVars(Seq(elementType), Seq(s"$objectArray[$counter]"),
      currentValueOffsetTerm, true, "", -1,
      true, nestingLevel)
    val varWidthNumNullBytes = ctx.freshName("numNullBytes")
    val varWidthNullBits = ctx.freshName("nullBits")
    val remainder = ctx.freshName("remainder")
    val indx = ctx.freshName("indx")

    s"""boolean $isExploded = $platformClass.getBoolean($vdBaseObjectTerm,
       |$currentValueOffsetTerm);
       |++$currentValueOffsetTerm;
       |if ($isExploded) {
       |int $arraySize = $platformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
       |$currentValueOffsetTerm += 4;
       |$objectClass[] $objectArray = new $objectClass[$arraySize];
       |if ($containsNull) {
       |int $varWidthNumNullBytes = $arraySize/8 + ($arraySize % 8 > 0 ? 1 : 0);
       |byte[] $varWidthNullBits = new byte[$varWidthNumNullBytes];
       |$platformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
       | $varWidthNullBits, ${Platform.BYTE_ARRAY_OFFSET}, $varWidthNumNullBytes);
       |$currentValueOffsetTerm += $varWidthNumNullBytes;
       |for (int $counter = 0; $counter < $arraySize; ++$counter ) {
       |int $remainder = $counter % 8;
       |int $indx = $counter / 8;
       |if ( ($varWidthNullBits[$indx] & (0x01 << $remainder)) == 0) {
       |${readingCodeExprs.map(_.code).mkString("\n")}
       |}
       |}
       |} else {
       |for (int $counter = 0; $counter < $arraySize; ++$counter ) {
       |${readingCodeExprs.map(_.code).mkString("\n")}
       |}
       |}

               $varName = new $genericArrayDataClass($objectArray);
       |} else {
       |int $arraySize = $platformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
       |$currentValueOffsetTerm += 4;
       |$byteBufferClass $holder = $allocatorTerm.allocate($arraySize, "SHA");
       |$platformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
       |$allocatorTerm.baseObject($holder), $allocatorTerm.baseOffset($holder),$arraySize);
       |$currentValueOffsetTerm += $arraySize;
       |$varName = new $unsafeArrayDataClass();
       |(($unsafeArrayDataClass)$varName).pointTo($allocatorTerm.baseObject($holder),
       |$allocatorTerm.baseOffset($holder), $arraySize);
       |}""".stripMargin

    case (currentValueOffsetTerm: String, nestingLevel: Int, i: Int, varName: String,
    st: StructType) =>
      val objectArray = ctx.freshName("objectArray")
      val byteBufferClass = classOf[ByteBuffer].getName
      val currentOffset = ctx.freshName("currentOffset")
      val nullBitSetTermForStruct = SHAMapAccessor.generateNullKeysBitTermForStruct(
        varName)
      val numNullKeyBytesForStruct = SHAMapAccessor.calculateNumberOfBytesForNullBits(st.length)
      val genericInternalRowClass = classOf[GenericInternalRow].getName
      val internalRowClass = classOf[InternalRow].getName
      val objectClass = classOf[Object].getName
      val keyVarNamesWithStructFlags = st.zipWithIndex.map { case (sf, indx) =>
        sf.dataType match {
          case _: StructType => SHAMapAccessor.generateVarNameForStructField(varName,
            nestingLevel, indx) -> true
          case _ => s"$objectArray[$indx]" -> false
        }
      }

      val isExploded = ctx.freshName("isUnsafeRow")
      val unsafeRowLength = ctx.freshName("unsafeRowLength")
      val holder = ctx.freshName("holder")
      // ${SHAMapAccessor.initNullBitsetCode(newNullBitSetTerm, newNumNullKeyBytes)}
      s"""boolean $isExploded = $platformClass.getBoolean($vdBaseObjectTerm,
         |$currentValueOffsetTerm);
         |++$currentValueOffsetTerm;
         |if ($isExploded) {
           |${readNullBitsCode(currentValueOffsetTerm, nullBitSetTermForStruct,
            numNullKeyBytesForStruct)
           }
         |$objectClass[] $objectArray = new $objectClass[${st.length}];
         |$varName = new $genericInternalRowClass($objectArray);
             // declare child struct variables
         |${
        keyVarNamesWithStructFlags.filter(_._2).map {
          case (name, _) => s"$internalRowClass $name = null;"
        }.mkString("\n")
      }
             ${
        getBufferVars(st.map(_.dataType), keyVarNamesWithStructFlags.unzip._1,
          currentValueOffsetTerm, true, nullBitSetTermForStruct,
          numNullKeyBytesForStruct, false, nestingLevel + 1).
          map(_.code).mkString("\n")
      }
            //add child Internal Rows to parent struct's object array
            ${
        keyVarNamesWithStructFlags.zipWithIndex.map { case ((name, isStruct), indx) =>
          if (isStruct) {
            s"$objectArray[$indx] = $name;"
          } else {
            ""
          }
        }.mkString("\n")
      }
          }
         |else {
         |int $unsafeRowLength = $platformClass.getInt($vdBaseObjectTerm,
         | $currentValueOffsetTerm);
         |$currentValueOffsetTerm += 4;
         |$byteBufferClass $holder = $allocatorTerm.allocate($unsafeRowLength, "SHA");
         |$platformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
         |$allocatorTerm.baseObject($holder), $allocatorTerm.baseOffset($holder),
         |$unsafeRowLength);
         |$currentValueOffsetTerm += $unsafeRowLength;
         |$varName = new $unsafeClass(${st.length});
         |(($unsafeClass)$varName).pointTo($allocatorTerm.baseObject($holder),
         | $allocatorTerm.baseOffset($holder), $unsafeRowLength);
         |} """.stripMargin

  }

  private val alwaysExplode = Property.TestExplodeComplexDataTypeInSHA.
    get(session.sessionState.conf)
  private[this] val hashingClass = classOf[ClientResolverUtils].getName

  override def children: Seq[SparkPlan] = Nil
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("unexpected invocation")

  override def inputRDDs(): Seq[RDD[InternalRow]] = Nil

  override protected def doProduce(ctx: CodegenContext): String =
    throw new UnsupportedOperationException("unexpected invocation")

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
    row: ExprCode): String = {
    throw new UnsupportedOperationException("unexpected invocation")
  }






  def initKeyOrBufferVal(dataTypes: Seq[DataType], varNames: Seq[String]):
  String = dataTypes.zip(varNames).map { case (dt, varName) =>
    s"${ctx.javaType(dt)} $varName = ${ctx.defaultValue(dt)};"
  }.mkString("\n")

  def declareNullVarsForAggBuffer(varNames: Seq[String]): String =
    varNames.map(varName => s"boolean ${varName}${SHAMapAccessor.nullVarSuffix} = false;").
      mkString("\n")
  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(valueInitVars: Seq[ExprCode],
    valueInitCode: String, evaluatedInputCode: String, keyVars: Seq[ExprCode],
    keysDataType: Seq[DataType], aggregateDataTypes: Seq[DataType],
    dictionaryCode: Option[DictionaryCode], dictionaryArrayTerm: String,
    aggFuncDependentOnGroupByKey: Boolean): String = {
    val hashVar = Array(ctx.freshName("hash"))
    val tempValueData = ctx.freshName("tempValueData")
    val linkedListClass = classOf[java.util.LinkedList[SHAMap]].getName
    val exceptionName = classOf[BufferSizeLimitExceededException].getName
    val skipLookupTerm = ctx.freshName("skipLookUp")
    val insertDoneTerm = ctx.freshName("insertDone");
    val putBufferIfAbsentArgs = if (useCustomHashMap) {
      s"""${keyVars.head.value}, $numKeyBytesTerm, $numValueBytes + $numKeyBytesTerm, ${hashVar(0)},
         |${keyVars.head.isNull}""".stripMargin
    } else {
      s"""$baseKeyObject, $baseKeyHolderOffset, $numKeyBytesTerm, $numValueBytes + $numKeyBytesTerm,
         | ${hashVar(0)}""".stripMargin
    }

    val lookUpInsertCode =
      s"""
         |// insert or lookup
         |if($overflowHashMapsTerm == null) {
           |try {
             |$valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($putBufferIfAbsentArgs);
             |$keyExistedTerm = $valueOffsetTerm >= 0;
             |if (!$keyExistedTerm) {
               |$valueOffsetTerm = -1 * $valueOffsetTerm;
               |if (($valueOffsetTerm + $numValueBytes + $numKeyBytesTerm) >=
                 |$valueDataCapacityTerm) {
                 |//$valueDataTerm = $tempValueData;
                 |$valueDataTerm =  $hashMapTerm.getValueData();
                 |$vdBaseObjectTerm = $valueDataTerm.baseObject();
                 |$vdBaseOffsetTerm = $valueDataTerm.baseOffset();
                 |$valueDataCapacityTerm = $valueDataTerm.capacity();
               |}
             |}
           |} catch ($exceptionName bsle) {
             |$overflowHashMapsTerm = new $linkedListClass<$shaMapClassName>();
             |$overflowHashMapsTerm.add($hashMapTerm);
             |$hashMapTerm = new $shaMapClassName(
             |${Property.initialCapacityOfSHABBMap.get(session.sessionState.conf)}, $keyValSize,
             |${Property.ApproxMaxCapacityOfBBMap.get(session.sessionState.conf)});
             |$overflowHashMapsTerm.add($hashMapTerm);
             |$valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($putBufferIfAbsentArgs);
             |$valueOffsetTerm = -1 * $valueOffsetTerm;
             |$valueDataTerm =  $hashMapTerm.getValueData();
             |$vdBaseObjectTerm = $valueDataTerm.baseObject();
             |$vdBaseOffsetTerm = $valueDataTerm.baseOffset();
             |$keyExistedTerm = false;
           |}
         |} else {
           |boolean $insertDoneTerm = false;
           |for($shaMapClassName shaMap : $overflowHashMapsTerm ) {
             |try {
               |$valueOffsetTerm = shaMap.putBufferIfAbsent($putBufferIfAbsentArgs);
               |$keyExistedTerm = $valueOffsetTerm >= 0;
               |if (!$keyExistedTerm) {
                 |$valueOffsetTerm = -1 * $valueOffsetTerm;
               |}
               |$hashMapTerm = shaMap;
               |$valueDataTerm =  $hashMapTerm.getValueData();
               |$vdBaseObjectTerm = $valueDataTerm.baseObject();
               |$vdBaseOffsetTerm = $valueDataTerm.baseOffset();
               |$insertDoneTerm = true;
               |break;
             |} catch ($exceptionName bsle) {
               |//ignore
             |}
           |}
           |if (!$insertDoneTerm) {
             |$hashMapTerm = new $shaMapClassName(
              |${Property.initialCapacityOfSHABBMap.get(session.sessionState.conf)},
              |$keyValSize,
              | ${Property.ApproxMaxCapacityOfBBMap.get(session.sessionState.conf)});
             |$overflowHashMapsTerm.add($hashMapTerm);
             |$valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($putBufferIfAbsentArgs);
             |$valueOffsetTerm = -1 * $valueOffsetTerm;
             |$keyExistedTerm = false;
             |$valueDataTerm =  $hashMapTerm.getValueData();
             |$vdBaseObjectTerm = $valueDataTerm.baseObject();
             |$vdBaseOffsetTerm = $valueDataTerm.baseOffset();
           |}
         |}
         |// position the offset to start of aggregate value
         |$valueOffsetTerm += $numKeyBytesTerm + $vdBaseOffsetTerm;
       """.stripMargin

    val hashCodeCalcSnippet = generateHashCode(hashVar, keyVars, keysDataType)

    val hashCodeGenCode = if (splitAggCode) {
      val funcName = ctx.freshName("calcHashCode")
      ctx.addNewFunction(funcName,
        s"""
           |private int $funcName() {
              |$hashCodeCalcSnippet
              |return ${hashVar(0)};
           |}
             """.stripMargin)
      s"int ${hashVar(0)} = $funcName();"
    } else {
      hashCodeCalcSnippet
    }

    val keySizeCodeCalcSnippet = generateKeySizeCode(keyVars, keysDataType, numBytesForNullKeyBits)
    val keySizeGenCode = if (splitAggCode) {
      val funcName = ctx.freshName("calcKeySize")
      ctx.addNewFunction(funcName,
        s"""
           |private int $funcName() {
             |return $keySizeCodeCalcSnippet;
           |}
             """.stripMargin)
      s"$funcName()"
    } else {
      keySizeCodeCalcSnippet
    }

    val keysPrepCodeCode =
      s"""
         |// evaluate key vars
         |${evaluateVariables(keyVars)}
         |${keyVars.zip(keysDataType).filter(_._2 match {
        case x: StructType => true
        case _ => false
      }).map {
        case (exprCode, dt) => explodeStruct(exprCode.value, exprCode.isNull,
          dt.asInstanceOf[StructType])
      }.mkString("\n")
      }
         | // evaluate hash code of the lookup key
         |$hashCodeGenCode
         |// get key size code
         |$numKeyBytesTerm = $keySizeGenCode;
         |// prepare the key
         |${generateKeyBytesHolderCodeOrEmptyString(numKeyBytesTerm, numValueBytes,
        keyVars, keysDataType, aggregateDataTypes, valueInitVars)
      }
       """.stripMargin

    val lookUpInsertCodeWithSkip = previousSingleKey_Position_LenTerm.map {
      case (keyTerm, posTerm, lenTerm) =>
        if (SHAMapAccessor.isPrimitive(keysDataType.head)) {
          s"""
             |boolean $skipLookupTerm = false;
             |if ($posTerm != -1 && !${keyVars.head.isNull} && $keyTerm == ${keyVars.head.value}) {
             |$skipLookupTerm = true;
             |$valueOffsetTerm = $posTerm;
             |$keyExistedTerm = true;
             |}
           if (!$skipLookupTerm) {
             $lookUpInsertCode
             if (${keyVars.head.isNull}) {
               $posTerm = -1L;
             } else {
               $posTerm = $valueOffsetTerm;
               $keyTerm = ${keyVars.head.value};
             }
           }
         """.stripMargin
        } else if (dictionaryCode.isDefined) {
          s"""
             |${dictionaryCode.map(dictCode => s"int ${dictCode.dictionaryIndex.value} = -1;").get}
             |if ($dictionaryArrayTerm != null) {
               |${dictionaryCode.map(_.evaluateIndexCode()).get}
             |}
             |boolean $skipLookupTerm = false;
             |${if (aggFuncDependentOnGroupByKey) keysPrepCodeCode else ""}
             |if ($dictionaryArrayTerm != null && $overflowHashMapsTerm == null &&
             |${dictionaryCode.map(_.dictionaryIndex.value).get} <
             | ${dictionaryCode.map(_.dictionary.value).get}.size() &&
             |${dictionaryCode.map(_.dictionaryIndex.value).get} >= 0) {
               |$posTerm = $dictionaryArrayTerm[${dictionaryCode.map(_.dictionaryIndex.value).get}];
               |if ($posTerm > 0) {
                 |$skipLookupTerm = true;
                 |$valueOffsetTerm = $posTerm;
                 |$keyExistedTerm = true;
               |}
             |}
           if (!$skipLookupTerm) {
             ${if (!aggFuncDependentOnGroupByKey) keysPrepCodeCode else ""}
             $lookUpInsertCode
             if (!${keyVars.head.isNull} && $dictionaryArrayTerm != null) {
               $dictionaryArrayTerm[${dictionaryCode.map(_.dictionaryIndex.value).get}]
                = $valueOffsetTerm;
             }
           }
           """.stripMargin
        }
        else {
          val actualKeyLen = if (numBytesForNullKeyBits == 0) lenTerm else s"($lenTerm - 1)"
          val equalityCheck = s"""$actualKeyLen == ${keyVars.head.value}.numBytes() &&
               |$byteArrayEqualsClass.arrayEquals($vdBaseObjectTerm, $posTerm - $actualKeyLen,
               |${keyVars.head.value}.getBaseObject(), ${keyVars.head.value}.getBaseOffset(),
               | $actualKeyLen)
             """.stripMargin
          s"""
             |boolean $skipLookupTerm = false;
             |if ($posTerm != -1 && !${keyVars.head.isNull} && $keyTerm == ${hashVar(0)}
             | && $equalityCheck) {
             |$skipLookupTerm = true;
             |$valueOffsetTerm = $posTerm;
             |$keyExistedTerm = true;
             |}
           if (!$skipLookupTerm) {
             $lookUpInsertCode
             if (${keyVars.head.isNull}) {
               $posTerm = -1L;
             } else {
               $posTerm = $valueOffsetTerm;
               $keyTerm = ${hashVar(0)};
               $lenTerm = $numKeyBytesTerm;
             }
           }
         """.stripMargin
        }
    }.getOrElse(lookUpInsertCode)

    s"""
        |${SHAMapAccessor.resetNullBitsetCode(nullKeysBitsetTerm, numBytesForNullKeyBits)}
        |${SHAMapAccessor.resetNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggBits)}
          // evaluate input row vars
        |$evaluatedInputCode
        |${if (dictionaryCode.isEmpty) keysPrepCodeCode else ""}
        |long $valueOffsetTerm = 0;
        |boolean $keyExistedTerm = false;
        |$lookUpInsertCodeWithSkip
        |long $currentOffSetForMapLookupUpdt = $valueOffsetTerm;""".stripMargin
  }

  // handle arraydata , map , object
  def explodeStruct(structVarName: String, structNullVarName: String, structType: StructType,
    nestingLevel: Int = 0): String = {
    val unsafeRowClass = classOf[UnsafeRow].getName
    val explodedStructCode = structType.zipWithIndex.map { case (sf, index) =>
      (sf.dataType, index, SHAMapAccessor.generateExplodedStructFieldVars(structVarName,
        nestingLevel, index))
    }.map { case (dt, index, (varName, nullVarName)) =>
      val valueExtractCode = dt match {
        case x: AtomicType => typeOf(x.tag) match {
          case t if t =:= typeOf[Boolean] => s"$structVarName.getBoolean($index); \n"
          case t if t =:= typeOf[Byte] => s"$structVarName.getByte($index); \n"
          case t if t =:= typeOf[Short] => s"$structVarName.getShort($index); \n"
          case t if t =:= typeOf[Int] => s"$structVarName.getInt($index); \n"
          case t if t =:= typeOf[Long] => s"$structVarName.getLong($index); \n"
          case t if t =:= typeOf[Float] => s"$structVarName.getFloat$index); \n"
          case t if t =:= typeOf[Double] => s"$structVarName.getDouble($index); \n"
          case t if t =:= typeOf[Decimal] => s"$structVarName.getDecimal($index, " +
            s"${dt.asInstanceOf[DecimalType].precision}," +
            s"${dt.asInstanceOf[DecimalType].scale}); \n"
          case t if t =:= typeOf[UTF8String] => s"$structVarName.getUTF8String($index); \n"
          case _ => throw new UnsupportedOperationException("unknown type " + dt)
        }
        case BinaryType => s"$structVarName.getBinary($index); \n"
        case CalendarIntervalType => s"$structVarName.getInterval($index); \n"
        case st: StructType => s"$structVarName.getStruct($index, ${st.length}); \n"

        case _ => throw new UnsupportedOperationException("unknown type " + dt)
      }

      val snippet =
       s"""|boolean $nullVarName = $structNullVarName ||
           | (!$alwaysExplode && $structVarName instanceof $unsafeRowClass) ||
           | $structVarName.isNullAt($index);
           | ${ctx.javaType(dt)} $varName = ${ctx.defaultValue(dt)};
           | if ($alwaysExplode|| !($structVarName instanceof $unsafeRowClass)) {
               |if (!$nullVarName) {
                 |$varName = $valueExtractCode;
               |}
             |}
           """.stripMargin

      snippet + (dt match {
        case st: StructType => explodeStruct(varName, nullVarName,
          st, nestingLevel + 1)
        case _ => ""
      })
    }.mkString("\n")
    s"""
    ${
      SHAMapAccessor.initNullBitsetCode(
        SHAMapAccessor.generateNullKeysBitTermForStruct(structVarName),
        SHAMapAccessor.calculateNumberOfBytesForNullBits(structType.length))
    }
    $explodedStructCode
     """.stripMargin
  }

  def generateUpdate(bufferVars: Seq[ExprCode], aggBufferDataType: Seq[DataType]): String = {
    val plaformClass = classOf[Platform].getName
    val setStoredAggNullBitsTerm = storedAggNullBitsTerm.map(storedNullBit => {
      s"""// If key did not exist, make cachedAggBit -1 , so that the update will always write
      // the right state of agg bit , else it will be that stored Agg Bit will match the
      // after update aggBit, but will not reflect it in the HashMap bits
      if ($keyExistedTerm) {
        $storedNullBit = $nullAggsBitsetTerm;
      } else {
        $storedNullBit = -1;
      }
      """.stripMargin
    }).getOrElse("")

    s"""
       |$setStoredAggNullBitsTerm
      ${SHAMapAccessor.resetNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggBits)}
      ${writeKeyOrValue(vdBaseObjectTerm, currentOffSetForMapLookupUpdt,
        aggBufferDataType, bufferVars, nullAggsBitsetTerm, numBytesForNullAggBits,
        false, false)
      }
    """.stripMargin

  }

  def writeKeyOrValue(baseObjectTerm: String, offsetTerm: String,
    dataTypes: Seq[DataType], varsToWrite: Seq[ExprCode], nullBitsTerm: String,
    numBytesForNullBits: Int, isKey: Boolean, skipNullEvalCode: Boolean,
    nestingLevel: Int = 0): String = {
    // Move the offset at the end of num Null Bytes space, we will fill that space later
    // store the starting value of offset

    val startingOffsetTerm = ctx.freshName("startingOffset")
    val tempBigDecArrayTerm = ctx.freshName("tempBigDecArray")
    val storeNullBitStartOffsetAndRepositionOffset = if (skipNullEvalCode) {
      ""
    } else {
      s"""long $startingOffsetTerm = $offsetTerm;
         |// move current offset to end of null bits
         |$offsetTerm += ${SHAMapAccessor.sizeForNullBits(numBytesForNullBits)};""".stripMargin
    }

    val fieldWritingCode = if (dataTypes.size <= codeSplitThresholdSize || nestingLevel > 0) {
      dataTypes.zip(varsToWrite).zipWithIndex.map {
        case ((dt, expr), i) =>
          val variable = expr.value
          val writingCode = writeVarPartialFunction((baseObjectTerm, offsetTerm, variable,
            nestingLevel, nestingLevel == 0 && i == skipLenForAttribIndex , dt)).trim
          // Now do the actual writing based on whether the variable is null or not
          if (skipNullEvalCode) {
            writingCode
          } else {
            SHAMapAccessor.evaluateNullBitsAndEmbedWrite(numBytesForNullBits, expr,
              i, nullBitsTerm, offsetTerm, dt, isKey, writingCode)
          }

      }.mkString("\n")
    } else {
      val stateTransferArray = ctx.freshName("stateTransferArray")
      val (nullCastPrimitive, nullCastObj) = if (skipNullEvalCode) {
        "" -> ""
      } else {

        if (!SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullBits)) {
          val ct = SHAMapAccessor.getNullBitsCastTerm(numBytesForNullBits)
          ct -> SHAMapAccessor.getObjectTypeForPrimitiveType(ct)
        } else "byte[]" -> "byte[]"
      }
      val methodFound = (methodName: String, groupSeq: Seq[(ExprCode, DataType)],
        attributeStartIndex: Int, skipLengthCase: Boolean, nestingLevel: Int) => {
        val methodArgs = groupSeq.map(tup => {
          val exprCd = tup._1
          s"${exprCd.value}, ${if (exprCd.isNull.isEmpty) "false" else exprCd.isNull}"
        }).mkString("", ",", s", $stateTransferArray, $baseObjectTerm," +
          s" $attributeStartIndex")
        s"$methodName($methodArgs);"
      }

      val methodNotFound = (baseMethodName: String, paramGroupData: Seq[(ExprCode, DataType)],
        argGroupData: Seq[(ExprCode, DataType)], attributeStartIndex: Int,
        isSkipLengthCase: Boolean, nestingLevel: Int) => {
        val attributeIndexStartPrm = ctx.freshName("attributeIndexStart")
        val methodName = ctx.freshName(baseMethodName)
        val partBody = paramGroupData.zipWithIndex.map {
          case ((partKeyVars, partKeysDataType), innerIndex) => {
            val skipLengthForAttr = isSkipLengthCase && (
              attributeStartIndex + innerIndex ==
                this.skipLenForAttribIndex)
            val fieldWrCode = writeVarPartialFunction((baseObjectTerm, offsetTerm,
              partKeyVars.value, nestingLevel, skipLengthForAttr, partKeysDataType)).trim
            // Now do the actual writing based on whether the variable is null or not
            if (skipNullEvalCode) {
              fieldWrCode
            } else {
              s"""
                 |${
                SHAMapAccessor.evaluateNullBitsAndEmbedWrite(numBytesForNullBits,
                  partKeyVars, attributeIndexStartPrm, nullBitsTerm, offsetTerm,
                  partKeysDataType, isKey, fieldWrCode, ctx)
              }
                 | ++$attributeIndexStartPrm;
                   """.stripMargin

            }
          }
        }.mkString("\n")

        val methodBody =
          s"""
             |long $offsetTerm = (Long)$stateTransferArray[0];
             |${ if (!skipNullEvalCode) {
                   s"$nullCastPrimitive $nullBitsTerm = ($nullCastObj)$stateTransferArray[1];"
                 } else ""
              }
             |$partBody
             |$stateTransferArray[0] = $offsetTerm;
             |${if (!skipNullEvalCode) s"$stateTransferArray[1] = $nullBitsTerm;" else ""}
                 """.stripMargin

        val methodParams = paramGroupData.zipWithIndex.map {
          case ((exprCode, dt), innerIndex) => s"${ctx.javaType(dt)} ${exprCode.value}," +
            s" boolean ${exprCode.isNull}"
        }.mkString("", ",", s", Object[] $stateTransferArray, Object $baseObjectTerm," +
          s" int $attributeIndexStartPrm")
        ctx.addNewFunction(methodName,
          s"""
             |private void $methodName($methodParams) {
             |$methodBody
             |}
             """.
            stripMargin)
        val methodArgs = argGroupData.zipWithIndex.map {
          case ((exprCode, dt), innerIndex) => s"${exprCode.value}," +
            s" ${if (exprCode.isNull.isEmpty) "false" else exprCode.isNull}"
        }.mkString("", ",", s", $stateTransferArray, $baseObjectTerm, $attributeStartIndex")

        s"$methodName($methodArgs); \n" -> methodName
      }

      val funcFieldWritingCode = codeSplit(dataTypes, varsToWrite, "writeKeyOrValGroup",
        nestingLevel, methodFound, methodNotFound, "\n", "", "", isKey)

      val stateArraySize = if (skipNullEvalCode) 1 else 2
      s"""
         |Object[] $stateTransferArray = new Object[$stateArraySize];
         |$stateTransferArray[0] = $offsetTerm;
         |${if (!skipNullEvalCode) s"$stateTransferArray[1] = $nullBitsTerm;" else ""}
         |$funcFieldWritingCode
         |$offsetTerm = (Long)$stateTransferArray[0];
         |${if (!skipNullEvalCode) s"$nullBitsTerm = ($nullCastObj)$stateTransferArray[1];"
           else ""}

       """.stripMargin
    }

    val nullBitsWritingCode = if (!skipNullEvalCode) {
      val nullBitsWritingCode = writeNullBitsAt(baseObjectTerm, startingOffsetTerm,
        nullBitsTerm, numBytesForNullBits)
      if(isKey) {
        if (nestingLevel == 0) {
          storedKeyNullBitsTerm.map(storedBit =>
            s"""
               | if ($storedBit != $nullBitsTerm) {
               |   $nullBitsWritingCode
               |   $storedBit = $nullBitsTerm;
               | }
               """.stripMargin).getOrElse(nullBitsWritingCode)
        } else {
          nullBitsWritingCode
        }
      } else {
        storedAggNullBitsTerm.map(storedAggBit =>
          s"""
             | if ($storedAggBit != $nullAggsBitsetTerm) {
             |   $nullBitsWritingCode
             | }
         """.stripMargin
        ).getOrElse(nullBitsWritingCode)
      }
    } else ""

    s"""$storeNullBitStartOffsetAndRepositionOffset
       |$fieldWritingCode
        // now write the nullBitsTerm
       |$nullBitsWritingCode""".stripMargin

  }


  def codeSplit(dataTypes: Seq[DataType], vars: Seq[ExprCode], baseMethod: String,
    nestingLevel: Int,
    methodFound: (String, Seq[(ExprCode, DataType)], Int, Boolean, Int) => String,
    methodNotFound: (String, Seq[(ExprCode, DataType)], Seq[(ExprCode, DataType)], Int,
      Boolean, Int) => (String, String), separator: String, prefix: String,
    suffix: String, isGroupByKeys: Boolean): String = {
    val groupIter = vars.zip(dataTypes).grouped(codeSplitFuncParamsSize).
      zipWithIndex
    val functionMapping = scala.collection.mutable.Map[String, String]()
    groupIter.map { case (groupSeq, index) => {
      val paramTypesStr = groupSeq.map(_._2.toString).mkString(",")
      val isSkipLengthCase = isGroupByKeys &&
        this.skipLenForAttribIndex != -1 && nestingLevel == 0 &&
        this.skipLenForAttribIndex >= index * codeSplitFuncParamsSize &&
        this.skipLenForAttribIndex < (index + 1) * codeSplitFuncParamsSize

      val existingFunc = if (isSkipLengthCase) None else functionMapping.get(paramTypesStr)

      existingFunc match {
        case Some(funcName) => methodFound(funcName, groupSeq, index * codeSplitFuncParamsSize,
          isSkipLengthCase, nestingLevel)
        case None =>
          val paramGroupSeq: Seq[(ExprCode, DataType)] = groupSeq.map {
            case (exprCode, dataType) => ExprCode("", ctx.freshName("isNull"),
              ctx.freshName("value")) -> dataType
          }
          val (code, functionName) = methodNotFound(baseMethod, paramGroupSeq, groupSeq,
            index * codeSplitFuncParamsSize, isSkipLengthCase, nestingLevel)

          if (!isSkipLengthCase) {
            functionMapping += (paramTypesStr -> functionName)
          }
          code
      }
    }
    }.mkString(prefix, separator, suffix)
  }

  def generateKeyBytesHolderCodeOrEmptyString(numKeyBytesVar: String, numValueBytes: Int,
    keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType], valueInitVars: Seq[ExprCode]): String = {

    val byteBufferClass = classOf[ByteBuffer].getName
    val currentOffset = ctx.freshName("currentOffset")
    val plaformClass = classOf[Platform].getName
    if (useCustomHashMap) {
      ""
    } else {
      val writeKeySnippet = writeKeyOrValue(baseKeyObject, currentOffset, keysDataType, keyVars,
        nullKeysBitsetTerm, numBytesForNullKeyBits, true, numBytesForNullKeyBits == 0)

      val writeKeyCode = if (splitAggCode) {
        val funcName = ctx.freshName("writeKeys")
        val nullKeysBitsetParam = if (numBytesForNullKeyBits == 0) {
          ""
        } else if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullKeyBits)) {
          s", byte[] $nullKeysBitsetTerm"
        } else {
          s", ${SHAMapAccessor.getNullBitsCastTerm(numBytesForNullKeyBits)} $nullKeysBitsetTerm"
        }
        val paramStr = s"long $currentOffset, Object $baseKeyObject $nullKeysBitsetParam"
        ctx.addNewFunction(funcName,
          s"""
             |private long $funcName($paramStr) {
               |$writeKeySnippet
               |return $currentOffset;
             |}
             """.stripMargin)
        val argStr = s"$currentOffset, $baseKeyObject" +
          s" ${ if (numBytesForNullKeyBits == 0) "" else s", $nullKeysBitsetTerm"}"
        s"$currentOffset = $funcName($argStr);"
      } else writeKeySnippet

      s"""
        if ($keyBytesHolderVarTerm == null || $keyHolderCapacityTerm <
      $numKeyBytesVar + $numValueBytes) {
          //$keyBytesHolderVarTerm =
           //$allocatorTerm.allocate($numKeyBytesVar + $numValueBytes, "SHA");
          //$baseKeyObject = $allocatorTerm.baseObject($keyBytesHolderVarTerm);
          //$baseKeyHolderOffset = $allocatorTerm.baseOffset($keyBytesHolderVarTerm);
           $keyHolderCapacityTerm = $numKeyBytesVar + $numValueBytes;
           $keyBytesHolderVarTerm = $byteBufferClass.allocate($keyHolderCapacityTerm);
           $baseKeyObject = $keyBytesHolderVarTerm.array();
           $baseKeyHolderOffset = $plaformClass.BYTE_ARRAY_OFFSET;
           ${storedKeyNullBitsTerm.map(x => s"$x = -1;").getOrElse("")}
        }

        long $currentOffset = $baseKeyHolderOffset;
        // first write key data
        $writeKeyCode
    """.stripMargin
    }
  }

  def writeNullBitsAt(baseObjectTerm: String, offsetToWriteTerm: String,
    nullBitsTerm: String, numBytesForNullBits: Int): String = {
    val plaformClass = classOf[Platform].getName
    if (numBytesForNullBits == 0) {
      ""
    } else if (numBytesForNullBits == 1) {
      s"$plaformClass.putByte($baseObjectTerm, $offsetToWriteTerm, $nullBitsTerm);"
    } else if (numBytesForNullBits == 2) {
      s"$plaformClass.putShort($baseObjectTerm, $offsetToWriteTerm, $nullBitsTerm);"
    } else if (numBytesForNullBits <= 4) {
      s"$plaformClass.putInt($baseObjectTerm, $offsetToWriteTerm, $nullBitsTerm);"
    } else if (numBytesForNullBits <= 8) {
      s"$plaformClass.putLong($baseObjectTerm, $offsetToWriteTerm, $nullBitsTerm);"
    } else {
      s"$plaformClass.copyMemory($nullBitsTerm, ${Platform.BYTE_ARRAY_OFFSET}," +
        s" $baseObjectTerm, $offsetToWriteTerm, $numBytesForNullBits);"
    }
  }



  def generateKeySizeCode(keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    numBytesForNullBits: Int, nestingLevel: Int = 0): String = {
    val unsafeRowClass = classOf[UnsafeRow].getName
    val unsafeArrayDataClass = classOf[UnsafeArrayData].getName

    def generateLengthCode(partKeyVars: Seq[ExprCode], partKeysDataType: Seq[DataType],
      nestingLevel: Int, indexCorrector: Int): String = {
      partKeysDataType.zip(partKeyVars).zipWithIndex.map { case ((dt, expr), indx) =>
        val nullVar = expr.isNull
        val notNullSizeExpr = if (TypeUtilities.isFixedWidth(dt)) {
          dt.defaultSize.toString
        } else {
          dt match {
            case StringType =>
              val strPart = s"${expr.value}.numBytes()"
              if (nestingLevel == 0 && (indx + indexCorrector) == skipLenForAttribIndex) {
                strPart
              } else {
                s"($strPart + 4)"
              }
            case BinaryType => s"(${expr.value}.length + 4) "
            case st: StructType => val (childKeysVars, childDataTypes) =
              getExplodedExprCodeAndDataTypeForStruct(expr.value, st, nestingLevel)
              val explodedStructSizeCode = generateKeySizeCode(childKeysVars, childDataTypes,
                SHAMapAccessor.calculateNumberOfBytesForNullBits(st.length), nestingLevel + 1)
              val unexplodedStructSizeCode = s"(($unsafeRowClass) ${expr.value})." +
                s"getSizeInBytes() + 4"

              "1 + " + (if (alwaysExplode) {
                explodedStructSizeCode
              } else {
                s"""(${expr.value} instanceof $unsafeRowClass ? $unexplodedStructSizeCode
                   |: $explodedStructSizeCode)
            """.stripMargin
              }
                )

            case at@ArrayType(elementType, containsNull) =>
              // The array serialization format is following
              /**
               *           Boolean (exploded or not)
               *             |
               *        --------------------------------------
               *   False|                                     | true
               * 4 bytes for num bytes                     ----------
               * all bytes                         no null |           | may be null
               *                                    allowed            | 4 bytes for total elements
               *                                        |              + num bytes for null bit mask
               *                                     4 bytes for       + inidividual not null elements
               *                                     num elements
               *                                     + each element
               *                                     serialzied
               *
               */
              val (isFixedWidth, unitSize) = if (TypeUtilities.isFixedWidth(elementType)) {
                (true, dt.defaultSize)
              } else {
                (false, 0)
              }
              val snippetNullBitsSizeCode =
                s"""${expr.value}.numElements()/8 + (${expr.value}.numElements() % 8 > 0 ? 1 : 0)
              """.stripMargin

              val snippetNotNullFixedWidth = s"4 + ${expr.value}.numElements() * $unitSize"
              val snippetNotNullVarWidth =
                s"""4 + (int)($sizeAndNumNotNullFuncForStringArr(${expr.value}, true) >>> 32L)
               """.stripMargin
              val snippetNullVarWidth = s" $snippetNullBitsSizeCode + $snippetNotNullVarWidth"
              val snippetNullFixedWidth =
                s"""4 + $snippetNullBitsSizeCode +
                   |$unitSize * (int)($sizeAndNumNotNullFuncForStringArr(
                   |${expr.value}, false) & 0xffffffffL)
            """.stripMargin

              "( 1 + " + (if (alwaysExplode) {
                if (isFixedWidth) {
                  if (containsNull) {
                    snippetNullFixedWidth
                  } else {
                    snippetNotNullFixedWidth
                  }
                } else {
                  if (containsNull) {
                    snippetNullVarWidth
                  } else {
                    snippetNotNullVarWidth
                  }
                }
              } else {
                s"""(${expr.value} instanceof $unsafeArrayDataClass ?
                   |(($unsafeArrayDataClass) ${expr.value}).getSizeInBytes() + 4
                   |: ${ if (isFixedWidth) {
                  s"""$containsNull ? ($snippetNullFixedWidth)
                     |: ($snippetNotNullFixedWidth))
                       """.stripMargin
                } else {
                  s"""$containsNull ? ($snippetNullVarWidth)
                     |: ($snippetNotNullVarWidth))
                       """.stripMargin
                }
                }
             """.stripMargin
              }) + ")"

          }
        }
        if (nullVar.isEmpty || nullVar == "false") {
          notNullSizeExpr
        } else {
          s"($nullVar? 0 : $notNullSizeExpr)"
        }
      }.mkString(" + ")
    }

    (if (keysDataType.size <= codeSplitThresholdSize || nestingLevel > 0) {
      generateLengthCode(keyVars, keysDataType, nestingLevel, 0)
    } else {
      val methodFound = (methodName: String, groupSeq: Seq[(ExprCode, DataType)],
        attributeStartIndex: Int, skipLengthCase: Boolean, nestingLevel: Int) => {
        val methodArgs = groupSeq.map(tup => {
          val exprCd = tup._1
          s"${exprCd.value}, ${if (exprCd.isNull.isEmpty) "false" else exprCd.isNull}"
        }).mkString(",")
        s"$methodName($methodArgs)"
      }

      val methodNotFound = (baseMethodName: String, paramGroupSeq: Seq[(ExprCode, DataType)],
        argGroupSeq: Seq[(ExprCode, DataType)], attributeStartIndex: Int,
        isSkipLengthCase: Boolean, nestingLevel: Int) => {
        val (partKeyVars, partKeysDataType) = paramGroupSeq.unzip
        val methodBody = s"return ${
          generateLengthCode(partKeyVars,
            partKeysDataType, nestingLevel, attributeStartIndex)
        };"
        val methodParams = paramGroupSeq.map {
          case (exprCode, dt) => s"${ctx.javaType(dt)} ${exprCode.value}," +
            s" boolean ${exprCode.isNull}"
        }.mkString(",")
        val methodName = ctx.freshName(baseMethodName)
        ctx.addNewFunction(methodName,
          s"""
             |private int $methodName($methodParams) {
             |$methodBody
             |}
             """.
            stripMargin)
        val methodArgs = argGroupSeq.map {
          case (exprCode, dt) => s"${exprCode.value}," +
            s" ${if (exprCode.isNull.isEmpty) "false" else exprCode.isNull}"
        }.mkString(",")

        s"$methodName($methodArgs)" -> methodName
      }

      codeSplit(keysDataType, keyVars, "calculatePartLength",
        nestingLevel, methodFound, methodNotFound, " + ", "", "", true)

    }) + s" + ${SHAMapAccessor.sizeForNullBits(numBytesForNullBits)}"
  }


  def getExplodedExprCodeAndDataTypeForStruct(parentStructVarName: String, st: StructType,
    nestingLevel: Int): (Seq[ExprCode], Seq[DataType]) = st.zipWithIndex.map {
    case (sf, index) => val (varName, nullVarName) =
      SHAMapAccessor.generateExplodedStructFieldVars(parentStructVarName, nestingLevel, index)
      ExprCode("", nullVarName, varName) -> sf.dataType
  }.unzip




  /**
   * Generate code to calculate the hash code for given column variables that
   * correspond to the key columns in this class.
   */
  def generateHashCode(hashVar: Array[String], keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    skipDeclaration: Boolean = false, register: Boolean = true): String = {
    var hash = hashVar(0)
    val hashDeclaration = if (skipDeclaration) "" else s"int $hash = 0;\n"
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
      case BinaryType =>
        hashBinary(colVar, nullVar, hash)
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

    def generateHashCodeCalcCode(keyDataTypesAndExprs: Seq[(ExprCode, DataType)]): String = {
      keyDataTypesAndExprs.map {
        case (ev, BooleanType) =>
          addHashInt(s"${ev.value} ? 1 : 0", ev.isNull, hash)
        case (ev, ByteType | ShortType | IntegerType | DateType) =>
          addHashInt(ev.value, ev.isNull, hash)
        case (ev, BinaryType) =>
          hashBinary(ev.value, ev.isNull, hash)
        case (ev, LongType | TimestampType) =>
          addHashLong(ctx, ev.value, ev.isNull, hash)
        case (ev, FloatType) =>
          addHashInt(s"Float.floatToIntBits(${ev.value})", ev.isNull, hash)
        case (ev, DoubleType) =>
          addHashLong(ctx, s"Double.doubleToLongBits(${ev.value})", ev.isNull,
            hash)
        case (ev, _: DecimalType) =>
          addHashInt(s"${ev.value}.fastHashCode()", ev.isNull, hash)
        case (ev, _) =>
          addHashInt(s"${ev.value}.hashCode()", ev.isNull, hash)
      }.mkString("")
    }

    if (keyVars.length > 1) {
      val remainingElems = keyVars.tail.zip(keysDataType.tail)
      if (remainingElems.size <= codeSplitThresholdSize ) {
        s"$prefix$firstColumnHash${generateHashCodeCalcCode(remainingElems)}$suffix"
      } else {
        val methodFound = (methodName: String, groupSeq: Seq[(ExprCode, DataType)],
          attributeStartIndex: Int, skipLengthCase: Boolean, nestingLevel: Int) => {
          val methodArgs = groupSeq.map(tup => {
            val exprCd = tup._1
            s"${exprCd.value}, ${if (exprCd.isNull.isEmpty) "false" else exprCd.isNull}"
          }).mkString("", ",", s", $hash")
          s"$hash = $methodName($methodArgs); \n"
        }

        val methodNotFound = (baseMethodName: String,
          paramGroupSeq: Seq[(ExprCode, DataType)], argGroupSeq: Seq[(ExprCode, DataType)],
          attributeStartIndex: Int, isSkipLengthCase: Boolean, nestingLevel: Int) => {
          val methodName = ctx.freshName(baseMethodName)
          val methodBody =
            s"""
               |${generateHashCodeCalcCode(paramGroupSeq)}
               |return $hash;
             """.stripMargin
          val methodParams = paramGroupSeq.map {
            case (exprCode, dt) => s"${ctx.javaType(dt)} ${exprCode.value}," +
              s" boolean ${exprCode.isNull}"
          }.mkString("", ",", s",int $hash")
          ctx.addNewFunction(methodName,
            s"""
               |private int $methodName($methodParams) {
               |$methodBody
               |}
                """.stripMargin)

          val methodArgs = argGroupSeq.map {
            case (exprCode, _) => s"${exprCode.value}," +
              s" ${if (exprCode.isNull.isEmpty) "false" else exprCode.isNull}"
          }.mkString("", ",", s", $hash")

          s"$hash = $methodName($methodArgs); \n" -> methodName
        }

        codeSplit(keysDataType.tail, keyVars.tail, "calculateHashCode",
          0, methodFound, methodNotFound, "", prefix + firstColumnHash, suffix,
          false)
      }
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

  private def hashBinary(colVar: String, nullVar: String,
    hashVar: String): String = {
    if (nullVar.isEmpty || nullVar == "false") {
      s"$hashVar = $hashingClass.addBytesToHash($colVar, $hashVar);\n"
    } else {
      s"$hashVar = ($nullVar) ? -1 : $hashingClass.addBytesToHash($colVar, $hashVar);\n"
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

  private def addHashLong(ctx: CodegenContext, hashExpr: String, nullVar: String,
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



    def generateCustomSHAMapClass(className: String, keyDataType: DataType): String = {

      val columnEncodingClassObject = ColumnEncoding.getClass.getName + ".MODULE$"
      val paramName = ctx.freshName("param")
      val paramJavaType = ctx.javaType(keyDataType)
      val utf8StringClass = classOf[UTF8String].getName
      val bbHashMapObject = ByteBufferHashMap.getClass.getName + ".MODULE$"
      val bsleExceptionClass = classOf[BufferSizeLimitExceededException].getName
      val customNewInsertTerm = "customNewInsert"
      val nullKeyBitsParamName = if (numBytesForNullKeyBits == 0) ""
      else ctx.freshName("nullKeyBits")

      val nullKeyBitsArg = if (numBytesForNullKeyBits == 0) ""
      else s", ${SHAMapAccessor.getNullBitsCastTerm(numBytesForNullKeyBits)}  $nullKeyBitsParamName"

      val nullKeyBitsParam = if (numBytesForNullKeyBits == 0) ""
      else s", $nullKeyBitsParamName"

      val useHashCodeForEquality = keyDataType match {
        case ByteType | ShortType | IntegerType => true
        case _ => false
      }

      def generatePutIfAbsent(): String = {
        val mapValueObjectTerm = ctx.freshName("mapValueObject")
        val mapValueOffsetTerm = ctx.freshName("mapValueOffset")
        val valueStartOffsetTerm = ctx.freshName("valueStartOffset")
        val numKeyBytesTerm = ctx.freshName("numKeyBytes")
        val isNullTerm = ctx.freshName("isNull")
        val valueEqualityFunctionStr = s""" equalsSize($mapValueObjectTerm, $mapValueOffsetTerm,
              $valueStartOffsetTerm, $paramName, $numKeyBytesTerm, $isNullTerm)""".stripMargin
        val equalSizeMethodStr = if (useHashCodeForEquality) {
          s" && (!$isNullTerm || $valueEqualityFunctionStr)"
        } else {
          s" && $valueEqualityFunctionStr"
        }
        s"""
           | public int putBufferIfAbsent($paramJavaType $paramName, int $numKeyBytesTerm,
           |  int numBytes, int hash, boolean $isNullTerm) {
           |  $bbDataClass kd = keyData();
           |  $bbDataClass vd = valueData();
           |  Object $mapValueObjectTerm = vd.baseObject();
           |  long $mapValueOffsetTerm = vd.baseOffset();
           |  Object mapKeyObject = kd.baseObject();
           |  long mapKeyBaseOffset = kd.baseOffset();
           |  int fixedKeySize = this.fixedKeySize();
           |  int localMask = this.getMask();
           |  int pos = hash & localMask;
           |  int delta = 1;
           |    while (true) {
           |      long mapKeyOffset = mapKeyBaseOffset + fixedKeySize * pos;
           |      long mapKey = $platformClass.getLong(mapKeyObject, mapKeyOffset);
           |      // offset will at least be 4 so mapKey can never be zero when occupied
           |      if (mapKey != 0L) {
           |        // first compare the hash codes followed by "equalsSize" that will
           |        // include the check for 4 bytes of numKeyBytes itself
           |        int $valueStartOffsetTerm = (int)(mapKey >>> 32L) - 4;
           |        if (hash == (int)mapKey $equalSizeMethodStr) {
           |          return handleExisting(mapKeyObject, mapKeyOffset, $valueStartOffsetTerm + 4);
           |        } else {
           |          // quadratic probing (increase delta)
           |          pos = (pos + delta) & localMask;
           |          delta += 1;
           |        }
           |      } else {
           |        if (this.maxSizeReached()) {
           |          throw $bbHashMapObject.bsle();
           |        }
           |        // insert into the map and rehash if required
           |        long relativeOffset = $customNewInsertTerm($paramName, $numKeyBytesTerm,
           |         $isNullTerm, numBytes);
           |        $platformClass.putLong(mapKeyObject, mapKeyOffset,
           |          (relativeOffset << 32L) | (hash & 0xffffffffL));
           |        try {
           |          return handleNew(mapKeyObject, mapKeyOffset, (int)relativeOffset);
           |        } catch ($bsleExceptionClass bsle) {
           |            this.maxSizeReached_$$eq(true);
           |            $platformClass.putLong(mapKeyObject, mapKeyOffset, 0L);
           |            throw $bbHashMapObject.bsle();
           |        }
           |      }
           |    }
           |   // return 0; // not expected to reach
           |  }
         """.stripMargin
      }

     def generateCustomNewInsert: String = {
       val valueBaseObjectTerm = ctx.freshName("valueBaseObject")
       val positionTerm = ctx.freshName("position")
       val paramIsNull = ctx.freshName("isNull")
       val keyWritingCode = writeVarPartialFunction((valueBaseObjectTerm, positionTerm, paramName,
         0, 0 == this.skipLenForAttribIndex, keyDataType))
       val nullAndKeyWritingCode = if (numBytesForNullKeyBits == 0) {
         keyWritingCode
       } else {
         s"""
            |${writeVarPartialFunction(valueBaseObjectTerm, positionTerm,
              s"$paramIsNull ? (byte)1 : 0", 0, 0 == this.skipLenForAttribIndex, ByteType)}
            | if (!$paramIsNull) {
            |   $keyWritingCode
            | }
          """.stripMargin
       }
       s"""
          | public long $customNewInsertTerm($paramJavaType $paramName, int numKeyBytes,
          | boolean $paramIsNull, int numBytes) {
          // write into the valueData ByteBuffer growing it if required
             |long $positionTerm = valueDataPosition();
             |$bbDataClass valueDataObj = valueData();
             |long dataSize = $positionTerm - valueDataObj.baseOffset();
             |if ($positionTerm + numBytes + 4 > valueDataObj.endPosition()) {
               |int oldCapacity = valueDataObj.capacity();
               |valueDataObj = valueDataObj.resize(numBytes + 4, allocator(), approxMaxCapacity());
               |valueData_$$eq(valueDataObj);
               |$positionTerm = valueDataObj.baseOffset() + dataSize;
               |acquireMemory(valueDataObj.capacity() - oldCapacity);
               |maxMemory_$$eq(maxMemory() + valueDataObj.capacity() - oldCapacity);
             |}
             |Object $valueBaseObjectTerm = valueDataObj.baseObject();
             // write the key size followed by the full key+value bytes
             |$columnEncodingClassObject.writeInt($valueBaseObjectTerm, $positionTerm, numKeyBytes);
             |$positionTerm += 4;
             |$nullAndKeyWritingCode
             |valueDataPosition_$$eq($positionTerm + numBytes - numKeyBytes);
             |// return the relative offset to the start excluding numKeyBytes
             |return (dataSize + 4);
          |}
         """.stripMargin
     }

      def generateEqualsSize: String = {
        val valueHolder = ctx.freshName("valueHolder")
        val valueStartOffset = ctx.freshName("valueStartOffset")
        val mapValueBaseOffset = ctx.freshName("mapValueBaseOffset")
        val valueOffset = ctx.freshName("valueOffset")
        val numKeyBytes = ctx.freshName("numKeyBytes")
        val isNull = ctx.freshName("isNull")
        val nullHolder = ctx.freshName("nullHolder")
        val lengthHolder = ctx.freshName("lengthHolder")
        val getLengthCode = s"""
           $lengthHolder = $columnEncodingClassObject.readInt($vdBaseObjectTerm, $valueOffset);
           $valueOffset += 4;
        """

        val getValueCode = keyDataType match {
          case StringType => ""
          case  _ => readVarPartialFunction(valueOffset, 0, 0, valueHolder, keyDataType)
        }
        val getNullCode = readVarPartialFunction(valueOffset, 0, 0, nullHolder, ByteType)
        val isPrimtive = SHAMapAccessor.isPrimitive(keyDataType)
        val valueEqualityCode = if (isPrimtive) {
           s"return $valueHolder == $paramName;"
        } else if (keyDataType.isInstanceOf[StringType]) {
          val stringLengthArg = if (numBytesForNullKeyBits == 0) {
            numKeyBytes
          } else {
            s"$numKeyBytes - 1"
          }
          s"""
              return $byteArrayEqualsClass.arrayEquals($vdBaseObjectTerm, $valueOffset,
            $paramName.getBaseObject(), $paramName.getBaseOffset(), $stringLengthArg);"""
        }
        else {
          s"return $valueHolder.equals($paramName);"
        }

        val equalityCode = if (numBytesForNullKeyBits == 0) {
          // this is case of not null group by column.
          // the whole keylength represents the key size
          s"""
             |$getLengthCode
             |if ($numKeyBytes == $lengthHolder) {
             |  $getValueCode
             |  $valueEqualityCode
             |} else {
             |  return false;
             |}
           """.stripMargin
        } else {
          s"""
             |$getLengthCode
             |if ($lengthHolder == $numKeyBytes) {
             |  $getNullCode
             |  if (${SHAMapAccessor.getExpressionForNullEvalFromMask(0, 1,
                  nullHolder)} == $isNull) {
                  if ($isNull) {
                    return true;
                  } else {
                    $getValueCode
                    $valueEqualityCode
                  }
                } else {
                  return false;
                }
             |} else {
             |  return false;
             |}""".stripMargin
        }

        s"""
           | public boolean equalsSize(Object $vdBaseObjectTerm, long $mapValueBaseOffset,
           |   int $valueStartOffset, $paramJavaType $paramName, int $numKeyBytes,
           | boolean $isNull) {
           |    long $valueOffset = $mapValueBaseOffset + $valueStartOffset;
           |    $paramJavaType $valueHolder = ${ctx.defaultValue(keyDataType)};
           |    byte $nullHolder = 0;
           |    int $lengthHolder = 0;
           |    $equalityCode
           |  }
         """.stripMargin

      }

      s"""
         |static class $className extends ${classOf[SHAMap].getName} {
           |public $className(int initialCapacity, int valueSize, int maxCapacity) {
              |super(initialCapacity, valueSize, maxCapacity);
           |}
           |${generatePutIfAbsent()}
           |${generateEqualsSize}
           |${generateCustomNewInsert}
         |}
     """.stripMargin
   }
}

object SHAMapAccessor {
  val nullVarSuffix = "_isNull"
  val supportedDataTypes: DataType => Boolean = dt =>
    dt match {
      case _: MapType => false
      case _: UserDefinedType[_] => false
      case CalendarIntervalType => false
      case NullType => false
      case _: ObjectType => false
      case ArrayType(elementType, _) => elementType match {
        case _: StructType => false
        case _ => true
      }
      case _ => true

      // includes atomic types, string type, array type
      // ( depends on element type) , struct type ( depends on fields)
    }

  def initNullBitsetCode(nullBitsetTerm: String,
    numBytesForNullBits: Int): String = if (numBytesForNullBits == 0) {
    ""
  } else if (numBytesForNullBits == 1) {
    s"byte $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits == 2) {
    s"short $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits <= 4) {
    s"int $nullBitsetTerm = 0;"
  } else if (numBytesForNullBits <= 8) {
    s"long $nullBitsetTerm = 0l;"
  } else {
    s"""
        |for( int i = 0 ; i < $numBytesForNullBits; ++i) {
          |$nullBitsetTerm[i] = 0;
        |}""".stripMargin
  }

  def resetNullBitsetCode(nullBitsetTerm: String,
    numBytesForNullBits: Int): String = if (numBytesForNullBits == 0) {
    ""
  } else if (numBytesForNullBits <= 8) {
    s"$nullBitsetTerm = 0; \n"
  } else {
    s"""
       for( int i = 0 ; i < $numBytesForNullBits; ++i) {
         $nullBitsetTerm[i] = 0;
       }

     """.stripMargin
  }

  def calculateNumberOfBytesForNullBits(numAttributes: Int): Int = (numAttributes + 7 )/ 8

  def generateNullKeysBitTermForStruct(structName: String): String = s"${structName}_nullKeysBitset"

  def generateVarNameForStructField(parentVar: String,
    nestingLevel: Int, index: Int): String = s"${parentVar}_${nestingLevel}_$index"

  def generateExplodedStructFieldVars(parentVar: String,
    nestingLevel: Int, index: Int): (String, String) = {
    val varName = generateVarNameForStructField(parentVar, nestingLevel, index)
    val isNullVarName = s"${varName}_isNull"
    (varName, isNullVarName)
  }

  def isByteArrayNeededForNullBits(numBytes: Int): Boolean = numBytes > 8

  def sizeForNullBits(numBytesForNullBits: Int): Int =
    if (numBytesForNullBits == 0) {
      0
    } else if (numBytesForNullBits < 3 || numBytesForNullBits > 8) {
      numBytesForNullBits
    } else if (numBytesForNullBits <= 4) {
      4
    } else {
      8
    }


   def getExpressionForNullEvalFromMask(i: Int, numBytesForNullBits: Int,
     nullBitTerm: String ): String = {
     val castTerm = getNullBitsCastTerm(numBytesForNullBits)
     if (isByteArrayNeededForNullBits(numBytesForNullBits)) {
       val remainder = i % 8
       val index = i / 8
       s"""($nullBitTerm[$index] & (0x01 << $remainder)) != 0"""
     } else {
       s"""($nullBitTerm & ((($castTerm)0x01) << $i)) != 0"""
     }
   }

  def getSizeOfValueBytes(aggDataTypes: Seq[DataType], numBytesForNullAggBits: Int): Int = {
    aggDataTypes.foldLeft(0)((size, dt) => size + dt.defaultSize) +
      sizeForNullBits(numBytesForNullAggBits)
  }

  def getNullBitsCastTerm(numBytesForNullBits: Int): String = if (numBytesForNullBits == 1) {
    "byte"
  } else if (numBytesForNullBits == 2) {
    "short"
  } else if (numBytesForNullBits <= 4) {
    "int"
  } else if (numBytesForNullBits <= 8){
    "long"
  } else {
   ""
  }

  def getOffsetIncrementCodeForNullAgg(offsetTerm: String, dt: DataType): String = {
    s"""$offsetTerm += ${dt.defaultSize};"""
  }

  def evaluateNullBitsAndEmbedWrite(numBytesForNullBits: Int, expr: ExprCode,
    attribIndex: Int, nullBitsTerm: String, offsetTerm: String, dt: DataType,
    isKey: Boolean, writingCodeToEmbed: String): String = {
    val nullVar = expr.isNull
    if (isByteArrayNeededForNullBits(numBytesForNullBits)) {
      val remainder = attribIndex % 8
      val index = attribIndex / 8
      if (nullVar.isEmpty || nullVar == "false") {
        s"""$writingCodeToEmbed"""
      } else if (nullVar == "true") {
        s"""
        |$nullBitsTerm[$index] |= (byte)((0x01 << $remainder));
        ${
          if (isKey) {
            ""
          } else {
            getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
          }
        }
        """.stripMargin
      }
      else {
        s""" if ($nullVar) {
               |$nullBitsTerm[$index] |= (byte)((0x01 << $remainder));
               |${
                   if (isKey) ""
                   else getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
                }
             } else {
               |$writingCodeToEmbed
             }
         """.stripMargin
      }
    }
    else {
      val castTerm = getNullBitsCastTerm(numBytesForNullBits)
      if (nullVar.isEmpty || nullVar == "false") {
        s"""$writingCodeToEmbed"""
      } else if (nullVar == "true") {
        s""""$nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $attribIndex));
            |${ if (isKey) ""
                else getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
             }
         """.stripMargin

      } else {
       s"""
          |if ($nullVar) {
            |$nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $attribIndex));
            |${ if (isKey) ""
                else getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
              }
          |} else {
          | $writingCodeToEmbed
          |}
        """.stripMargin
      }
    }
  }

  def evaluateNullBitsAndEmbedWrite(numBytesForNullBits: Int, expr: ExprCode,
    attribIndex: String, nullBitsTerm: String, offsetTerm: String, dt: DataType,
    isKey: Boolean, writingCodeToEmbed: String, ctx: CodegenContext): String = {
    val nullVar = expr.isNull
    if (isByteArrayNeededForNullBits(numBytesForNullBits)) {
      val remainderTrm = ctx.freshName("remainder")
      val indexTerm = ctx.freshName("index")
      s"""
         |int $remainderTrm = $attribIndex % 8;
         |int $indexTerm = $attribIndex / 8;
         |if ($nullVar) {
            |$nullBitsTerm[$indexTerm] |= (byte) ((0x01 << $remainderTrm));
            |${if (isKey) "" else getOffsetIncrementCodeForNullAgg(offsetTerm, dt)}
         |} else {
            |$writingCodeToEmbed
         |} """.stripMargin
    }
    else {
      val castTerm = getNullBitsCastTerm(numBytesForNullBits)
        s"""
           |if ($nullVar) {
             |$nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $attribIndex));
             |${ if (isKey) ""
               else getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
              }
           |} else {
             |$writingCodeToEmbed
           |}
        """.stripMargin
      }
  }

  def isPrimitive(dataType: DataType): Boolean =
     dataType match {
       case at: AtomicType =>
         typeOf(at.tag) match {
           case t if t =:= typeOf[Boolean] => true
           case t if t =:= typeOf[Byte] => true
           case t if t =:= typeOf[Short] => true
           case t if t =:= typeOf[Int] => true
           case t if t =:= typeOf[Long] => true
           case t if t =:= typeOf[Float] => true
           case t if t =:= typeOf[Double] => true
           case _ => false
         }
       case _ => false
     }


  def initDictionaryCodeForSingleKeyCase(
    input: Seq[ExprCode], keyExpressions: Seq[Expression],
    output: Seq[Attribute], ctx: CodegenContext, session: SnappySession): Option[DictionaryCode] = {
    // make a copy of input key variables if required since this is used
    // only for lookup and the ExprCode's code should not be cleared
   DictionaryOptimizedMapAccessor.checkSingleKeyCase(
      keyExpressions, getExpressionVars(keyExpressions, input.map(_.copy()),
        output, ctx), ctx, session)
  }

  private def getExpressionVars(expressions: Seq[Expression],
    input: Seq[ExprCode],
    output: Seq[Attribute], ctx: CodegenContext): Seq[ExprCode] = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val vars = ctx.generateExpressions(expressions.map(e =>
      BindReferences.bindReference[Expression](e, output)))
    ctx.currentVars = null
    vars
  }

  def getObjectTypeForPrimitiveType(primType: String): String =
    if (primType.equals("int")) {
     "Integer"
    } else {
      primType.substring(0, 1).toUpperCase + primType.substring(1)
    }
}