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
import io.snappydata.Property
import io.snappydata.collection.ByteBufferData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

case class SHAMapAccessor(@transient session: SnappySession,
  @transient ctx: CodegenContext, @transient keyExprs: Seq[Expression],
  @transient valueExprs: Seq[Expression], classPrefix: String,
  hashMapTerm: String, valueOffsetTerm: String, numKeyBytesTerm: String,
  numValueBytesTerm: String, currentOffSetForMapLookupUpdt: String, valueDataTerm: String,
  vdBaseObjectTerm: String, vdBaseOffsetTerm: String,
  nullKeysBitsetTerm: String, numBytesForNullKeyBits: Int,
  allocatorTerm: String, numBytesForNullAggBits: Int,
  nullAggsBitsetTerm: String, sizeAndNumNotNullFuncForStringArr: String,
  keyBytesHolderVarTerm: String, baseKeyObject: String,
  baseKeyHolderOffset: String, keyExistedTerm: String,
  skipLenForAttribIndex: Int, codeForLenOfSkippedTerm: String,
  valueDataCapacityTerm: String, storedAggNullBitsTerm: Option[String],
  aggregateBufferVars: Seq[String], keyHolderCapacityTerm: String)
  extends CodegenSupport {

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

  def getBufferVars(dataTypes: Seq[DataType], varNames: Seq[String],
    currentValueOffsetTerm: String, isKey: Boolean, nullBitTerm: String,
    numBytesForNullBits: Int, skipNullBitsCode: Boolean, nestingLevel: Int = 0):
  Seq[ExprCode] = {
    val plaformClass = classOf[Platform].getName
    val decimalClass = classOf[Decimal].getName
    val bigDecimalObjectClass = s"$decimalClass$$.MODULE$$"
    val bigDecimalClass = classOf[java.math.BigDecimal].getName
    val bigIntegerClass = classOf[java.math.BigInteger].getName
    val byteBufferClass = classOf[ByteBuffer].getName
    val unsafeClass = classOf[UnsafeRow].getName
    val castTerm = getNullBitsCastTerm(numBytesForNullBits)
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
        if (numBytesForNullBits <= 8) {
          s"""$booleanStr $nullVar = ($nullBitTerm & ((($castTerm)0x01) << $i)) == 0;""".stripMargin
        } else {
          val remainder = i % 8
          val index = i / 8
          s"""$booleanStr $nullVar =
             |($nullBitTerm[$index] & (0x01 << $remainder)) == 0;""".stripMargin
        }
      }

      val evaluationCode = (dt match {
       /* case StringType =>
          val holder = ctx.freshName("holder")
          val holderBaseObject = ctx.freshName("holderBaseObject")
          val holderBaseOffset = ctx.freshName("holderBaseOffset")
          val len = ctx.freshName("len")
          val readLenCode = if (nestingLevel == 0 && i == skipLenForAttribIndex) {
            s"int $len = $codeForLenOfSkippedTerm"
          } else {
            s"""int $len = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
               |$currentValueOffsetTerm += 4;
               """
          }
          s"""$readLenCode
             |$byteBufferClass $holder =  $allocatorTerm.allocate($len, "SHA");
             | Object $holderBaseObject = $allocatorTerm.baseObject($holder);
             | long $holderBaseOffset = $allocatorTerm.baseOffset($holder);
             | $plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
             | $holderBaseObject, $holderBaseOffset , $len);
             | $varName = ${classOf[UTF8String].getName}.fromAddress($holderBaseObject,
             |  $holderBaseOffset, $len);
             |$currentValueOffsetTerm += $len;
          """.stripMargin
          */
        case StringType =>
          val len = ctx.freshName("len")
          val readLenCode = if (nestingLevel == 0 && i == skipLenForAttribIndex) {
            s"int $len = $codeForLenOfSkippedTerm"
          } else {
            s"""int $len = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
               |$currentValueOffsetTerm += 4;
               """
          }
          s"""$readLenCode
             | $varName = ${classOf[UTF8String].getName}.fromAddress($vdBaseObjectTerm,
             | $currentValueOffsetTerm, $len);
             |$currentValueOffsetTerm += $len;
          """.stripMargin
        case BinaryType =>
          s"""$varName = new byte[$plaformClass.getInt($vdBaseObjectTerm,
             | $currentValueOffsetTerm)];
             |$currentValueOffsetTerm += 4;
             |$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
             | $varName, ${Platform.BYTE_ARRAY_OFFSET}, $varName.length);
             | $currentValueOffsetTerm += $varName.length;
               """.stripMargin
        case x: AtomicType => {
          (typeOf(x.tag) match {
            case t if t =:= typeOf[Boolean] =>
              s"""$varName = $plaformClass.getBoolean($vdBaseObjectTerm, $currentValueOffsetTerm);
              """
            case t if t =:= typeOf[Byte] =>
              s"""$varName = $plaformClass.getByte($vdBaseObjectTerm, $currentValueOffsetTerm);
               """
            case t if t =:= typeOf[Short] =>
              s"""$varName = $plaformClass.getShort($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
            case t if t =:= typeOf[Int] =>
              s"""$varName = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
               """.stripMargin
            case t if t =:= typeOf[Long] =>
              s"""$varName = $plaformClass.getLong($vdBaseObjectTerm,$currentValueOffsetTerm);
              """.stripMargin
            case t if t =:= typeOf[Float] =>
              s"""$varName = $plaformClass.getFloat($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
            case t if t =:= typeOf[Double] =>
              s"""$varName = $plaformClass.getDouble($vdBaseObjectTerm, $currentValueOffsetTerm);
              """.stripMargin
            case t if t =:= typeOf[Decimal] =>
                if (dt.asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS) {
                s"""$varName = new $decimalClass().set(
                   |$plaformClass.getLong($vdBaseObjectTerm, $currentValueOffsetTerm),
                   ${dt.asInstanceOf[DecimalType].precision},
                   ${dt.asInstanceOf[DecimalType].scale});""".stripMargin
              } else {
                val tempByteArrayTerm = ctx.freshName("tempByteArray")
                val len = ctx.freshName("len")
                s"""int $len = $plaformClass.getByte($vdBaseObjectTerm, $currentValueOffsetTerm);
                   |$currentValueOffsetTerm += 1;
                   |byte[] $tempByteArrayTerm = new byte[$len];
                   |$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
                   |$tempByteArrayTerm, ${Platform.BYTE_ARRAY_OFFSET} , $len);
                   |$varName = $bigDecimalObjectClass.apply(new $bigDecimalClass(
                   |new $bigIntegerClass($tempByteArrayTerm),
                   |${dt.asInstanceOf[DecimalType].scale}),
                   |${dt.asInstanceOf[DecimalType].precision},
                   |${dt.asInstanceOf[DecimalType].scale});
                   """.stripMargin
              }

            case _ => throw new UnsupportedOperationException("unknown type " + dt)
          }) +
            s"""$currentValueOffsetTerm += ${dt.defaultSize};"""
        }
        case ArrayType(elementType, containsNull) =>
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

          s"""boolean $isExploded = $plaformClass.getBoolean($vdBaseObjectTerm,
             |$currentValueOffsetTerm);
             |++$currentValueOffsetTerm;
             |if ($isExploded) {
               |int $arraySize = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
               |$currentValueOffsetTerm += 4;
               |$objectClass[] $objectArray = new $objectClass[$arraySize];
               |if ($containsNull) {
                 |int $varWidthNumNullBytes = $arraySize/8 + ($arraySize % 8 > 0 ? 1 : 0);
                 |byte[] $varWidthNullBits = new byte[$varWidthNumNullBytes];
                 |$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
                 | $varWidthNullBits, ${Platform.BYTE_ARRAY_OFFSET}, $varWidthNumNullBytes);
                 |$currentValueOffsetTerm += $varWidthNumNullBytes;
                 |for (int $counter = 0; $counter < $arraySize; ++$counter ) {
                   |int $remainder = $counter % 8;
                   |int $indx = $counter / 8;
                   |if ( ($varWidthNullBits[$indx] & (0x01 << $remainder)) != 0) {
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
            |int $arraySize = $plaformClass.getInt($vdBaseObjectTerm, $currentValueOffsetTerm);
            |$currentValueOffsetTerm += 4;
            |$byteBufferClass $holder = $allocatorTerm.allocate($arraySize, "SHA");
            |$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
            |$allocatorTerm.baseObject($holder), $allocatorTerm.baseOffset($holder),$arraySize);
            |$currentValueOffsetTerm += $arraySize;
            |$varName = new $unsafeArrayDataClass();
            |(($unsafeArrayDataClass)$varName).pointTo($allocatorTerm.baseObject($holder),
            |$allocatorTerm.baseOffset($holder), $arraySize);
          |}""".stripMargin
        case st: StructType =>
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
          s"""boolean $isExploded = $plaformClass.getBoolean($vdBaseObjectTerm,
             |$currentValueOffsetTerm);
             |++$currentValueOffsetTerm;
             |if ($isExploded) {
               |${
                 readNullBitsCode(currentValueOffsetTerm, nullBitSetTermForStruct,
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
                   numNullKeyBytesForStruct, false, nestingLevel + 1).map(_.code).mkString("\n")
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
               |int $unsafeRowLength = $plaformClass.getInt($vdBaseObjectTerm,
               | $currentValueOffsetTerm);
               |$currentValueOffsetTerm += 4;
               |$byteBufferClass $holder = $allocatorTerm.allocate($unsafeRowLength, "SHA");
               |$plaformClass.copyMemory($vdBaseObjectTerm, $currentValueOffsetTerm,
               |$allocatorTerm.baseObject($holder), $allocatorTerm.baseOffset($holder),
               |$unsafeRowLength);
               |$currentValueOffsetTerm += $unsafeRowLength;
               |$varName = new $unsafeClass(${st.length});
               |(($unsafeClass)$varName).pointTo($allocatorTerm.baseObject($holder),
               | $allocatorTerm.baseOffset($holder), $unsafeRowLength);
             |} """.stripMargin
      }).trim

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
               ${getOffsetIncrementCodeForNullAgg(currentValueOffsetTerm, dt)}
             } """.stripMargin
          } else {
            ""
          }
        }""".stripMargin
      }
      ExprCode(exprCode, nullVar, varName)
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


  def initKeyOrBufferVal(dataTypes: Seq[DataType], varNames: Seq[String]):
  String = dataTypes.zip(varNames).map { case (dt, varName) =>
    s"${ctx.javaType(dt)} $varName = ${ctx.defaultValue(dt)};"
  }.mkString("\n")

  def declareNullVarsForAggBuffer(varNames: Seq[String]): String =
    varNames.map(varName => s"boolean ${varName}${SHAMapAccessor.nullVarSuffix};").mkString("\n")
  /**
   * Generate code to lookup the map or insert a new key, value if not found.
   */
  def generateMapGetOrInsert(valueInitVars: Seq[ExprCode],
    valueInitCode: String, input: Seq[ExprCode], keyVars: Seq[ExprCode],
    keysDataType: Seq[DataType], aggregateDataTypes: Seq[DataType]): String = {
    val hashVar = Array(ctx.freshName("hash"))
    val tempValueData = ctx.freshName("tempValueData")



    val bbDataClass = classOf[ByteBufferData].getName

    // val valueInit = valueInitCode + '\n'
    val numAggBytes = getSizeOfValueBytes(aggregateDataTypes)
    /* generateUpdate(objVar, Nil,
      valueInitVars, forKey = false, doCopy = false) */
    val inputEvals = evaluateVariables(input)

    s"""|$valueInitCode
        |${SHAMapAccessor.resetNullBitsetCode(nullKeysBitsetTerm, numBytesForNullKeyBits)}
        |${SHAMapAccessor.resetNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggBits)}
          // evaluate input row vars
        |$inputEvals
           // evaluate key vars
        |${evaluateVariables(keyVars)}
        |${keyVars.zip(keysDataType).filter(_._2 match {
              case x: StructType => true
              case _ => false
            }).map {
                case (exprCode, dt) => explodeStruct(exprCode.value, exprCode.isNull,
                  dt.asInstanceOf[StructType])
              }.mkString("\n")
        }
        // evaluate hash code of the lookup key
        |${generateHashCode(hashVar, keyVars, this.keyExprs, keysDataType)}
        |//  System.out.println("hash code for key = " +${hashVar(0)});
        |// get key size code
        |$numKeyBytesTerm = ${generateKeySizeCode(keyVars, keysDataType, numBytesForNullKeyBits)};
        |$numValueBytesTerm = $numAggBytes;
        |// prepare the key
        |${generateKeyBytesHolderCode(numKeyBytesTerm, numValueBytesTerm,
           keyVars, keysDataType, aggregateDataTypes, valueInitVars)
          }
        // insert or lookup
        |long $valueOffsetTerm = $hashMapTerm.putBufferIfAbsent($baseKeyObject,
        | $baseKeyHolderOffset, $numKeyBytesTerm, $numValueBytesTerm + $numKeyBytesTerm,
        | ${hashVar(0)});
        |boolean $keyExistedTerm = $valueOffsetTerm >= 0;
        |if (!$keyExistedTerm) {
          |$valueOffsetTerm = -1 * $valueOffsetTerm;
          |// $bbDataClass $tempValueData = $hashMapTerm.getValueData();
          |// if ($valueDataTerm !=  $tempValueData) {
          |if ($valueOffsetTerm >=  $valueDataCapacityTerm) {
            |//$valueDataTerm = $tempValueData;
            |$valueDataTerm =  $hashMapTerm.getValueData();
            |$vdBaseObjectTerm = $valueDataTerm.baseObject();
            |$vdBaseOffsetTerm = $valueDataTerm.baseOffset();
            |$valueDataCapacityTerm = $valueDataTerm.capacity();
          |}
        |}
        |// position the offset to start of aggregate value
        |$valueOffsetTerm += $numKeyBytesTerm + $vdBaseOffsetTerm;
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
       |${storedAggNullBitsTerm.map(storedNullBit =>
         s"$storedNullBit = $nullAggsBitsetTerm;").getOrElse("")}
      ${SHAMapAccessor.resetNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggBits)}
      ${
      writeKeyOrValue(vdBaseObjectTerm, currentOffSetForMapLookupUpdt,
        aggBufferDataType, bufferVars, nullAggsBitsetTerm, numBytesForNullAggBits,
        false, false)
     }
    """.stripMargin

  }

  def getOffsetIncrementCodeForNullAgg(offsetTerm: String, dt: DataType): String = {
    s"""$offsetTerm += ${
      dt.defaultSize + (dt match {
        case dec: DecimalType  if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
        case _ => 0
      })
    };"""
  }

  def writeKeyOrValue(baseObjectTerm: String, offsetTerm: String,
    dataTypes: Seq[DataType], varsToWrite: Seq[ExprCode], nullBitsTerm: String,
    numBytesForNullBits: Int, isKey: Boolean, skipNullEvalCode: Boolean,
    nestingLevel: Int = 0): String = {
    // Move the offset at the end of num Null Bytes space, we will fill that space later
    // store the starting value of offset
    val unsafeArrayClass = classOf[UnsafeArrayData].getName
    val unsafeRowClass = classOf[UnsafeRow].getName
    val startingOffsetTerm = ctx.freshName("startingOffset")
    val tempBigDecArrayTerm = ctx.freshName("tempBigDecArray")
    val plaformClass = classOf[Platform].getName
    val storeNullBitStartOffsetAndRepositionOffset = if (skipNullEvalCode) {
      ""
    } else {
      s"""long $startingOffsetTerm = $offsetTerm;
          |// move current offset to end of null bits
          |$offsetTerm += ${sizeForNullBits(numBytesForNullBits)};""".stripMargin
    }
    s"""$storeNullBitStartOffsetAndRepositionOffset
       |${dataTypes.zip(varsToWrite).zipWithIndex.map {
         case ((dt, expr), i) =>
          val variable = expr.value
          val writingCode = (dt match {
            case x: AtomicType =>
              val snippet = typeOf(x.tag) match {
                case t if t =:= typeOf[Boolean] =>
                s"""$plaformClass.putBoolean($baseObjectTerm, $offsetTerm, $variable);
                    |$offsetTerm += ${dt.defaultSize};
                """.stripMargin
                case t if t =:= typeOf[Byte] =>
                  s"""$plaformClass.putByte($baseObjectTerm, $offsetTerm, $variable);
                     |$offsetTerm += ${dt.defaultSize};
                """.stripMargin
                case t if t =:= typeOf[Array[Byte]] =>
                  s"""$plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable.length);
                     |$offsetTerm += 4;
                     |$plaformClass.copyMemory($variable, ${Platform.BYTE_ARRAY_OFFSET},
                     |$baseObjectTerm, $offsetTerm, $variable.length);
                     |$offsetTerm += $variable.length;
                """.stripMargin
                case t if t =:= typeOf[Short] =>
                  s"""$plaformClass.putShort($baseObjectTerm, $offsetTerm, $variable);
                     |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
                case t if t =:= typeOf[Int] =>
                  s"""
                  $plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable);
                 $offsetTerm += ${dt.defaultSize};
                 """.stripMargin
                case t if t =:= typeOf[Long] =>
                  s"""$plaformClass.putLong($baseObjectTerm, $offsetTerm, $variable);
                     |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
                case t if t =:= typeOf[Float] =>
                  s"""$plaformClass.putFloat($baseObjectTerm, $offsetTerm, $variable);
                     |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
                case t if t =:= typeOf[Double] =>
                  s"""$plaformClass.putDouble($baseObjectTerm, $offsetTerm, $variable);
                     |$offsetTerm += ${dt.defaultSize};
                  """.stripMargin
                case t if t =:= typeOf[Decimal] =>
                  s"""
                     |if (!$variable.changePrecision(
                     |${dt.asInstanceOf[DecimalType].precision},
                     | ${dt.asInstanceOf[DecimalType].scale})) {
                     |   throw new AssertionError("Unable to set precision & scale on Decimal");
                     | }
                   """.stripMargin +
                    (if (dt.asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS) {
                    s"""$plaformClass.putLong($baseObjectTerm, $offsetTerm,
                       | $variable.toUnscaledLong());
                       |$offsetTerm += ${dt.defaultSize};
                     """.stripMargin
                  } else {
                    s"""byte[] $tempBigDecArrayTerm = $variable.toJavaBigDecimal().
                       |unscaledValue().toByteArray();
                       |assert ($tempBigDecArrayTerm.length <= 16);
                       |$plaformClass.putByte($baseObjectTerm, $offsetTerm,
                       |(byte)$tempBigDecArrayTerm.length);
                       |$plaformClass.copyMemory($tempBigDecArrayTerm,
                       |$plaformClass.BYTE_ARRAY_OFFSET,
                       |$baseObjectTerm, $offsetTerm + 1, $tempBigDecArrayTerm.length);
                       |$offsetTerm += ${dt.defaultSize} + 1;
                    """.stripMargin
                  })
                case t if t =:= typeOf[UTF8String] =>
                  val lengthWritingPart = if (nestingLevel > 0 || i != skipLenForAttribIndex) {
                    s"""$plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable.numBytes());
                        |$offsetTerm += 4;""".stripMargin
                  } else ""

                  s"""$lengthWritingPart
                     |$variable.writeToMemory($baseObjectTerm, $offsetTerm);
                     |$offsetTerm += $variable.numBytes();
               """.stripMargin
                case _ => throw new UnsupportedOperationException("unknown type " + dt)
              }
              snippet
            case st: StructType => val (childExprCodes, childDataTypes) =
              getExplodedExprCodeAndDataTypeForStruct(variable, st, nestingLevel)
              val newNullBitTerm = SHAMapAccessor.generateNullKeysBitTermForStruct(variable)
              val newNumBytesForNullBits = SHAMapAccessor.
                calculateNumberOfBytesForNullBits(st.length)
              val explodeStructSnipet =
                s"""$plaformClass.putBoolean($baseObjectTerm, $offsetTerm, true);
                   |$offsetTerm += 1;
                   |${
                      writeKeyOrValue(baseObjectTerm, offsetTerm, childDataTypes, childExprCodes,
                      newNullBitTerm, newNumBytesForNullBits, true, false,
                        nestingLevel + 1)
                    }
                 """.stripMargin
              val unexplodedStructSnippet =
               s"""|$plaformClass.putBoolean($baseObjectTerm, $offsetTerm, false);
                   |$offsetTerm += 1;
                   |$plaformClass.putInt($baseObjectTerm, $offsetTerm,
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


            case at@ArrayType(elementType, containsNull) =>
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
               s"""|$plaformClass.putBoolean($baseObjectTerm, $offsetTerm, true);
                   |$offsetTerm += 1;
                   |$plaformClass.putInt($baseObjectTerm, $offsetTerm, $variable.numElements());
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
                     |if (!$variable.isNullAt($counter)) {
                       |${ctx.javaType(elementType)} $arrElement =
                       |(${ctx.boxedType(elementType)}) $variable.get($counter, $dataType);
                       |$elementWitingCode
                       |if ($containsNull) {
                         |$varWidthNullBits[$arrIndex] |= (byte)((0x01 << $remainder));
                       |}
                     |}
                   |}
                   |${ if (containsNull ) {
                         s"""
                          |$plaformClass.copyMemory($varWidthNullBits,
                          |${Platform.BYTE_ARRAY_OFFSET},
                          |$baseObjectTerm, $varWidthNullBitStartPos, $varWidthNumNullBytes);
                         """.stripMargin
                        } else ""
                     }
                """.stripMargin
              val unexplodedArraySnippet =
                s"""$plaformClass.putBoolean($baseObjectTerm, $offsetTerm, false);
                   |$offsetTerm += 1;
                   |$plaformClass.putInt($baseObjectTerm, $offsetTerm,
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
            case _ => throw new UnsupportedOperationException("unknown type " + dt)
          }).trim


          // Now do the actual writing based on whether the variable is null or not
          if (skipNullEvalCode) {
            writingCode
          } else {
            val castTerm = getNullBitsCastTerm(numBytesForNullBits)
            val nullVar = expr.isNull
            if (numBytesForNullBits > 8) {
              val remainder = i % 8
              val index = i / 8
              if (nullVar.isEmpty || nullVar == "false") {
                s"""$nullBitsTerm[$index] |= (byte)((0x01 << $remainder));
                   |$writingCode""".stripMargin
              } else if (nullVar == "true") {
                if (!isKey) {
                  getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
                } else {
                  ""
                }
              }
              else {
                s"""if (!$nullVar) {
                     |$nullBitsTerm[$index] |= (byte)((0x01 << $remainder));
                     |$writingCode
                    }${
                  if (!isKey) {
                    s"""else {
                          ${getOffsetIncrementCodeForNullAgg(offsetTerm, dt)}
                        }""".stripMargin
                  } else ""
                }""".stripMargin
              }
            }
            else {
              if (nullVar.isEmpty || nullVar == "false") {
                s"""$nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $i));
                   |$writingCode
                """.stripMargin
              } else if (nullVar == "true") {
                if (!isKey) {
                  getOffsetIncrementCodeForNullAgg(offsetTerm, dt)
                } else {
                  ""
                }
              } else {
                s"""if (!$nullVar) {
                        $nullBitsTerm |= ($castTerm)(( (($castTerm)0x01) << $i));
                        $writingCode
                    }${if (!isKey) {
                      s"""else {
                           ${getOffsetIncrementCodeForNullAgg(offsetTerm, dt)}
                          }""".stripMargin
                  } else ""
                 }""".stripMargin
              }
            }
          }

      }.mkString("\n")
    }
    // now write the nullBitsTerm
    ${if (!skipNullEvalCode) {
        val nullBitsWritingCode = writeNullBitsAt(baseObjectTerm, startingOffsetTerm,
          nullBitsTerm, numBytesForNullBits)
       if(isKey) {
         nullBitsWritingCode
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
    }"""

  }

  def generateKeyBytesHolderCode(numKeyBytesVar: String, numValueBytesVar: String,
    keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    aggregatesDataType: Seq[DataType], valueInitVars: Seq[ExprCode]): String = {

    val byteBufferClass = classOf[ByteBuffer].getName
    val currentOffset = ctx.freshName("currentOffset")
    val plaformClass = classOf[Platform].getName
    s"""
        if ($keyBytesHolderVarTerm == null || $keyHolderCapacityTerm <
      $numKeyBytesVar + $numValueBytesVar) {
          //$keyBytesHolderVarTerm = $allocatorTerm.allocate($numKeyBytesVar + $numValueBytesVar, "SHA");
          //$baseKeyObject = $allocatorTerm.baseObject($keyBytesHolderVarTerm);
          //$baseKeyHolderOffset = $allocatorTerm.baseOffset($keyBytesHolderVarTerm);
           $keyHolderCapacityTerm = $numKeyBytesVar + $numValueBytesVar;
           $keyBytesHolderVarTerm = $byteBufferClass.allocate($keyHolderCapacityTerm);
           $baseKeyObject = $keyBytesHolderVarTerm.array();
           $baseKeyHolderOffset = $plaformClass.BYTE_ARRAY_OFFSET;
        }

        long $currentOffset = $baseKeyHolderOffset;
        // first write key data
        ${ writeKeyOrValue(baseKeyObject, currentOffset, keysDataType, keyVars,
          nullKeysBitsetTerm, numBytesForNullKeyBits, true, numBytesForNullKeyBits == 0)
        }
       // write value data
       ${"" /* writeKeyOrValue(baseKeyObject, currentOffset, aggregatesDataType, valueInitVars,
          nullAggsBitsetTerm, numBytesForNullAggBits, false, false) */
        }
    """.stripMargin
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

  def getSizeOfValueBytes(aggDataTypes: Seq[DataType]): Int = {
    aggDataTypes.foldLeft(0)((size, dt) => size + dt.defaultSize + (dt match {
      case dec: DecimalType if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
      case _ => 0
    })
    ) + sizeForNullBits(numBytesForNullAggBits)
  }

  def generateKeySizeCode(keyVars: Seq[ExprCode], keysDataType: Seq[DataType],
    numBytesForNullBits: Int, nestingLevel: Int = 0): String = {
    val unsafeRowClass = classOf[UnsafeRow].getName
    val unsafeArrayDataClass = classOf[UnsafeArrayData].getName

    keysDataType.zip(keyVars).zipWithIndex.map { case ((dt, expr), i) =>
      val nullVar = expr.isNull
      val notNullSizeExpr = if (TypeUtilities.isFixedWidth(dt)) {
        (dt.defaultSize + (dt match {
          case dec: DecimalType if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
          case _ => 0
        })).toString
      } else {
        dt match {
          case StringType =>
            val strPart = s"${expr.value}.numBytes()"
            if (nestingLevel == 0 && i == skipLenForAttribIndex) {
              strPart
            } else {
              s"($strPart + 4)"
            }
          case BinaryType => s"(${expr.value}.length + 4) "
          case st: StructType => val (childKeysVars, childDataTypes) =
            getExplodedExprCodeAndDataTypeForStruct(expr.value, st, nestingLevel)
            val explodedStructSizeCode = generateKeySizeCode(childKeysVars, childDataTypes,
              SHAMapAccessor.calculateNumberOfBytesForNullBits(st.length), nestingLevel + 1)
            val unexplodedStructSizeCode = s"(($unsafeRowClass) ${expr.value}).getSizeInBytes() + 4"

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
              (true, dt.defaultSize + (dt match {
                case dec: DecimalType if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
                case _ => 0
              }))

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
    }.mkString(" + ") + s" + ${sizeForNullBits(numBytesForNullBits)}"
  }

  def getExplodedExprCodeAndDataTypeForStruct(parentStructVarName: String, st: StructType,
    nestingLevel: Int): (Seq[ExprCode], Seq[DataType]) = st.zipWithIndex.map {
    case (sf, index) => val (varName, nullVarName) =
      SHAMapAccessor.generateExplodedStructFieldVars(parentStructVarName, nestingLevel, index)
      ExprCode("", nullVarName, varName) -> sf.dataType
  }.unzip


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

  /**
   * Generate code to calculate the hash code for given column variables that
   * correspond to the key columns in this class.
   */
  def generateHashCode(hashVar: Array[String], keyVars: Seq[ExprCode],
    keyExpressions: Seq[Expression], keysDataType: Seq[DataType],
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
    if (keyVars.length > 1) {
      keysDataType.tail.zip(keyVars.tail).map {
        case (BooleanType, ev) =>
          addHashInt(s"${ev.value} ? 1 : 0", ev.isNull, hash)
        case (ByteType | ShortType | IntegerType | DateType, ev) =>
          addHashInt(ev.value, ev.isNull, hash)
        case (BinaryType, ev) =>
          hashBinary(ev.value, ev.isNull, hash)
        case (LongType | TimestampType, ev) =>
          addHashLong(ctx, ev.value, ev.isNull, hash)
        case (FloatType, ev) =>
          addHashInt(s"Float.floatToIntBits(${ev.value})", ev.isNull, hash)
        case (DoubleType, ev) =>
          addHashLong(ctx, s"Double.doubleToLongBits(${ev.value})", ev.isNull,
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

  def calculateNumberOfBytesForNullBits(numAttributes: Int): Int = numAttributes / 8 +
    (if (numAttributes % 8 > 0) 1 else 0)

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

}