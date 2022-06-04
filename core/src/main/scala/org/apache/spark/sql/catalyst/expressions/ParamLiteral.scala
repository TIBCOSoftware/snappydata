/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.catalyst.expressions

import java.util
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.shared.ClientResolverUtils
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.json4s.JsonAST.JField

import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class TermValues(literalValueRef: String, isNull: String, valueTerm: String)

// A marker interface to extend usage of Literal case matching.
// A literal that can change across multiple query execution.
trait DynamicReplacableConstant extends Expression {

  @transient private lazy val termMap =
    java.util.Collections.synchronizedMap(new util.HashMap[CodegenContext, TermValues]())

  def value: Any

  /**
   * Used only for checking types and can be overridden by implementations to return
   * dummy result (like null) whose [[value]] evaluation can be potentially expensive.
   */
  private[sql] def getValueForTypeCheck: Any = {
    val value = this.value
    assert(value != null || nullable, "Expected nullable as true when value is null")
    value
  }

  override final def deterministic: Boolean = true

  private def checkValueType(value: Any, expectedClass: Class[_]): Unit = {
    val valueClass = if (value != null) value.getClass else null
    assert((valueClass eq expectedClass) || (valueClass eq null),
      s"Unexpected data type $dataType for value type: $valueClass")
  }

  override final def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = getValueForTypeCheck
    val addMutableState = !ctx.references.exists(_.asInstanceOf[AnyRef] eq this)
    val termValues = if (addMutableState) {
      val literalValueRef = ctx.addReferenceObj("literal", this,
        classOf[DynamicReplacableConstant].getName)
      val isNull = ctx.freshName("isNull")
      val valueTerm = ctx.freshName("value")
      val tv = TermValues(literalValueRef, isNull, valueTerm)
      termMap.put(ctx, tv)
      tv
    } else {
      val tv = termMap.get(ctx)
      assert(tv != null)
      tv
    }
    // temporary variable for storing value() result for cases where it can be
    // potentially expensive (e.g. for DynamicFoldableExpression)
    val valueResult = ctx.freshName("valueResult")
    val isNullLocal = ev.isNull
    val valueLocal = ev.value
    val dataType = Utils.getSQLDataType(this.dataType)
    val javaType = ctx.javaType(dataType)
    // get values from map
    val isNull = termValues.isNull
    val valueTerm = termValues.valueTerm
    val literalValueRef = termValues.literalValueRef
    val initCode =
      s"""
         |final boolean $isNullLocal = $isNull;
         |final $javaType $valueLocal = $valueTerm;
      """.stripMargin

    if (!addMutableState) {
      // use the already added fields
      return ev.copy(initCode, isNullLocal, valueLocal)
    }
    val valueRef = literalValueRef
    val box = ctx.boxedType(javaType)

    val unbox = dataType match {
      case BooleanType =>
        checkValueType(value, classOf[java.lang.Boolean])
        ".booleanValue()"
      case FloatType =>
        checkValueType(value, classOf[java.lang.Float])
        ".floatValue()"
      case DoubleType =>
        checkValueType(value, classOf[java.lang.Double])
        ".doubleValue()"
      case ByteType =>
        checkValueType(value, classOf[java.lang.Byte])
        ".byteValue()"
      case ShortType =>
        checkValueType(value, classOf[java.lang.Short])
        ".shortValue()"
      case _: IntegerType | _: DateType =>
        checkValueType(value, classOf[java.lang.Integer])
        ".intValue()"
      case _: TimestampType | _: LongType =>
        checkValueType(value, classOf[java.lang.Long])
        ".longValue()"
      case StringType =>
        // allocate UTF8String on off-heap so that Native can be used if possible
        checkValueType(value, classOf[UTF8String])

        val getContext = Utils.genTaskContextFunction(ctx)
        val memoryManagerClass = classOf[TaskMemoryManager].getName
        val memoryModeClass = classOf[MemoryMode].getName
        val consumerClass = classOf[DirectStringConsumer].getName
        ctx.addMutableState(javaType, valueTerm,
          s"""
             |Object $valueResult = $valueRef.value();
             |if (($isNull = ($valueResult == null))) {
             |  $valueTerm = ${ctx.defaultValue(dataType)};
             |} else {
             |  $valueTerm = ($box)$valueResult;
             |  if (com.gemstone.gemfire.internal.cache.GemFireCacheImpl.hasNewOffHeap() &&
             |      $getContext() != null) {
             |    // convert to off-heap value if possible
             |    $memoryManagerClass mm = $getContext().taskMemoryManager();
             |    if (mm.getTungstenMemoryMode() == $memoryModeClass.OFF_HEAP) {
             |      $consumerClass consumer = new $consumerClass(mm);
             |      $valueTerm = consumer.copyUTF8String($valueTerm);
             |    }
             |  }
             |}
          """.stripMargin)
        // indicate that code for valueTerm has already been generated
        null.asInstanceOf[String]
      case _ => ""
    }
    ctx.addMutableState("boolean", isNull, "")
    if (unbox ne null) {
      ctx.addMutableState(javaType, valueTerm,
        s"""
           |Object $valueResult = $valueRef.value();
           |$isNull = $valueResult == null;
           |$valueTerm = $isNull ? ${ctx.defaultValue(dataType)} : (($box)$valueResult)$unbox;
        """.stripMargin)
    }
    ev.copy(initCode, isNullLocal, valueLocal)
  }
}

trait TokenizedLiteral extends LeafExpression with DynamicReplacableConstant {

  protected final var _foldable: Boolean = _

  // avoid constant folding and let it be done by TokenizedLiteralFolding
  // so that generated code does not embed constants when there are constant
  // expressions (common case being a CAST when literal type does not match exactly)
  override final def foldable: Boolean = _foldable

  def valueString: String

  final def markFoldable(b: Boolean): TokenizedLiteral = {
    _foldable = b
    this
  }

  override final def makeCopy(newArgs: Array[AnyRef]): Expression = {
    assert(newArgs.length == 0)
    this
  }

  override final def withNewChildren(newChildren: Seq[Expression]): Expression = {
    assert(newChildren.isEmpty)
    this
  }
}

/**
 * A Literal that passes its value as a reference object in generated code instead
 * of embedding as a constant to allow generated code reuse.
 */
final class TokenLiteral(_value: Any, _dataType: DataType)
    extends Literal(_value, _dataType) with TokenizedLiteral with KryoSerializable {

  override def valueString: String = toString()

  override def jsonFields: List[JField] = super.jsonFields

  override def equals(other: Any): Boolean = other match {
    case l: Literal => foldable == l.foldable && super.equals(other)
    case _ => super.equals(other)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    StructTypeSerializer.writeType(kryo, output, dataType)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    TokenLiteral.valueField.set(this, kryo.readClassAndObject(input))
    TokenLiteral.dataTypeField.set(this, StructTypeSerializer.readType(kryo, input))
  }
}

/**
 * In addition to [[TokenLiteral]], this class can also be used in plan caching
 * so allows for internal value to be updated in subsequent runs when the
 * plan is re-used with different constants. For that reason this does not
 * extend Literal (to avoid Analyzer/Optimizer etc doing constant propagation
 * for example) and its hash/equals ignores the value matching and only the position
 * of the literal in the plan is used with the data type.
 *
 * Where ever ParamLiteral case matching is required, it must match
 * for DynamicReplacableConstant and use .eval(..) for code generation.
 * see SNAP-1597 for more details. For cases of common-subexpression elimination
 * that depend on constant values being equal in different parts of the tree,
 * a new RefParamLiteral has been added that points to a ParamLiteral and is always
 * equal to it, see SNAP-2462 for more details.
 */
case class ParamLiteral(var value: Any, var dataType: DataType,
    var pos: Int, @transient private[sql] val execId: Int,
    private[sql] var tokenized: Boolean = false,
    private[sql] var positionIndependent: Boolean = false,
    @transient private[sql] var valueEquals: Boolean = false)
    extends TokenizedLiteral with KryoSerializable {

  override def nullable: Boolean = dataType.isInstanceOf[NullType]

  override def eval(input: InternalRow): Any = value

  override def nodeName: String = "ParamLiteral"

  override def prettyName: String = "ParamLiteral"

  def asLiteral: TokenLiteral = new TokenLiteral(value, dataType)

  override protected[sql] def jsonFields: List[JField] = asLiteral.jsonFields

  override def sql: String = asLiteral.sql

  override def hashCode(): Int = {
    if (tokenized) {
      if (positionIndependent) dataType.hashCode()
      else ClientResolverUtils.addIntToHashOpt(pos, dataType.hashCode())
    } else {
      val valueHashCode = value match {
        case null => 0
        case binary: Array[Byte] => java.util.Arrays.hashCode(binary)
        case other => other.hashCode()
      }
      ClientResolverUtils.addIntToHashOpt(valueHashCode, dataType.hashCode())
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: AnyRef if this eq a => true
    case r: RefParamLiteral if r.param ne null => r.referenceEquals(this)
    case l: ParamLiteral =>
      // match by position only if "tokenized" else value comparison (no-caching case)
      if (tokenized && !valueEquals) pos == l.pos && dataType == l.dataType
      else dataType == l.dataType && valueEquals(l)
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = equals(other)

  private def valueEquals(p: ParamLiteral): Boolean = value match {
    case null => p.value == null
    case a: Array[Byte] => p.value match {
      case b: Array[Byte] => java.util.Arrays.equals(a, b)
      case _ => false
    }
    case _ => value.equals(p.value)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    StructTypeSerializer.writeType(kryo, output, dataType)
    output.writeVarInt(pos, true)
    output.writeBoolean(tokenized)
    output.writeBoolean(positionIndependent)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    value = kryo.readClassAndObject(input)
    dataType = StructTypeSerializer.readType(kryo, input)
    pos = input.readVarInt(true)
    tokenized = input.readBoolean()
    positionIndependent = input.readBoolean()
  }

  override def valueString: String = value match {
    case null => "null"
    case binary: Array[Byte] => "0x" + DatatypeConverter.printHexBinary(binary)
    case other => other.toString
  }

  override def toString: String = {
    // add the length of value in string for easy replace later
    val sb = new StringBuilder
    sb.append(TokenLiteral.PARAMLITERAL_START).append(pos).append(',').append(execId)
    val valStr = valueString
    sb.append('#').append(valStr.length).append(',').append(valStr)
    sb.toString()
  }
}

/**
 * This class is used as a substitution for ParamLiteral when two
 * ParamLiterals have same constant values during parsing.
 * This behaves like being equal to the ParamLiteral it points
 * to in all respects but will be different from other ParamLiterals.
 * Two RefParamLiterals will be equal iff their respective ParamLiterals are.
 *
 * The above policy allows an expression like "a = 4 and b = 4" to be equal to
 * "a = 5 and b = 5" after tokenization but will be different from
 * "a = 5 and b = 6". This distinction is required because former can
 * lead to a different execution plan after common-subexpression processing etc
 * that can apply on because the actual values for the two tokenized values are equal
 * in this instance. Hence it can lead to a different plan in case where actual constants
 * are different, so after tokenization they should act as different expressions.
 * See TPCH Q19 for an example where equal values in two different positions
 * lead to an optimized plan due to common-subexpression being pulled out of
 * OR conditions as a separate AND condition which leads to further filter
 * push down which is not possible if the actual values are different.
 *
 * Note: This class maintains its own copy of value since it can change
 * in execution (e.g. ROUND can change precision of underlying Decimal value)
 * which should not lead to a change of value of referenced ParamLiteral or vice-versa.
 * However, during planning, code generation and other phases before runJob,
 * the value and dataType should match exactly which is checked by referenceEquals.
 * After deserialization on remote executor, the class no longer maintains a reference
 * and falls back to behaving like a regular ParamLiteral since the required analysis and
 * other phases are already done, and final code generation requires a copy of the values.
 */
final class RefParamLiteral(val param: ParamLiteral, _value: Any, _dataType: DataType, _pos: Int)
    extends ParamLiteral(_value, _dataType, _pos, execId = param.execId) {

  assert(!param.isInstanceOf[RefParamLiteral])

  private[sql] def referenceEquals(p: ParamLiteral): Boolean = {
    if (param eq p) {
      // Check that value and dataType should also be equal at this point.
      // These can potentially change during execution due to application
      // of functions on the value like ROUND.
      assert(value == p.value)
      assert(dataType == p.dataType)
      true
    } else false
  }

  override def hashCode(): Int = if (param ne null) param.hashCode() else super.hashCode()

  override def equals(obj: Any): Boolean = {
    if (param ne null) obj match {
      case a: AnyRef if this eq a => true
      case r: RefParamLiteral => param == r.param
      case l: ParamLiteral => referenceEquals(l)
      case _ => false
    } else super.equals(obj)
  }
}

object TokenLiteral {

  private val valueField = {
    val f = classOf[Literal].getDeclaredField("value")
    f.setAccessible(true)
    f
  }
  private val dataTypeField = {
    val f = classOf[Literal].getDeclaredField("dataType")
    f.setAccessible(true)
    f
  }

  val PARAMLITERAL_START = "ParamLiteral:"

  def unapply(expression: Expression): Option[Any] = expression match {
    case l: DynamicReplacableConstant => Some(convertToScala(l.value, l.dataType))
    case Literal(v, t) => Some(convertToScala(v, t))
    case _ => None
  }

  def isConstant(expression: Expression): Boolean = expression match {
    case _: DynamicReplacableConstant | _: Literal => true
    case Cast(child, dataType) =>
      val isConstant = child match {
        case _: DynamicReplacableConstant | _: Literal => true
        case _ => false
      }
      isConstant & dataType.isInstanceOf[AtomicType]
    case _ => false
  }

  def newToken(value: Any): TokenLiteral = {
    val l = Literal(value)
    new TokenLiteral(l.value, l.dataType)
  }
}

trait ParamLiteralHolder {

  @transient
  private final val parameterizedConstants = new mutable.ArrayBuffer[ParamLiteral](4)
  @transient
  private final var paramConstantMap: Object2ObjectOpenHashMap[(DataType, Any), ParamLiteral] = _
  @transient
  protected final var paramListId = 0

  private[sql] final def getAllLiterals: Array[ParamLiteral] = parameterizedConstants.toArray

  private[sql] final def getCurrentParamsId: Int = paramListId

  /**
   * Find existing ParamLiteral with given value and DataType. This should
   * never return a RefParamLiteral.
   */
  private def findExistingParamLiteral(value: Any, dataType: DataType,
      numConstants: Int): Option[ParamLiteral] = {
    // for size >= 4 use a lookup map to search for same constant else linear search
    if (numConstants >= 4) {
      if (paramConstantMap eq null) {
        // populate the map while checking for a match
        paramConstantMap = new Object2ObjectOpenHashMap(8)
        var i = 0
        var existing: Option[ParamLiteral] = None
        while (i < numConstants) {
          parameterizedConstants(i) match {
            case _: RefParamLiteral => // skip
            case param =>
              if (existing.isEmpty && dataType == param.dataType && value == param.value) {
                existing = Some(param)
              }
              paramConstantMap.put(param.dataType -> param.value, param)
          }
          i += 1
        }
        existing
      } else Option(paramConstantMap.get(dataType -> value))
    } else {
      var i = 0
      while (i < numConstants) {
        parameterizedConstants(i) match {
          case _: RefParamLiteral => // skip
          case param =>
            if (dataType == param.dataType && value == param.value) {
              return Some(param)
            }
        }
        i += 1
      }
      None
    }
  }

  private[sql] final def addParamLiteralToContext(value: Any,
      dataType: DataType): ParamLiteral = {
    val numConstants = parameterizedConstants.length
    findExistingParamLiteral(value, dataType, numConstants) match {
      case None =>
        val p = ParamLiteral(value, dataType, numConstants, paramListId)
        parameterizedConstants += p
        if (paramConstantMap ne null) paramConstantMap.put(dataType -> value, p)
        p
      case Some(existing) =>
        // Add to parameterizedConstants list so that its position can be updated
        // if required (e.g. if a ParamLiteral is reverted to a Literal for
        //   functions that require so as in SnappyParserConsts.FOLDABLE_FUNCTIONS)
        // In addition RefParamLiteral maintains its own copy of value to avoid updating
        // the referenced ParamLiteral's value by functions like ROUND, so that needs to
        // be changed too when a plan with updated tokens is created.
        val ref = new RefParamLiteral(existing, value, dataType, numConstants)
        parameterizedConstants += ref
        ref
    }
  }

  private[sql] final def removeIfParamLiteralFromContext(l: TokenizedLiteral): Unit = l match {
    case pl: ParamLiteral => removeParamLiteralFromContext(pl)
    case _ =>
  }

  private[sql] final def removeParamLiteralFromContext(p: ParamLiteral): Unit = {
    val paramConstants = parameterizedConstants
    var pos = p.pos
    assert(paramConstants(pos) eq p)
    val newLen = paramConstants.length - 1
    if (pos == newLen) paramConstants.reduceToSize(pos)
    else {
      paramConstants.remove(pos)
      // correct the positions of ParamLiterals beyond this position
      while (pos < newLen) {
        val p = paramConstants(pos)
        p.pos -= 1
        assert(p.pos == pos)
        pos += 1
      }
    }
    if ((paramConstantMap ne null) && !p.isInstanceOf[RefParamLiteral]) {
      assert(paramConstantMap.remove(p.dataType -> p.value) eq p)
    }
  }

  private[sql] final def clearConstants(): Unit = {
    parameterizedConstants.clear()
    if (paramConstantMap ne null) paramConstantMap = null
    paramListId += 1
  }
}
