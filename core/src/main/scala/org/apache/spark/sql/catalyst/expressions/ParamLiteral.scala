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

package org.apache.spark.sql.catalyst.expressions

import java.util.concurrent.ConcurrentHashMap
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.shared.ClientResolverUtils
import org.json4s.JsonAST.JField

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

case class TermValues(literalValueRef : String, isNull : String, valueTerm : String)
// A marker interface to extend usage of Literal case matching.
// A literal that can change across multiple query execution.
trait DynamicReplacableConstant extends Expression {

  @transient private lazy val termMap = new mutable.HashMap[CodegenContext, TermValues]

  def value: Any

  override final def deterministic: Boolean = true

  private def checkValueType(value: Any, expectedClass: Class[_]): Unit = {
    val valueClass = if (value != null) value.getClass else null
    assert((valueClass eq expectedClass) || (valueClass eq null),
      s"Unexpected data type $dataType for value type: $valueClass")
  }

  override final def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = this.value
    assert(value != null || nullable, "Expected nullable as true when value is null")
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
      val tvOption = termMap.get(ctx)
      assert(tvOption.isDefined)
      tvOption.get
    }
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
             |if (($isNull = $valueRef.value() == null)) {
             |  $valueTerm = ${ctx.defaultValue(dataType)};
             |} else {
             |  $valueTerm = ($box)$valueRef.value();
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
           |$isNull = $valueRef.value() == null;
           |$valueTerm = $isNull ? ${ctx.defaultValue(dataType)} : (($box)$valueRef.value())$unbox;
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
 * see SNAP-1597 for more details.
 */
final case class ParamLiteral(var value: Any, var dataType: DataType,
    var pos: Int, @transient private[sql] val execId: Int,
    private[sql] var tokenized: Boolean = false,
    private[sql] var positionIndependent: Boolean = false,
    @transient private[sql] var valueEquals: Boolean = false)
    extends TokenizedLiteral with KryoSerializable {

  override def nullable: Boolean = dataType eq NullType

  override def eval(input: InternalRow): Any = value

  override def nodeName: String = "ParamLiteral"

  override def prettyName: String = "ParamLiteral"

  def asLiteral: TokenLiteral = new TokenLiteral(value, dataType)

  override protected def jsonFields: List[JField] = asLiteral.jsonFields

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
    case binary: Array[Byte] => s"0x" + DatatypeConverter.printHexBinary(binary)
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
    case Cast(child, dataType) => {
      val isConstant = child match {
        case _: DynamicReplacableConstant | _: Literal => true
        case _ => false
      }
      isConstant & dataType.isInstanceOf[AtomicType]
    }
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
  protected final var paramListId = 0

  private[sql] final def getAllLiterals: Array[ParamLiteral] = parameterizedConstants.toArray

  private[sql] final def getCurrentParamsId: Int = paramListId

  private[sql] final def addParamLiteralToContext(value: Any,
      dataType: DataType): ParamLiteral = {
    val p = ParamLiteral(value, dataType, parameterizedConstants.length, paramListId)
    parameterizedConstants += p
    p
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
  }

  private[sql] final def clearConstants(): Unit = {
    parameterizedConstants.clear()
    paramListId += 1
  }
}

final class DirectStringConsumer(memoryManager: TaskMemoryManager, pageSize: Int)
    extends MemoryConsumer(memoryManager, pageSize, MemoryMode.OFF_HEAP) {

  def this(memoryManager: TaskMemoryManager) = this(memoryManager, 8)

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  def copyUTF8String(s: UTF8String): UTF8String = {
    if ((s ne null) && (s.getBaseObject ne null)) {
      val size = s.numBytes()
      val page = taskMemoryManager.allocatePage(Math.max(pageSize, size), this)
      if ((page ne null) && page.size >= size) {
        used += page.size
        val ds = UTF8String.fromAddress(null, page.getBaseOffset, size)
        Platform.copyMemory(s.getBaseObject, s.getBaseOffset, null, ds.getBaseOffset, size)
        return ds
      } else if (page ne null) {
        taskMemoryManager.freePage(page, this)
      }
    }
    s
  }
}

/**
 * Wrap any TokenizedLiteral expression with this so that we can invoke literal
 * initialization code within the <code>.init()</code> method of the generated class.
 * <br><br>
 *
 * This pushes itself as reference object and uses a call to eval() on itself for actual
 * evaluation and avoids embedding any generated code. This allows it to keep the
 * generated code identical regardless of the constant expression (and in addition
 *   DynamicReplacableConstant trait casts to itself rather than actual object type).
 * <br><br>
 *
 * We try to locate first foldable expression in a query tree such that all its child is foldable
 * but parent isn't. That way we locate the exact point where an expression is safe to evaluate
 * once instead of evaluating every row.
 * <br><br>
 *
 * Expressions like <code> select c from tab where
 *  case col2 when 1 then col3 else 'y' end = 22 </code>
 * like queries don't convert literal evaluation into init method.
 *
 * @param expr minimal expression tree that can be evaluated only once and turn into a constant.
 */
case class DynamicFoldableExpression(var expr: Expression) extends UnaryExpression
    with DynamicReplacableConstant with KryoSerializable {

  override def checkInputDataTypes(): TypeCheckResult = expr.checkInputDataTypes()

  override def child: Expression = expr

  override def eval(input: InternalRow): Any = expr.eval(input)

  override def dataType: DataType = expr.dataType

  override def value: Any = eval(EmptyRow)

  override def nodeName: String = "DynamicExpression"

  override def prettyName: String = "DynamicExpression"

  override def toString: String = {
    def removeCast(expr: Expression): Expression = expr match {
      case Cast(child, _) => removeCast(child)
      case _ => expr
    }
    "DynExpr(" + removeCast(expr) + ")"
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    assert(newArgs.length == 1)
    if (newArgs(0) eq expr) this
    else DynamicFoldableExpression(newArgs(0).asInstanceOf[Expression])
  }

  override def withNewChildren(newChildren: Seq[Expression]): Expression = {
    assert(newChildren.length == 1)
    if (newChildren.head ne expr) expr = newChildren.head
    this
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    StructTypeSerializer.writeType(kryo, output, dataType)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val value = kryo.readClassAndObject(input)
    val dateType = StructTypeSerializer.readType(kryo, input)
    expr = new TokenLiteral(value, dateType)
  }
}

/**
 * Unlike Spark's InSet expression, this allows for TokenizedLiterals that can
 * change dynamically in executions.
 */
case class DynamicInSet(child: Expression, hset: IndexedSeq[Expression])
    extends UnaryExpression with Predicate {

  require((hset ne null) && hset.nonEmpty, "hset cannot be null or empty")
  // all expressions must be constant types
  require(hset.forall(TokenLiteral.isConstant), "hset can only have constant expressions")

  override def toString: String =
    s"$child DynInSet ${hset.mkString("(", ",", ")")}"

  override def nullable: Boolean = hset.exists(_.nullable)

  @transient private lazy val (hashSet, hasNull) = {
    val m = new ConcurrentHashMap[AnyRef, AnyRef](hset.length)
    var hasNull = false
    for (e <- hset) {
      val v = e.eval(EmptyRow).asInstanceOf[AnyRef]
      if (v ne null) {
        m.put(v, v)
      } else if (!hasNull) {
        hasNull = true
      }
    }
    (m, hasNull)
  }

  protected override def nullSafeEval(value: Any): Any = {
    if (hashSet.contains(value)) {
      true
    } else if (hasNull) {
      null
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // JDK8 ConcurrentHashMap consistently clocks fastest for gets among all
    val setName = classOf[ConcurrentHashMap[AnyRef, AnyRef]].getName
    val exprClass = classOf[Expression].getName
    val elements = new Array[AnyRef](hset.length)
    val childGen = child.genCode(ctx)
    val hsetTerm = ctx.freshName("hset")
    val elementsTerm = ctx.freshName("elements")
    val idxTerm = ctx.freshName("idx")
    val idx = ctx.references.length
    ctx.references += elements
    val hasNullTerm = ctx.freshName("hasNull")

    for (i <- hset.indices) {
      val e = hset(i)
      val v = e match {
        case d: DynamicReplacableConstant => d
        case _ => e.eval(EmptyRow).asInstanceOf[AnyRef]
      }
      elements(i) = v
    }

    ctx.addMutableState("boolean", hasNullTerm, "")
    ctx.addMutableState(setName, hsetTerm,
      s"""
         |Object[] $elementsTerm = (Object[])references[$idx];
         |$hsetTerm = new $setName($elementsTerm.length, 0.7f, 1);
         |for (int $idxTerm = 0; $idxTerm < $elementsTerm.length; $idxTerm++) {
         |  Object e = $elementsTerm[$idxTerm];
         |  if (e instanceof $exprClass) e = (($exprClass)e).eval(null);
         |  if (e != null) {
         |    $hsetTerm.put(e, e);
         |  } else if (!$hasNullTerm) {
         |    $hasNullTerm = true;
         |  }
         |}
      """.stripMargin)

    ev.copy(code = s"""
      ${childGen.code}
      boolean ${ev.isNull} = ${childGen.isNull};
      boolean ${ev.value} = false;
      if (!${ev.isNull}) {
        ${ev.value} = $hsetTerm.containsKey(${childGen.value});
        if (!${ev.value} && $hasNullTerm) {
          ${ev.isNull} = true;
        }
      }
     """)
  }

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = hset.map(_.eval(EmptyRow)).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}
