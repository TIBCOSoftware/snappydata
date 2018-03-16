/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.util.Objects

import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.shared.ClientResolverUtils
import io.snappydata.collection.OpenHashSet
import org.json4s.JsonAST.JField

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, SubExprEliminationState}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

// A marker interface to extend usage of Literal case matching.
// A literal that can change across multiple query execution.
trait DynamicReplacableConstant {

  def dataType: DataType

  def eval(input: InternalRow = null): Any

  def convertedLiteral: Any
}

trait TokenizedLiteral extends LeafExpression {

  protected final var _foldable: Boolean = _

  // avoid constant folding and let it be done by TokenizedLiteralFolding
  // so that generated code does not embed constants when there are constant
  // expressions (common case being a CAST when literal type does not match exactly)
  override def foldable: Boolean = _foldable

  def value: Any

  def valueString: String

  protected def literalValue: LiteralValue

  override final def deterministic: Boolean = true

  def markFoldable(b: Boolean): TokenizedLiteral = {
    _foldable = b
    this
  }

  @transient protected final var literalValueRef: String = _

  @transient protected final var isNull: String = _
  @transient protected final var valueTerm: String = _

  protected final def lv(c: CodegenContext): String =
    if (c.references.exists(_.asInstanceOf[AnyRef] eq literalValue)) {
      assert(literalValueRef != null)
      literalValueRef
    } else {
      literalValueRef = c.addReferenceObj("literal", literalValue)
      literalValueRef
    }

  override final def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = this.value
    val addMutableState = (isNull eq null) || !ctx.mutableStates.exists(_._2 == isNull)
    if (addMutableState) {
      isNull = ctx.freshName("isNullTerm")
      valueTerm = ctx.freshName("valueTerm")
    }
    val isNullLocal = ev.isNull
    val valueLocal = ev.value
    val dataType = Utils.getSQLDataType(this.dataType)
    val javaType = ctx.javaType(dataType)
    val initCode =
      s"""
         |final boolean $isNullLocal = $isNull;
         |final $javaType $valueLocal = $valueTerm;
      """.stripMargin
    if (!addMutableState) {
      // use the already added fields
      return ev.copy(initCode, isNullLocal, valueLocal)
    }
    val valueRef = lv(ctx)
    val box = ctx.boxedType(javaType)

    val unbox = dataType match {
      case BooleanType =>
        assert(value.isInstanceOf[Boolean], s"unexpected type $dataType instead of BooleanType")
        ".booleanValue()"
      case FloatType =>
        assert(value.isInstanceOf[Float], s"unexpected type $dataType instead of FloatType")
        ".floatValue()"
      case DoubleType =>
        assert(value.isInstanceOf[Double], s"unexpected type $dataType instead of DoubleType")
        ".doubleValue()"
      case ByteType =>
        assert(value.isInstanceOf[Byte], s"unexpected type $dataType instead of ByteType")
        ".byteValue()"
      case ShortType =>
        assert(value.isInstanceOf[Short], s"unexpected type $dataType instead of ShortType")
        ".shortValue()"
      case t@(IntegerType | DateType) =>
        assert(value.isInstanceOf[Int], s"unexpected type $dataType instead of $t")
        ".intValue()"
      case t@(TimestampType | LongType) =>
        assert(value.isInstanceOf[Long], s"unexpected type $dataType instead of $t")
        ".longValue()"
      case StringType =>
        // allocate UTF8String on off-heap so that Native can be used if possible
        assert(value.isInstanceOf[UTF8String],
          s"unexpected type $dataType instead of UTF8String")

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

/**
 * A Literal that passes its value as a reference object in generated code instead
 * of embedding as a constant to allow generated code reuse.
 */
final class TokenLiteral(_value: Any, _dataType: DataType)
    extends Literal(_value, _dataType) with TokenizedLiteral {

  @transient protected lazy val literalValue: LiteralValue = new LiteralValue(value)

  override def valueString: String = toString()

  override def jsonFields: List[JField] = super.jsonFields
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
 *
 * Having LiteralValue as case class parameter avoids copy of LiteralValue
 * being made when ParamLiteral is copied, so the original LiteralValues will
 * stay intact whether or not the ParamLiteral does.
 */
final case class ParamLiteral(protected[sql] var literalValue: LiteralValue,
    var dataType: DataType, var pos: Int, private var normalized: Boolean)
    extends LeafExpression with TokenizedLiteral
        with DynamicReplacableConstant with KryoSerializable {

  @transient private[this] lazy val converter = createToScalaConverter(dataType)

  def value: Any = literalValue.value

  private[sql] def updateValue(value: Any): Unit = literalValue.value = value

  override def nullable: Boolean = true // runtime value can be null

  override def eval(input: InternalRow): Any = value

  override def convertedLiteral: Any = converter(value)

  private def asLiteral: TokenLiteral = new TokenLiteral(value, dataType)

  override protected def jsonFields: List[JField] = asLiteral.jsonFields

  override def sql: String = asLiteral.sql

  override def hashCode(): Int = {
    if (normalized) {
      ClientResolverUtils.fastHashLong(dataType.hashCode().toLong << 32L | (pos & 0xffffffffL))
    } else {
      ClientResolverUtils.fastHashLong(dataType.hashCode().toLong << 32L |
          (Objects.hashCode(value) & 0xffffffffL))
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: AnyRef if this eq a => true
    case l: ParamLiteral =>
      // match by position only if "normalized" else value comparison (no-caching case)
      if (normalized) l.pos == pos && l.dataType == dataType
      else l.dataType == dataType && l.value == value
    case _ => false
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    literalValue.write(kryo, output)
    StructTypeSerializer.writeType(kryo, output, dataType)
    output.writeVarInt(pos, true)
    output.writeBoolean(normalized)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    literalValue = new LiteralValue(null)
    literalValue.read(kryo, input)
    dataType = StructTypeSerializer.readType(kryo, input)
    pos = input.readVarInt(true)
    normalized = input.readBoolean()
  }

  override def valueString: String = asLiteral.toString()

  override def toString: String = {
    // add the length of value in string for easy replace later
    val sb = new StringBuilder
    sb.append(ParamLiteral.PARAMLITERAL_START).append(pos)
    val valStr = valueString
    sb.append('#').append(valStr.length).append(',').append(valStr)
    sb.toString()
  }
}

object ParamLiteral {

  def apply(value: Any, pos: Int): ParamLiteral = {
    val l = Literal(value)
    apply(l.value, l.dataType, pos)
  }

  def apply(value: Any, dataType: DataType, pos: Int, normalized: Boolean = false): ParamLiteral =
    ParamLiteral(new LiteralValue(value), dataType, pos, normalized)

  val PARAMLITERAL_START = "ParamLiteral:"
}

trait ParamLiteralHolder {

  @transient
  protected final val parameterizedConstants: ArrayBuffer[ParamLiteral] =
    new ArrayBuffer[ParamLiteral](4)

  private[sql] final def getAllLiterals: Array[ParamLiteral] = parameterizedConstants.toArray

  private[sql] final def addParamLiteralToContext(value: Any,
      dataType: DataType): ParamLiteral = {
    val p = ParamLiteral(value, dataType, parameterizedConstants.length, normalized = true)
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

final case class LiteralValue(private[sql] var value: Any) extends KryoSerializable {

  def getValue: Any = value

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    value = kryo.readClassAndObject(input)
  }
}

object LiteralValue {

  def unapply(expression: Expression): Option[Any] = expression match {
    case l: DynamicReplacableConstant => Some(l.convertedLiteral)
    case Literal(v, t) => Some(convertToScala(v, t))
    case _ => None
  }

  def isConstant(expression: Expression): Boolean = expression match {
    case _: DynamicReplacableConstant | _: Literal => true
    case _ => false
  }
}

/**
 * Wrap any TokenizedLiteral expression with this so that we can generate literal
 * initialization code within the <code>.init()</code> method of the generated class.
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
case class DynamicFoldableExpression(expr: Expression) extends Expression
    with DynamicReplacableConstant {
  override def nullable: Boolean = expr.nullable

  override def eval(input: InternalRow): Any = expr.eval(input)

  def convertedLiteral: Any = createToScalaConverter(dataType)(eval(null))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // subexpression elimination can lead to local variables but this expression
    // needs mutable state, so disable it (extra evaluations only once in init in any case)
    val numSubExprs = ctx.subExprEliminationExprs.size
    val oldSubExprs = if (numSubExprs != 0) {
      val exprs = new ArrayBuffer[(Expression, SubExprEliminationState)](numSubExprs)
      exprs ++= ctx.subExprEliminationExprs
      ctx.subExprEliminationExprs.clear()
      exprs
    } else null
    val eval = expr.genCode(ctx)
    if (oldSubExprs ne null) {
      ctx.subExprEliminationExprs ++= oldSubExprs
    }
    val newVar = ctx.freshName("tokenizedLiteralExpr")
    val newVarIsNull = ctx.freshName("tokenizedLiteralExprIsNull")
    val comment = ctx.registerComment(expr.toString)
    // initialization for both variable and isNull is being done together
    // due to dependence of latter on the variable and the two get
    // separated due to Spark's splitExpressions -- SNAP-1794
    ctx.addMutableState(ctx.javaType(expr.dataType), newVar,
      s"$comment\n${eval.code}\n$newVar = ${eval.value};\n" +
        s"$newVarIsNull = ${eval.isNull};")
    ctx.addMutableState("boolean", newVarIsNull, "")
    // allow sub-expression elimination of this expression itself
    ctx.subExprEliminationExprs += this -> SubExprEliminationState(newVarIsNull, newVar)
    ev.copy(code = "", value = newVar, isNull = newVarIsNull)
  }

  override def dataType: DataType = expr.dataType

  override def children: Seq[Expression] = Seq(expr)

  override def nodeName: String = "DynamicExpression"

  override def prettyName: String = "DynamicExpression"

  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = {
    new TokenLiteral(eval(null), dataType)
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
  require(hset.forall(LiteralValue.isConstant), "hset can only have constant expressions")

  override def toString: String =
    s"$child DynInSet ${hset.mkString("(", ",", ")")}"

  override def nullable: Boolean = true // can have null value at runtime

  @transient private lazy val (hashSet, hasNull) = {
    val m = new OpenHashSet[AnyRef](hset.length)
    var hasNull = false
    for (e <- hset) {
      val v = e.eval(EmptyRow).asInstanceOf[AnyRef]
      if (!hasNull && (v eq null)) {
        hasNull = true
      }
      m.add(v)
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
    val setName = classOf[OpenHashSet[AnyRef]].getName
    val exprClass = classOf[Expression].getName
    val lvalClass = classOf[LiteralValue].getName
    val elements = new Array[AnyRef](hset.length)
    val childGen = child.genCode(ctx)
    val hsetTerm = ctx.freshName("hset")
    val elementsTerm = ctx.freshName("elements")
    val idx = ctx.references.length
    ctx.references += elements
    val hasNullTerm = ctx.freshName("hasNull")

    for (i <- hset.indices) {
      val e = hset(i)
      val v = e match {
        case p: ParamLiteral => p.literalValue
        case d: DynamicFoldableExpression => d
        case _ => e.eval(EmptyRow).asInstanceOf[AnyRef]
      }
      elements(i) = v
    }

    ctx.addMutableState("boolean", hasNullTerm, "")
    ctx.addMutableState("Object[]", elementsTerm, "")
    ctx.addMutableState(setName, hsetTerm,
      s"""
         |$elementsTerm = (Object[])references[$idx];
         |$hsetTerm = new $setName($elementsTerm.length);
         |for (int i = 0; i < $elementsTerm.length; i++) {
         |  Object e = $elementsTerm[i];
         |  if (e instanceof $lvalClass) e = (($lvalClass)e).getValue();
         |  else if (e instanceof $exprClass) e = (($exprClass)e).eval(null);
         |  if (e == null && !$hasNullTerm) $hasNullTerm = true;
         |  $hsetTerm.add(e);
         |}
      """.stripMargin)

    ev.copy(code = s"""
      ${childGen.code}
      boolean ${ev.isNull} = ${childGen.isNull};
      boolean ${ev.value} = false;
      if (!${ev.isNull}) {
        ${ev.value} = $hsetTerm.contains(${childGen.value});
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
