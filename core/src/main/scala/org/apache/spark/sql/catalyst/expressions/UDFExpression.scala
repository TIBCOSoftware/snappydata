package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, DataTypes, DateType, DecimalType, TimestampType}

/**
 * User-defined function.
 * Note that the user-defined functions must be deterministic.
 * @param function  The user defined scala function to run.
 *                  Note that if you use primitive parameters, you are not able to check if it is
 *                  null or not, and the UDF will return null for you if the primitive input is
 *                  null. Use boxed type or [[Option]] if you wanna do the null-handling yourself.
 * @param dataType  Return type of function.
 * @param children  The input expressions of this UDF.
 * @param inputTypes  The expected input types of this UDF, used to perform type coercion. If we do
 *                    not want to perform coercion, simply use "Nil". Note that it would've been
 *                    better to use Option of Seq[DataType] so we can use "None" as the case for no
 *                    type coercion. However, that would require more refactoring of the codebase.
 */
case class UDFExpression(
    udfClazz: Class[_],
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Seq[DataType] = Nil)
    extends Expression with ImplicitCastInputTypes with NonSQLExpression {

  override def nullable: Boolean = true

  override def toString: String = s"UDFExpression(${children.mkString(", ")})"

  def userDefinedFunc(): AnyRef = function

  def getChildren(): Seq[Expression] = children

  // scalastyle:on line.size.limit

  // Generate codes used to convert the arguments to Scala type for user-defined functions
  private[this] def genCodeForConverter(ctx: CodegenContext, dataType: DataType): String = {
    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"
    val converterTerm = ctx.freshName("converter")
    val dataTypeTerm = getDataTypeString(dataType)
    ctx.addMutableState(converterClassName, converterTerm,
      s"this.$converterTerm = ($converterClassName)$typeConvertersClassName" +
          s".createToScalaConverter($dataTypeTerm);")
    converterTerm
  }

  private[this] def getDataTypeString(t: DataType): String = {
    if (t eq DataTypes.BooleanType) {
      s"org.apache.spark.sql.types.DataTypes.BooleanType"
    }
    else if (t eq DataTypes.ByteType) {
      s"org.apache.spark.sql.types.DataTypes.ByteType"
    }
    else if (t eq DataTypes.ShortType) {
      s"org.apache.spark.sql.types.DataTypes.ShortType"
    }
    else if (t eq DataTypes.IntegerType) {
      s"org.apache.spark.sql.types.DataTypes.IntegerType"
    }
    else if (t eq DataTypes.LongType) {
      s"org.apache.spark.sql.types.DataTypes.LongType"
    }
    else if (t eq DataTypes.FloatType) {
      s"org.apache.spark.sql.types.DataTypes.FloatType"
    }
    else if (t eq DataTypes.DoubleType) {
      s"org.apache.spark.sql.types.DataTypes.DoubleType"
    }
    else if (t eq DataTypes.StringType) {
      s"org.apache.spark.sql.types.DataTypes.StringType"
    }
    else if (t.isInstanceOf[DecimalType]) {
      s"org.apache.spark.sql.types.DataTypes.DecimalType"
    }
    else if (t.isInstanceOf[CalendarIntervalType]) {
      s"org.apache.spark.sql.types.DataTypes.CalendarIntervalType"
    }
    else if (t.isInstanceOf[DateType]) {
      s"org.apache.spark.sql.types.DataTypes.DateType"
    }
    else if (t.isInstanceOf[TimestampType]) {
      s"org.apache.spark.sql.types.DataTypes.TimestampType"
    } else {
      throw new SparkException(s"$t is not a valid DataType")
    }
  }


  override def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {

    // Initialize user-defined function
    val funcClassName = s"${udfClazz.getCanonicalName}"

    val inputTypes = children.map(_.dataType.simpleString).mkString(", ")
    val errorMessage
    = s"Failed to execute user defined function($funcClassName: ($inputTypes) => ${dataType.simpleString})"

    val funcTerm = ctx.freshName("udf")
    val castTerm = s"""io.snappydata.udf.UDF${children.size}"""
    ctx.addMutableState(castTerm, funcTerm,
      s"""
         try{
           System.out.println("Loading class " + Thread.currentThread().getContextClassLoader());
           this.$funcTerm = ($castTerm)Class.forName("$funcClassName", false,
              Thread.currentThread().getContextClassLoader()).newInstance();
         } catch (Exception e) {
           throw new org.apache.spark.SparkException("Could not instantiate the UDF $funcClassName", e);
         }""".stripMargin)



    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"

    // Generate codes used to convert the returned value of user-defined functions to Catalyst type
    val catalystConverterTerm = ctx.freshName("catalystConverter")
    ctx.addMutableState(converterClassName, catalystConverterTerm,
      s"this.$catalystConverterTerm = ($converterClassName)$typeConvertersClassName" +
          s".createToCatalystConverter($funcTerm.getDataType());")

    val resultTerm = ctx.freshName("result")

    // This must be called before children expressions' codegen
    // because ctx.references is used in genCodeForConverter
    val converterTerms = children.map(c => genCodeForConverter(ctx, c.dataType))

    // codegen for children expressions
    val evals = children.map(_.genCode(ctx))

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString
    val (converters, funcArguments) = converterTerms.zipWithIndex.map { case (converter, i) =>
      val eval = evals(i)
      val argTerm = ctx.freshName("arg")
      val convert = s"Object $argTerm = ${eval.isNull} ? null : $converter.apply(${eval.value});"
      (convert, argTerm)
    }.unzip

    val getFuncResult = s"$funcTerm.call(${funcArguments.mkString(", ")})"
    val callFunc =
      s"""
         ${ctx.boxedType(dataType)} $resultTerm = null;
         try {
           $resultTerm = (${ctx.boxedType(dataType)})$catalystConverterTerm.apply($getFuncResult);
         } catch (Exception e) {
           throw new org.apache.spark.SparkException("$errorMessage", e);
         }
       """

    ev.copy(code =
        s"""
      $evalCode
      ${converters.mkString("\n")}
      $callFunc

      boolean ${ev.isNull} = $resultTerm == null;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = $resultTerm;
      }""")
  }

  private[this] val converter = CatalystTypeConverters.createToCatalystConverter(dataType)

  lazy val udfErrorMessage = {
    val funcCls = function.getClass.getSimpleName
    val inputTypes = children.map(_.dataType.simpleString).mkString(", ")
    s"Failed to execute user defined function($funcCls: ($inputTypes) => ${dataType.simpleString})"
  }

  override def eval(input: InternalRow): Any = {
    val result = try {
      //Do nothing
    } catch {
      case e: Exception =>
        throw new SparkException(udfErrorMessage, e)
    }

    converter(result)
  }
}
