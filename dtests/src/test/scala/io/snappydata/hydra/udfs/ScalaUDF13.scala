package com.snappy.scala.poc.udf

import org.apache.spark.sql.api.java.UDF13
import java.math.{BigDecimal, BigInteger}

class ScalaUDF13 extends UDF13[BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal] {
  override def call(t1: BigDecimal, t2: BigDecimal, t3: BigDecimal, t4: BigDecimal, t5: BigDecimal, t6: BigDecimal, t7: BigDecimal, t8: BigDecimal, t9: BigDecimal, t10: BigDecimal, t11: BigDecimal, t12: BigDecimal, t13: BigDecimal): BigDecimal = {
          val bigInteger1 : BigInteger = t1.unscaledValue()
          val bigDecimal1 : BigDecimal = new BigDecimal(bigInteger1)

          val bigInteger2 : BigInteger = t2.unscaledValue();
          val bigDecimal2  : BigDecimal = new BigDecimal(bigInteger2)

          val bigInteger3 : BigInteger = t3.unscaledValue();
          val bigDecimal3  : BigDecimal = new BigDecimal(bigInteger3)

          val bigInteger4 : BigInteger = t4.unscaledValue();
          val bigDecimal4  : BigDecimal = new BigDecimal(bigInteger4)

          val bigInteger5 : BigInteger = t5.unscaledValue();
          val bigDecimal5  : BigDecimal = new BigDecimal(bigInteger5)

          val bigInteger6 : BigInteger = t6.unscaledValue();
          val bigDecimal6  : BigDecimal = new BigDecimal(bigInteger6)

          val bigInteger7 : BigInteger = t7.unscaledValue();
          val bigDecimal7  : BigDecimal = new BigDecimal(bigInteger7)

          val bigInteger8 : BigInteger = t8.unscaledValue();
          val bigDecimal8  : BigDecimal = new BigDecimal(bigInteger8)

          val bigInteger9 : BigInteger = t9.unscaledValue();
          val bigDecimal9  : BigDecimal = new BigDecimal(bigInteger9)

          val bigInteger10 : BigInteger = t10.unscaledValue();
          val bigDecimal10  : BigDecimal = new BigDecimal(bigInteger10)

          val bigInteger11 : BigInteger = t11.unscaledValue();
          val bigDecimal11  : BigDecimal = new BigDecimal(bigInteger11)

          val bigInteger12 : BigInteger = t12.unscaledValue();
          val bigDecimal12  : BigDecimal = new BigDecimal(bigInteger12)

          val bigInteger13 : BigInteger = t13.unscaledValue();
          val bigDecimal13  : BigDecimal = new BigDecimal(bigInteger13)


          return bigDecimal1.add(bigDecimal2.add(bigDecimal3.add(bigDecimal4.add(bigDecimal5.add(bigDecimal6.add(bigDecimal7.add(bigDecimal8.add(bigDecimal9.add(bigDecimal10.add(bigDecimal11.add(bigDecimal12.add(bigDecimal13))))))))))))
  }
}
