package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF6;
import java.math.BigDecimal;

public class JavaUDF6 implements UDF6<BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal> {
    public BigDecimal call(BigDecimal bd1, BigDecimal bd2, BigDecimal bd3, BigDecimal bd4, BigDecimal bd5, BigDecimal bd6) throws Exception {
        BigDecimal bigDecimal ;
        bigDecimal = bd1.setScale(6,0);
        bigDecimal = bd2.setScale(6,0).add(bigDecimal);
        bigDecimal = bd3.setScale(6,0).add(bigDecimal);
        bigDecimal = bd4.setScale(6,0).add(bigDecimal);
        bigDecimal = bd5.setScale(6,0).add(bigDecimal);
        bigDecimal = bd6.setScale(6,0).add(bigDecimal);
        return bigDecimal;
    }
}
