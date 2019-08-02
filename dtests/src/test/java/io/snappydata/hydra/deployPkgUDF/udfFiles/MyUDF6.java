package io.snappydata.hydra.deployPkgUDF.udfFiles;

import org.apache.spark.sql.api.java.UDF4;

public class MyUDF6 implements UDF4<Double, Double, Double,Double, Integer> {
    public Integer call(Double d1, Double d2, Double d3, Double d4) throws Exception {
        Double d  = d1 + d2 + d3 + d4;
        Double d5 = Math.ceil(d);
        return d5.intValue();
    }
}
