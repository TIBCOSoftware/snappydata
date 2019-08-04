package io.snappydata.hydra.deployPkgUDF.udfFiles;

import org.apache.spark.sql.api.java.UDF1;

public class MyUDF4 implements UDF1<Integer, Float> {
    public Float call(Integer integer) throws Exception {
        return integer/100.0f;
    }
}
