package io.snappydata.hydra.deployPkgUDF.udfFiles;

import org.apache.spark.sql.api.java.UDF2;

public class MyUDF7 implements UDF2<Integer,Integer,Integer> {
    public Integer call(Integer i1, Integer i2) throws Exception {
        return Math.max(i1,i2);
    }
}
