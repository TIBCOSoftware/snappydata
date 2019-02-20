package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class JavaUDF1 implements UDF1<Integer,Integer> {
    public Integer call(Integer i1) throws Exception {
        return  (i1 + 100);
    }
}
