package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class JavaUDF1 implements UDF1<Integer,Integer> {
    // Add 100 to  given number i.e i1 + 100.
    public Integer call(Integer i1) throws Exception {
        return  (i1 + 100);
    }
}
