package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF2;

public class JavaUDF2 implements UDF2<String,String,String> {
    public String call(String s1, String s2) {
        return s1 + "#" + s2;
    }
}
