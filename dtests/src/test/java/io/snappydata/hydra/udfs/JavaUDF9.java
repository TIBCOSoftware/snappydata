package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF9;

public class JavaUDF9 implements UDF9<String,String,String,String,String,String,String,String,String,String> {
    // Input is of type Char but since competible type of Char is String have taken the String as input type and output type
    public String call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9) throws Exception {
        String str = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9;
        return str;
    }
}
