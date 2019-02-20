package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF16;

public class JavaUDF16 implements UDF16<String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String> {
    public String call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10, String s11, String s12, String s13, String s14, String s15, String s16) throws Exception {
        return s1 + "+" + s2 + "+" + s3 + "+" + s4 + "+" + s5 + "+" + s6 + "+" + s7 + "+" +
                s8 +  "+" + s9 + "+" + s10 + "+" + s11 + "+" + s12 + "+" + s13 + "+" + s14 + "+" + s15 + "+" + s16;
    }
}
