package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF2;

public class JavaUDF2 implements UDF2<String,String,String> {
    //  Concat two given String s1, s2 and add symbole # between two given string.
    //      i.e s1 + # + s2.
    public String call(String s1, String s2) {
        return s1 + "#" + s2;
    }
}
