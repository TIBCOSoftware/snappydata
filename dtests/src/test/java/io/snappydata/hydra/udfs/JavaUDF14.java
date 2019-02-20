package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF14;
import java.util.ArrayList;
import java.util.Collections;

public class JavaUDF14 implements UDF14<String,String,String,String,String,String,String,String,String,String,String,String,String,String,Long> {
    public Long call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10, String s11, String s12, String s13, String s14) throws Exception {
        ArrayList<Long> al = new ArrayList<Long>();
        al.add(Long.decode(s1));
        al.add(Long.decode(s2));
        al.add(Long.decode(s3));
        al.add(Long.decode(s4));
        al.add(Long.decode(s5));
        al.add(Long.decode(s6));
        al.add(Long.decode(s7));
        al.add(Long.decode(s8));
        al.add(Long.decode(s9));
        al.add(Long.decode(s10));
        al.add(Long.decode(s11));
        al.add(Long.decode(s12));
        al.add(Long.decode(s13));
        al.add(Long.decode(s14));

        Long minValue = Collections.min(al);
        return minValue;
    }
}
