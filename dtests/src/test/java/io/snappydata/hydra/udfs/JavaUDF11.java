package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF11;
import java.util.ArrayList;

public class JavaUDF11 implements UDF11<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Boolean> {
    public Boolean call(Float f1,Float f2,Float f3,Float f4,Float f5,Float f6,Float f7,Float f8,Float f9,Float f10,Float f11) {
        Boolean isContained = false;
        ArrayList<Float> al = new ArrayList<Float>();
        al.add(f1);
        al.add(f2);
        al.add(f3);
        al.add(f4);
        al.add(f5);
        al.add(f6);
        al.add(f7);
        al.add(f8);
        al.add(f9);
        al.add(f10);

        if(al.contains(f11))
            isContained = true;

        return  isContained;
    }
}
