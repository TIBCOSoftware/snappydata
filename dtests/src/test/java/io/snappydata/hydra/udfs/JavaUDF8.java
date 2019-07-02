package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF8;
import java.util.ArrayList;

public class JavaUDF8 implements UDF8<Long,Long,Long,Long,Long,Long,Long,Long,Short> {

    //  User provides the 8 Long numbers. Purpose is to test the Long as input, Short as output.
    //  Below function add all the Long numbers to ArrayList and returns the size of ArrayList as Short.

    public Short call(Long l1, Long l2, Long l3, Long l4, Long l5, Long l6, Long l7, Long l8) throws Exception {
        ArrayList<Long> al = new ArrayList<Long>();
        al.add(l1);
        al.add(l2);
        al.add(l3);
        al.add(l4);
        al.add(l5);
        al.add(l6);
        al.add(l7);
        al.add(l8);
        Integer iSize = al.size();
        short size = iSize.shortValue();
        return size;
    }
}
