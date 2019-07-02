package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF22;

public class JavaUDF22 implements UDF22<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> {

    // Add all the 22 Integer numbers and return the sum.

    public Integer call(Integer i1,Integer i2,Integer i3,Integer i4,Integer i5,Integer i6,Integer i7,Integer i8,Integer i9,Integer i10,Integer i11,Integer i12,Integer i13,
                        Integer i14,Integer i15,Integer i16,Integer i17,Integer i18,Integer i19,Integer i20,Integer i21,Integer i22)
    {
        return (i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 + i11 + i12 + i13 + i14 + i15 + i16 + i17 + i18 + i19 + i20 + i21 + i22);
    }
}