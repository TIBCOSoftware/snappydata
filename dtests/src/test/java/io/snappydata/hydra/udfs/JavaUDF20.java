package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF20;

public  class JavaUDF20 implements UDF20<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> {
    public Integer call(Integer i1,Integer i2,Integer i3,Integer i4,Integer i5,Integer i6,Integer i7,Integer i8,Integer i9,Integer i10,Integer i11,Integer i12,Integer i13,Integer i14,Integer i15,Integer i16,Integer i17,Integer i18,Integer i19,Integer i20) {
        int count = 0;
        Integer[] numbers = new Integer[20];
        numbers[0] = i1;
        numbers[1] = i2;
        numbers[2] = i3;
        numbers[3] = i4;
        numbers[4] = i5;
        numbers[5] = i6;
        numbers[6] = i7;
        numbers[7] = i8;
        numbers[8] = i9;
        numbers[9] = i10;
        numbers[10] = i11;
        numbers[11] = i12;
        numbers[12] = i13;
        numbers[13] = i14;
        numbers[14] = i15;
        numbers[15] = i16;
        numbers[16] = i17;
        numbers[17] = i18;
        numbers[18] = i19;
        numbers[19] = i20;
        for(int i = 0; i < 20; i++) {
            if(numbers[i] % 2 == 0) {
                count++;
            }
        }
        return count;
    }
}