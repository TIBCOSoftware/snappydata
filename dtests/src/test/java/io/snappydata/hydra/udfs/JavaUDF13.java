package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF13;
import java.sql.Timestamp;

public  class JavaUDF13 implements UDF13<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Timestamp> {
    public Timestamp call(Integer sh1,Integer sh2,Integer sh3,Integer sh4,Integer sh5,Integer sh6,Integer sh7,Integer sh8,Integer sh9,Integer sh10,Integer sh11,Integer sh12,Integer sh13) {
       Integer i = sh1 + sh2 + sh3 + sh4 + sh5 + sh6 + sh7 + sh8 + sh9 + sh10 + sh11 + sh12 + sh13;
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return  ts;
    }
}
