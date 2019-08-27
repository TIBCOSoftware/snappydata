package io.snappydata.hydra.deployPkgUDF.udfFiles;

import org.apache.spark.sql.api.java.UDF2;

public class MyUDF3 implements UDF2<Integer,Integer,Integer> {
    public Integer call(Integer integer1, Integer integer2) throws Exception {
        return integer1 + integer2;
    }
}
