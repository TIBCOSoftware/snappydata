package io.snappydata.hydra.deployUnDeploy.udfFiles;

import org.apache.spark.sql.api.java.UDF1;

public class MyUDF5 implements UDF1<String, String> {
    public String call(String s) throws Exception {
        return s.toUpperCase();
    }
}
