package io.snappydata.hydra;

import org.apache.spark.sql.api.java.UDF1;


public class MySubject implements UDF1<Long,String> {
    public String call(Long l1) throws Exception {
        String subject[] = new String[10];
        subject[0] = "Maths-1";
        subject[1] = "Science";
        subject[2] = "Parallel Processing";
        subject[3] = "Graphics";
        subject[4] = "Mechanics";
        subject[5] = "DataStructures";
        subject[6] = "Algorithms";
        subject[7] = "Control System";
        subject[8] = "Maths-2";
        subject[9] = "Electronics";
        return  (subject[l1.intValue()]);
    }
}


//https://living-sun.com/es/hadoop/280297-hive-on-spark-missing-ltspark-assemblyjargt-hadoop-apache-spark-hive.html