package io.snappydata.hydra;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.mutable.WrappedArray;

import java.util.Random;

public class ThreadID implements UDF1<WrappedArray<String>, Integer> {
    public Integer call(WrappedArray arr) throws Exception {

        String[] arr1 = new String[arr.length()];
        Random rand = new Random();
        arr.copyToArray(arr1);
        int index = rand.nextInt(arr1.length);
        return Integer.parseInt(arr1[index]);
    }
}
