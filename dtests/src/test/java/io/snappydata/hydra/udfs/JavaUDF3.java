package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF3;
import scala.collection.mutable.WrappedArray;

public class JavaUDF3 implements UDF3<WrappedArray<String>, WrappedArray<String>, WrappedArray<String>, String> {
    public String call(WrappedArray<String> wstr1, WrappedArray<String> wstr2, WrappedArray<String> wstr3) throws Exception {
        String[] arr1 = new String[wstr1.length()];
        String[] arr2 = new String[wstr2.length()];
        String[] arr3 = new String[wstr3.length()];
        String outputStr = "";

        wstr1.copyToArray(arr1);
        wstr2.copyToArray(arr2);
        wstr3.copyToArray(arr3);

        for(int i=0; i < arr1.length ;i++) {
            outputStr = outputStr + arr1[i] + " ";
            outputStr = outputStr.toUpperCase();
        }

        for(int i=0; i < arr2.length ;i++) {
            outputStr = outputStr + arr2[i] + " ";
            outputStr = outputStr.toUpperCase();
        }

        for(int i=0; i < arr3.length ;i++) {
            outputStr = outputStr + arr3[i] + " ";
            outputStr = outputStr.toUpperCase();
        }

        return  outputStr;
    }
}


