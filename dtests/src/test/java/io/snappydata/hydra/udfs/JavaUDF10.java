package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF10;

public class JavaUDF10 implements UDF10<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float> {

    // Purpose is to test the Float type as an input and an output.
    // User provides the 10 Float numbers, add all the numbers and return the result as Float.

    public Float call(Float d1,Float d2,Float d3,Float d4,Float d5,Float d6,Float d7,Float d8,Float d9,Float d10) {
            return  d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 + d10;
    }
 }