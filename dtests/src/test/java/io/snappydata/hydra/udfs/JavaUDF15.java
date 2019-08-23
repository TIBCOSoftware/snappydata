/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF15;
import java.util.ArrayList;
import java.util.Collections;

public class JavaUDF15 implements UDF15<Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> {

    //  Purpose of is to test the Double Type as an Input and Output.
    //  Below function add the 15 double numbers to ArrayList and returns the Max Double value from ArrayList.

    public Double call(Double d1, Double d2, Double d3, Double d4, Double d5, Double d6, Double d7, Double d8, Double d9, Double d10, Double d11, Double d12, Double d13, Double d14, Double d15) throws Exception {
        ArrayList<Double> al = new ArrayList<Double>();
        al.add(d1);
        al.add(d2);
        al.add(d3);
        al.add(d4);
        al.add(d5);
        al.add(d6);
        al.add(d7);
        al.add(d8);
        al.add(d9);
        al.add(d10);
        al.add(d11);
        al.add(d12);
        al.add(d13);
        al.add(d14);
        al.add(d15);

        Double d = Collections.max(al);

        return d;
    }
}
