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

import org.apache.spark.sql.api.java.UDF11;
import java.util.ArrayList;

public class JavaUDF11 implements UDF11<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Boolean> {

    // Below function add 10 float numbers to ArrayList and last element not added to the ArrayList.
    // Check that 11th Element is in ArrayList and returns the Boolean value.
    //  Purpose is to test the Boolean type as an output type.

    public Boolean call(Float f1,Float f2,Float f3,Float f4,Float f5,Float f6,Float f7,Float f8,Float f9,Float f10,Float f11) {
        Boolean isContained = false;
        ArrayList<Float> al = new ArrayList<Float>();
        al.add(f1);
        al.add(f2);
        al.add(f3);
        al.add(f4);
        al.add(f5);
        al.add(f6);
        al.add(f7);
        al.add(f8);
        al.add(f9);
        al.add(f10);

        if(al.contains(f11))
            isContained = true;

        return  isContained;
    }
}
