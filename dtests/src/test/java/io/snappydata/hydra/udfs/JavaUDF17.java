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

import org.apache.spark.sql.api.java.UDF17;
import java.util.Arrays;

public  class JavaUDF17 implements UDF17<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Boolean> {

    //  Purpose is to the test the Boolean type as an output type.
    //  Below function do nothing, just return true value.

    public  Boolean call(Float f1,Float f2,Float f3,Float f4,Float f5,Float f6,Float f7,Float f8, Float f9,Float f10,Float f11,Float f12,Float f13,Float f14,Float f15,Float f16,Float f17) {

        Float[] fltArr = new Float[17];
        fltArr[0] = f1;
        fltArr[1] = f2;
        fltArr[2] = f3;
        fltArr[3] = f4;
        fltArr[4] = f5;
        fltArr[5] = f6;
        fltArr[6] = f7;
        fltArr[7] = f8;
        fltArr[8] = f9;
        fltArr[9] = f10;
        fltArr[10] = f11;
        fltArr[11] = f12;
        fltArr[12] = f13;
        fltArr[13] = f14;
        fltArr[14] = f15;
        fltArr[15] = f16;
        fltArr[16] = f17;
        Arrays.sort(fltArr);
        return true;
    }
}