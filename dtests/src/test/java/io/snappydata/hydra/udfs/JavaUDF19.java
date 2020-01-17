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

import org.apache.spark.sql.api.java.UDF19;

public  class JavaUDF19 implements UDF19<String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String> {

    //  Purpose is to test the String as input and output.
    //  Add all the Strings to Array and concat all the strings and convert it to upper case and return the String.


    public String call(String s1,String s2,String s3,String s4,String s5,String s6,String s7,String s8, String s9,String s10,String s11,String s12,String s13,String s14,String s15,String s16,String s17,String s18,String s19) {
        String str = "";
        String[] strArr = new String[19];
        strArr[0] = s1;
        strArr[1] = s2;
        strArr[2] = s3;
        strArr[3] = s4;
        strArr[4] = s5;
        strArr[5] = s6;
        strArr[6] = s7;
        strArr[7] = s8;
        strArr[8] = s9;
        strArr[9] = s10;
        strArr[10] = s11;
        strArr[11] = s12;
        strArr[12] = s13;
        strArr[13] = s14;
        strArr[14] = s15;
        strArr[15] = s16;
        strArr[16] = s17;
        strArr[17] = s18;
        strArr[18] = s19;

        for(int i=0;i < 19;i++) {
            str = str + strArr[i].toUpperCase().concat(",");
        }
        return str;
    }
}