/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql.api.java.UDF21;

public class JavaUDF21 implements UDF21<String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,Integer> {

    //  User provides the 21 Strings, calculate the length of all Strings and return it as an Integer.

    public Integer call(String s1,String s2,String s3,String s4,String s5,String s6,String s7,String s8,String s9,String s10,String s11,String s12,String s13,String s14,String s15,String s16,String s17,String s18,String s19,String s20,String s21) {
        int length = 0;
        length = s1.length() + s2.length() + s3.length() + s4.length() + s5.length() + s6.length()
                  +  s7.length() + s8.length() + s9.length() + s10.length() + s11.length() + s12.length() +
                      s13.length() + s14.length() + s15.length() + s16.length() + s17.length() + s18.length() +
                      s19.length() + s20.length() + s21.length();
        return length;
    }
}