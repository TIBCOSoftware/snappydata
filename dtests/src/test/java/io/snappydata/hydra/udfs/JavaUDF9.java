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

import org.apache.spark.sql.api.java.UDF9;

public class JavaUDF9 implements UDF9<String,String,String,String,String,String,String,String,String,String> {

    // Input is of type Char but since competible type of Char is String have taken the String as input type and output type.
    // Purpose is to test the Char data type.
    // User provides 9 Char data, we concate it and return as String.

    public String call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9) throws Exception {
        String str = s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9;
        return str;
    }
}
