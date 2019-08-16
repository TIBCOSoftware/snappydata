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

import org.apache.spark.sql.api.java.UDF16;

public class JavaUDF16 implements UDF16<String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String> {

    //  Below function concat all the Strings (16 Strings) and returns it.

    public String call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10, String s11, String s12, String s13, String s14, String s15, String s16) throws Exception {
        return s1 + "+" + s2 + "+" + s3 + "+" + s4 + "+" + s5 + "+" + s6 + "+" + s7 + "+" +
                s8 +  "+" + s9 + "+" + s10 + "+" + s11 + "+" + s12 + "+" + s13 + "+" + s14 + "+" + s15 + "+" + s16;
    }
}
