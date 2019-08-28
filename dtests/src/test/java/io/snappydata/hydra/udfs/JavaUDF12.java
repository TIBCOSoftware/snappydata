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

import org.apache.spark.sql.api.java.UDF12;
import java.sql.Date;
import java.util.ArrayList;

public class JavaUDF12 implements UDF12<String,String,String,String,String,String,String,String,String,String,String,String, Date> {

    // Purpose the below function is to test the Date type as an output type.
    // It does nothing, only returns current Date.

    public Date call(String s1,String s2,String s3,String s4,String s5,String s6,String s7,String s8,String s9,String s10,String s11,String s12) {
        ArrayList<String> al = new ArrayList<String>();
        al.add(s1);
        al.add(s2);
        al.add(s3);
        al.add(s4);
        al.add(s5);
        al.add(s6);
        al.add(s7);
        al.add(s8);
        al.add(s9);
        al.add(s10);
        al.add(s11);
        al.add(s12);

        Object[] strArr = al.toArray();
         return new Date(System.currentTimeMillis());
    }
}