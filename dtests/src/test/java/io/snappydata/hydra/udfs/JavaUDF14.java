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

import org.apache.spark.sql.api.java.UDF14;
import java.util.ArrayList;
import java.util.Collections;

public class JavaUDF14 implements UDF14<String,String,String,String,String,String,String,String,String,String,String,String,String,String,Long> {

    //  Purpose of this function is to test Long type.
    //  Convert the String to Long, add it to the ArrayList and returns the min value from ArrayList.

    public Long call(String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10, String s11, String s12, String s13, String s14) throws Exception {
        ArrayList<Long> al = new ArrayList<Long>();
        al.add(Long.decode(s1));
        al.add(Long.decode(s2));
        al.add(Long.decode(s3));
        al.add(Long.decode(s4));
        al.add(Long.decode(s5));
        al.add(Long.decode(s6));
        al.add(Long.decode(s7));
        al.add(Long.decode(s8));
        al.add(Long.decode(s9));
        al.add(Long.decode(s10));
        al.add(Long.decode(s11));
        al.add(Long.decode(s12));
        al.add(Long.decode(s13));
        al.add(Long.decode(s14));

        Long minValue = Collections.min(al);
        return minValue;
    }
}
