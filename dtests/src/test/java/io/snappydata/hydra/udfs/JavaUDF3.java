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

import org.apache.spark.sql.api.java.UDF3;
import scala.collection.mutable.WrappedArray;

public class JavaUDF3 implements UDF3<WrappedArray<String>, WrappedArray<String>, WrappedArray<String>, String> {

   //  User provides 3 String Array, JavaUDF3 concat given 3 String Array and convert it into the upper case.

    public String call(WrappedArray<String> wstr1, WrappedArray<String> wstr2, WrappedArray<String> wstr3) throws Exception {
        String[] arr1 = new String[wstr1.length()];
        String[] arr2 = new String[wstr2.length()];
        String[] arr3 = new String[wstr3.length()];
        String outputStr = "";

        wstr1.copyToArray(arr1);
        wstr2.copyToArray(arr2);
        wstr3.copyToArray(arr3);

        for(int i=0; i < arr1.length ;i++) {
            outputStr = outputStr + arr1[i] + " ";
        }

        for(int i=0; i < arr2.length ;i++) {
            outputStr = outputStr + arr2[i] + " ";
        }

        for(int i=0; i < arr3.length ;i++) {
            outputStr = outputStr + arr3[i] + " ";
         }

        outputStr = outputStr.toUpperCase();

        return  outputStr;
    }
}


