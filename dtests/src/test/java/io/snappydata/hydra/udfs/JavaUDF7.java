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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF7;

public class JavaUDF7 implements UDF7<Integer,Integer,Integer,Integer,Integer,Integer,Row,String> {

    //  User provides the 6 Integer numbers and one Struct type Data type. Purpose is to test the Struct type data type.
    //  Below function returns sum of 6 integer numbers and individual element of Struct data type.

    public String call(Integer la1, Integer la2, Integer la3, Integer la4, Integer la5, Integer la6, Row row) throws Exception {

       Integer lResult = la1 + la2 + la3 + la4 + la5 +la6;
       Integer  i1 = row.getInt(0);
       Double  d2 = row.getDouble(1);
       String s3 = row.getString(2);
       Boolean b4 =   row.isNullAt(3);

        return  lResult.toString() + "(" + i1.toString() + "," + d2.toString() + "," +  s3 + ", IsNull :" + b4.toString() + ")";
    }
}
