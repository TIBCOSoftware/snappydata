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

import org.apache.spark.sql.api.java.UDF13;
import java.sql.Timestamp;

public  class JavaUDF13 implements UDF13<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Timestamp> {

    // Purpose the below function is to test the TimeStamp type as an output type.
    // It does nothing, only returns current TimeStamp.

    public Timestamp call(Integer sh1,Integer sh2,Integer sh3,Integer sh4,Integer sh5,Integer sh6,Integer sh7,Integer sh8,Integer sh9,Integer sh10,Integer sh11,Integer sh12,Integer sh13) {
       Integer i = sh1 + sh2 + sh3 + sh4 + sh5 + sh6 + sh7 + sh8 + sh9 + sh10 + sh11 + sh12 + sh13;
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return  ts;
    }
}
