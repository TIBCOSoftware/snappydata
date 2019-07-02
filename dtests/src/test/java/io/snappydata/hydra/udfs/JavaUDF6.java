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

import org.apache.spark.sql.api.java.UDF6;
import java.math.BigDecimal;

public class JavaUDF6 implements UDF6<BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal> {

    //  User provides 6 BigDecimal number,
    //  Below function use BigDecimal class methods and add all the BigDecimal numbers.
    //  Purpose is to test the BigDecimal as input and BigDecimal as output.

    public BigDecimal call(BigDecimal bd1, BigDecimal bd2, BigDecimal bd3, BigDecimal bd4, BigDecimal bd5, BigDecimal bd6) throws Exception {
        BigDecimal bigDecimal ;
        bigDecimal = bd1.setScale(6,0);
        bigDecimal = bd2.setScale(6,0).add(bigDecimal);
        bigDecimal = bd3.setScale(6,0).add(bigDecimal);
        bigDecimal = bd4.setScale(6,0).add(bigDecimal);
        bigDecimal = bd5.setScale(6,0).add(bigDecimal);
        bigDecimal = bd6.setScale(6,0).add(bigDecimal);
        return bigDecimal;
    }
}
