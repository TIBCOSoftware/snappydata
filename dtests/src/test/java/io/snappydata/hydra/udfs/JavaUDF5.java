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

import org.apache.spark.sql.api.java.UDF5;

public class JavaUDF5 implements UDF5<Boolean,Boolean,Boolean,Boolean,Boolean,String> {

    //  User provides five Boolean values,
    //  if value is true convert it as String value 1 and if value is false convert it as String value 0
    //  Below function concat all the String values and parse the String as Binary value,
    //  convert the Binary number to Hex String.

    public String call(Boolean b1, Boolean b2, Boolean b3, Boolean b4, Boolean b5) throws Exception {
        String myStr = "";
        if(b1 == true)
            myStr = "1";
        else
            myStr = "0";

        if(b2 == true)
            myStr = myStr + "1";
        else
            myStr = myStr + "0";

        if(b3 == true)
            myStr = myStr + "1";
        else
            myStr = myStr + "0";

        if(b4 == true)
            myStr = myStr + "1";
        else
            myStr = myStr + "0";

        if(b5 == true)
            myStr = myStr + "1";
        else
            myStr = myStr + "0";

        Integer i1 = Integer.parseInt(myStr,2);
        String result = Integer.toHexString(i1);
        return  "0x:" + result;
    }
}
