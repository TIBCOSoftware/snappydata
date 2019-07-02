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

import org.apache.spark.sql.api.java.UDF4;
import scala.collection.JavaConverters;
import scala.collection.Map;

public class JavaUDF4 implements UDF4<Map<String,Double>,Map<String,Double>,Map<String,Double>,Map<String,Double>, Double> {

    //  User provides for Map type  variable, Subject as Key and it's mark as value.
    //  Below function returns the Total marks of given four subjects.

   public Double call(Map<String, Double> m1, Map<String, Double> m2, Map<String, Double> m3, Map<String, Double> m4) throws Exception {
       java.util.Map<String, Double> javam1 = JavaConverters.mapAsJavaMapConverter(m1).asJava();
       java.util.Map<String, Double> javam2 = JavaConverters.mapAsJavaMapConverter(m2).asJava();
       java.util.Map<String, Double> javam3 = JavaConverters.mapAsJavaMapConverter(m3).asJava();
       java.util.Map<String, Double> javam4 = JavaConverters.mapAsJavaMapConverter(m4).asJava();
       Double d1 = javam1.get("Maths");
       Double d2 = javam2.get("Science");
       Double d3 = javam3.get("English");
       Double d4 = javam4.get("Music");
       return d1+d2+d3+d4;
    }
}
