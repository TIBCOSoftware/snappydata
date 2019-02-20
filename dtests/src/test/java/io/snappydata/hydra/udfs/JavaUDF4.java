package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF4;
import scala.collection.JavaConverters;
import scala.collection.Map;

public class JavaUDF4 implements UDF4<Map<String,Double>,Map<String,Double>,Map<String,Double>,Map<String,Double>, Double> {

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
