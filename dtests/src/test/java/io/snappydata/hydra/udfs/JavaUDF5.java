package io.snappydata.hydra.udfs;

import org.apache.spark.sql.api.java.UDF5;

public class JavaUDF5 implements UDF5<Boolean,Boolean,Boolean,Boolean,Boolean,String> {
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
