package io.snappydata.hydra.cluster;

import java.util.Vector;
import hydra.BasePrms;


/**
 * Created by swati on 11/3/16.
 */
public class SnappyPrms extends BasePrms {
    /**
     * Parameter used to get the user specified script names.
     * (VectosetValuesr of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNamesForInitTask;
    public static Long sqlScriptNamesForTask;

    public static Vector getSQLScriptNamesForInitTask() {
        Long key = sqlScriptNamesForInitTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSQLScriptNamesForTask() {
        Long key = sqlScriptNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    static {
        BasePrms.setValues(SnappyPrms.class);
    }

    public static void main(String args[]) {
        BasePrms.dumpKeys();
    }
}
