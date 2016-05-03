package io.snappydata.hydra.cluster;

import java.util.Vector;

import hydra.BasePrms;


/**
 * Created by swati on 11/3/16.
 */
public class SnappyPrms extends BasePrms {
    /**
     * Parameter used to get the user specified script names for INITTASK.
     * (VectosetValuesr of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNamesForInitTask;

    /**
     * Parameter used to get the user specified param List for INITTASK.
     * (VectosetValuesr of Strings) A list of values for parameters to be replaced in the sql scripts.
     */
    public static Long sqlScriptParamsForInitTask;

    /**
     * Parameter used to get the user specified script names for TASK.
     * (VectosetValuesr of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNamesForTask;

    /**
     * Parameter used to get the user specified snappy job class names for CLOSETASK.
     * (VectosetValuesr of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long jobClassNamesForCloseTask;

    /**
     * Parameter used to get the user specified snappy job class names for TASK.
     * (VectosetValuesr of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long jobClassNamesForTask;

    /**
     * Parameter used to get the user specified snappy streaming job class names for TASK.
     * (VectosetValuesr of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long streamingJobClassNamesForTask;

    /**
     * (boolean) for testing HA
     */
    public static Long cycleVms;

    /**
     * (String) cycleVMTarget - which node to be cycled "store, lead" etc
     */
    public static Long cycleVMTarget;

    /**
     * (String) e.g. simulateFileStream
     */
    public static Long simulateStreamScriptName;

    /**
     * (String) - destination folder to copy the streaming data. e.g. /home/swati
     */
    public static Long simulateStreamScriptDestinationFolder;

    /**
     * (String) userAppJar containing the user snappy job class
     */
    public static Long userAppJar;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Task
     */
    public static Long jobExecutionTimeInMillisForTask;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Task
     */
    public static Long streamingJobExecutionTimeInMillisForTask;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Close Task
     */
    public static Long jobExecutionTimeInMillisForCloseTask;

    /**
     * (int) how long (milliseconds) it should wait before Cycle VMs again
     */
    public static Long waitTimeBeforeNextCycleVM;

    /**
     * App Props that are passed to the job
     */
    public static Long appPropsForJobServer;


    public static Vector getSQLScriptNamesForInitTask() {
        Long key = sqlScriptNamesForInitTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSQLScriptParamsForInitTask() {
        Long key = sqlScriptParamsForInitTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSQLScriptNamesForTask() {
        Long key = sqlScriptNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyJobClassNamesForTask() {
        Long key = jobClassNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyStreamingJobClassNamesForTask() {
        Long key = streamingJobClassNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyJobClassNamesForCloseTask() {
        Long key = jobClassNamesForCloseTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static String getCommaSepAPPProps() {
        Long key = appPropsForJobServer;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }


    static {
        BasePrms.setValues(SnappyPrms.class);
    }

    public static void main(String args[]) {
        BasePrms.dumpKeys();
    }
}
