/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.cluster;

import hydra.BasePrms;
import hydra.HydraVector;

import java.util.Vector;


/**
 * Created by swati on 11/3/16.
 */
public class SnappyPrms extends BasePrms {
    /**
     * Parameter used to get the user specified script names.
     * (VectosetValues of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNames;

    /**
     * Parameter used to get the user specified param List.
     * (VectosetValues of Strings) A list of values for parameters to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long sqlScriptParams;

    /**
     * Parameter used to get the user specified snappy job class names.
     * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long jobClassNames;

    /**
     * Parameter used to get the user specified spark job class names.
     * (VectosetValues of Strings) A list of values for spark-job Names to execute.
     */
    public static Long sparkJobClassNames;

    /**
     * Parameter used to get the user specified snappy streaming job class names.
     * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long streamingJobClassNames;

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
     * (boolean) - whether snappy servers and locators needs to be started using rowstore option.
     */
    public static Long useRowStore;

    /**
     * (boolean) - whether split mode cluster needs to be started.
     */
    public static Long useSplitMode;

    /**
     * (boolean) - whether stop mode needs to be checked before deleting the config data if already exists.
     * This is required in case user wants to start the cluster and then stop the same later on using different script.
     * In this case, test should not delete the existing configuration data created by previous test.
     */
    public static Long isStopMode;

    /**
     * (boolean) - whether created tables to be replicated or partitioned. snappy hydra already sets the gemfirexd.table-default-partitioned to false.
     */
    public static Long tableDefaultPartitioned;

    /**
     * (boolean) - whether test is long running.
     */
    public static Long isLongRunningTest;

    /**
     * (boolean) - whether to enable time statistics. snappy hydra already sets the enable-time-statistics to true.
     */
    public static Long enableTimeStatistics;

    /**
     * (String) log level to be applied while generating logs for snappy members. Defaults to config if not provided.
     */
    public static Long logLevel;

    /**
     * (String) userAppJar containing the user snappy job class
     */
    public static Long userAppJar;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status
     */
    public static Long streamingJobExecutionTimeInMillis;

    /**
     * (int) how long (milliseconds) it should wait before Cycle VMs again
     */
    public static Long waitTimeBeforeNextCycleVM;

    /**
     * (int) The number of VMs to stop (then restart) at a time.
     */
    public static Long numVMsToStop;

    /**
     * (int) The number of lead VMs to stop (then restart).
     */
    public static Long numLeadsToStop;

    /**
     * Parameter used to get the user APP_PROPS for snappy job.
     * (VectosetValues of Strings) A list of values for snappy-job.
     */
    public static Long appPropsForJobServer;

    /**
     * (int) number of executor cores to be used in test
     */
    public static Long executorCores;

    /**
     * (String) Maximun Result Size for Driver. Defaults to 1GB if not provided.
     */
    public static Long driverMaxResultSize;

    /**
     * (String) Local Memory. Defaults to 1GB if not provided.
     */
    public static Long locatorMemory;

    /**
     * (String) Memory to be used while starting the Server process. Defaults to 1GB if not provided.
     */
    public static Long serverMemory;

    /**
     * (String) Memory to be used while starting the Lead process. Defaults to 1GB if not provided.
     */
    public static Long leadMemory;

    /**
     * (String) sparkSchedulerMode. Defaults to 'FAIR' if not provided.
     */
    public static Long sparkSchedulerMode;

    /**
     * (int) sparkSqlBroadcastJoinThreshold
     */
    public static Long sparkSqlBroadcastJoinThreshold;

    /**
     * (boolean) - whether in-memory Columnar store needs to be compressed . Defaults to false if not provided.
     */
    public static Long compressedInMemoryColumnarStorage;

    /**
     * (boolean) - whether to use conserveSockets. Defaults to false if not provided.
     */
    public static Long conserveSockets;

    /**
     * (int) number of shuffle partitions to be used in test
     */
    public static Long shufflePartitions;

    public static int getExecutorCores() {
        Long key = executorCores;
        return tasktab().intAt(key, tab().intAt(key, 1));
    }

    public static String getDriverMaxResultSize() {
        Long key = driverMaxResultSize;
        return tab().stringAt(key, "1g").toLowerCase();
    }

    public static String getLocatorMemory() {
        Long key = locatorMemory;
        return tab().stringAt(key, "1G");
    }

    public static String getServerMemory() {
        Long key = serverMemory;
        return tab().stringAt(key, "4G");
    }

    public static String getLeadMemory() {
        Long key = leadMemory;
        return tab().stringAt(key, "1G");
    }

    public static String getSparkSchedulerMode() {
        Long key = sparkSchedulerMode;
        return tab().stringAt(key, "FAIR");
    }

    public static int getSparkSqlBroadcastJoinThreshold() {
        Long key = sparkSqlBroadcastJoinThreshold;
        return tasktab().intAt(key, tab().intAt(key, -1));
    }

    public static boolean getCompressedInMemoryColumnarStorage() {
        Long key = compressedInMemoryColumnarStorage;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static boolean getConserveSockets() {
        Long key = conserveSockets;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static int getShufflePartitions() {
        Long key = shufflePartitions;
        return tasktab().intAt(key, tab().intAt(key, 1));
    }

    public static String getCommaSepAPPProps() {
        Long key = appPropsForJobServer;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }

    public static boolean getTableDefaultDataPolicy() {
        Long key = tableDefaultPartitioned;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static boolean getTimeStatistics() {
        Long key = enableTimeStatistics;
        return tasktab().booleanAt(key, tab().booleanAt(key, true));
    }

    public static String getLogLevel() {
        Long key = logLevel;
        return tab().stringAt(key, "config");
    }

    public static Vector getSQLScriptNames() {
        Long key = sqlScriptNames;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static String getUserAppJar() {
        Long key = userAppJar;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }

    public static Vector getSQLScriptParams() {
        Long key = sqlScriptParams;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getSnappyJobClassNames() {
        Long key = jobClassNames;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSparkJobClassNames() {
        Long key = sparkJobClassNames;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyStreamingJobClassNames() {
        Long key = streamingJobClassNames;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    static {
        BasePrms.setValues(SnappyPrms.class);
    }

    public static void main(String args[]) {
        BasePrms.dumpKeys();
    }
}
