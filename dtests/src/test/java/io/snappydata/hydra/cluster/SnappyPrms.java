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
     * Parameter used to get the user specified data location List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for dataLocation to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long dataLocation;

    /**
     * Parameter used to get the user specified persistence mode List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for persistenceMode to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as "async" in this case.
     */
    public static Long persistenceMode;

    /**
     * Parameter used to get the user specified PARTITION_BY option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for PARTITION_BY option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long partitionBy;

    /**
     * Parameter used to get the user specified BUCKETS option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for BUCKETS option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as "113 " in this case.
     */
    public static Long numPartitions;

    /**
     * Parameter used to get the user specified colocation option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for COLOCATE_WITH option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as "none" in this case.
     */
    public static Long colocateWith;

    /**
     * Parameter used to get the user specified REDUNDANCY option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for REDUNDANCY option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long redundancy;

    /**
     * Parameter used to get the user specified RECOVER_DELAY option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for RECOVER_DELAY option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long recoverDelay;

    /**
     * Parameter used to get the user specified MAX_PART_SIZE option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for MAX_PART_SIZE option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long maxPartitionSize;

    /**
     * Parameter used to get the user specified EVICTION_BY option List for the sql scripts.
     * (VectorsetValues of Strings) A list of values for EVICTION_BY option to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNames parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long evictionBy;

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
     * (boolean) - whether to enable closedForm Estimates. snappy hydra already sets the spark.sql.aqp.closedFormEstimates to true.
     */
    public static Long closedFormEstimates;

    /**
     * (boolean) - whether to enable zeppelin Interpreter. snappy hydra already sets the zeppelin.interpreter.enable to false.
     */
    public static Long zeppelinInterpreter;

    /**
     * (boolean) - whether to enable Java Flight Recorder (JFR) for collecting diagnostic and profiling data while launching server and lead members in cluster. Defaults to false if not provided.
     */
    public static Long enableFlightRecorder;

    /**
     * (String) log level to be applied while generating logs for snappy members. Defaults to config if not provided.
     */
    public static Long logLevel;

    /**
     * (String) userAppJar containing the user snappy job class. The wildcards in jar file name are supported in order to removes the hard coding of jar version.
     * e.g. user can specify the jar file name as "snappydata-store-scala-tests*tests.jar" instead of full jar name as "snappydata-store-scala-tests-0.1.0-SNAPSHOT-tests.jar".
     */
    public static Long userAppJar;

    /**
     * (String) AppName for the user app jar containing snappy job class.
     */
    public static Long userAppName;

    /**
     * (String) A unique identifier for the JAR installation. The identifier you provide must specify a schema name delimiter. For example: APP.myjar.
     */
    public static Long jarIdentifier;

    /**
     * (String) args to be passed to the Spark App
     */
    public static Long userAppArgs;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status
     */
    public static Long streamingJobExecutionTimeInMillis;

    /**
     * (int) how long (milliseconds) it should wait before Cycle VMs again
     */
    public static Long waitTimeBeforeNextCycleVM;

    /**
     * (int) Number of times the test should retry submitting failed job in case of lead node failover.
     */
    public static Long numTimesToRetry;

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
     * (String) criticalHeapPercentage to be used while starting the Server process. Defaults to 90% if not provided.
     */
    public static Long criticalHeapPercentage;

    /**
     * (String) evictionHeapPercentage to be used while starting the Server process. Defaults to 90% of critical-heap-percentage if not provided.
     */
    public static Long evictionHeapPercentage;

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
     * (long) inMemoryColumnarStorageBatchSize
     */
    public static Long inMemoryColumnarStorageBatchSize;

    /**
     * (boolean) - whether to use conserveSockets. Defaults to false if not provided.
     */
    public static Long conserveSockets;

    /**
     * (int) number of BootStrap trials to be used in test
     */
    public static Long numBootStrapTrials;

    /**
     * (int) number of shuffle partitions to be used in test
     */
    public static Long shufflePartitions;

    public static int getRetryCountForJob() {
        Long key = numTimesToRetry;
        return tasktab().intAt(key, tab().intAt(key, 5));
    }

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

    public static String getCriticalHeapPercentage() {
        String criticalHeapPercentageString = " -critical-heap-percentage=" + tab().stringAt(criticalHeapPercentage, "90");
        return criticalHeapPercentageString;
    }

    public static String calculateDefaultEvictionPercentage() {
        int criticalHeapPercent = Integer.parseInt(tab().stringAt(criticalHeapPercentage, "90"));
        int evictionHeapPercent = (criticalHeapPercent * 90) / 100;
        String evictionHeapPercentString = String.valueOf(evictionHeapPercent);
        return evictionHeapPercentString;
    }

    public static String getEvictionHeapPercentage() {
        String evictionHeapPercentageString = " -eviction-heap-percentage=" + tab().stringAt(evictionHeapPercentage, calculateDefaultEvictionPercentage());
        return evictionHeapPercentageString;
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

    public static long getInMemoryColumnarStorageBatchSize() {
        Long key = inMemoryColumnarStorageBatchSize;
        return tasktab().longAt(key, tab().longAt(key, 10000));
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

    public static String getTimeStatistics() {
        boolean enableTimeStats = tasktab().booleanAt(enableTimeStatistics, tab().booleanAt(enableTimeStatistics, true));
        String timeStatistics = " -enable-time-statistics=" + enableTimeStats + " -statistic-archive-file=statArchive.gfs";
        return timeStatistics;
    }

    public static String getClosedFormEstimates() {
        boolean enableClosedFormEstimates = tasktab().booleanAt(closedFormEstimates, tab().booleanAt(closedFormEstimates, true));
        String closedFormEstimates = " -spark.sql.aqp.closedFormEstimates=" + enableClosedFormEstimates;
        return closedFormEstimates;
    }

    public static String getZeppelinInterpreter() {
        boolean enableZeppelinInterpreter = tasktab().booleanAt(zeppelinInterpreter, tab().booleanAt(zeppelinInterpreter, false));
        String zeppelinInterpreter = " -zeppelin.interpreter.enable=" + enableZeppelinInterpreter;
        return zeppelinInterpreter;
    }

    public static String getFlightRecorderOptions(String dirPath) {
        boolean flightRecorder = tasktab().booleanAt(enableFlightRecorder, tab().booleanAt(enableFlightRecorder, false));
        if (flightRecorder) {
            String flightRecorderOptions = " -J-XX:+UnlockCommercialFeatures -J-XX:+FlightRecorder -J-XX:FlightRecorderOptions=defaultrecording=true,disk=true,repository=" +
                    dirPath + ",maxage=6h,dumponexit=true,dumponexitpath=flightRecorder.jfr";
            return flightRecorderOptions;
        } else return "";
    }

    public static int getNumBootStrapTrials() {
        Long key = numBootStrapTrials;
        return tasktab().intAt(key, tab().intAt(key, 100));
    }

    public static String getLogLevel() {
        String snappyLogLevel = " -log-level=" + tab().stringAt(logLevel, "config");
        return snappyLogLevel;
    }

    public static Vector getSQLScriptNames() {
        Long key = sqlScriptNames;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static String getUserAppJar() {
        Long key = userAppJar;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }

    public static String getUserAppName() {
        Long key = userAppName;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, "myApp"));
    }

    public static String getJarIdentifier() {
        Long key = jarIdentifier;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }

    public static String getUserAppArgs() {
        Long key = userAppArgs;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, " "));
    }

    public static Vector getDataLocationList() {
        Long key = dataLocation;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getPersistenceModeList() {
        Long key = persistenceMode;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getColocateWithOptionList() {
        Long key = colocateWith;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getPartitionByOptionList() {
        Long key = partitionBy;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getNumPartitionsList() {
        Long key = numPartitions;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getRedundancyOptionList() {
        Long key = redundancy;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getRecoverDelayOptionList() {
        Long key = recoverDelay;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getMaxPartitionSizeList() {
        Long key = maxPartitionSize;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
    }

    public static Vector getEvictionByOptionList() {
        Long key = evictionBy;
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
