hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;
io.snappydata.hydra.cluster.SnappyPrms-kafkaDir="${kafkaDir}";
io.snappydata.hydra.cluster.SnappyPrms-kafkaLogDir="${kafkaLogDir}";


//specified user defined configuration in local.putIntoStability.conf
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_1.inc;

THREADGROUP snappyThread
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) - 1 " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP snappyInitThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;


INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = snappyInitThread,snappyThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_copyUserConfs
            runMode = always
            threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLocatorInfo
            runMode = always
            threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writePrimaryLocatorHostPortInfo
            runMode = always
            threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLeadHostPortInfo
            runMode = always
            threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_recordProcessIDWithHostWithUserConfsTest
            runMode = always
            threadGroups = snappyInitThread;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_initializeSnappyAdAnalyticsTest
    runMode = always

    threadGroups = snappyInitThread,snappyThread;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StartKafkaZookeeper
    runMode = always
    threadGroups = snappyInitThread;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StartKafkaBrokers
    runMode = always
    threadGroups = snappyInitThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
    runMode = always
    threadGroups = snappyInitThread;

hydra.Prms-totalTaskTimeSec           = 259200;
hydra.Prms-maxResultWaitSec           = 3600;

hydra.Prms-maxCloseTaskResultWaitSec  = 3600;

io.snappydata.hydra.cluster.SnappyPrms-isStopMode = true;
io.snappydata.hydra.cluster.SnappyPrms-isLongRunningTest = true;
io.snappydata.hydra.cluster.SnappyPrms-isUserConfTest = true;

io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar;
