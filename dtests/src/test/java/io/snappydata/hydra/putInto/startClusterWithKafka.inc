hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;
io.snappydata.hydra.cluster.SnappyPrms-kafkaDir="${kafkaDir}";


//INCLUDE $JTESTS/io/snappydata/hydra/cluster/startEmbeddedModeClusterWithUserConfs.conf;
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

hydra.Prms-totalTaskTimeSec           = 600;
hydra.Prms-maxResultWaitSec           = 3600;

hydra.Prms-maxCloseTaskResultWaitSec  = 3600;

io.snappydata.hydra.cluster.SnappyPrms-isStopMode = true;
io.snappydata.hydra.cluster.SnappyPrms-isLongRunningTest = true;
io.snappydata.hydra.cluster.SnappyPrms-isUserConfTest = true;

// Below parameters to be added in local.conf while running the test
io.snappydata.hydra.cluster.SnappyPrms-locatorList = "localhost:1527,localhost:1528";
io.snappydata.hydra.cluster.SnappyPrms-primaryLocatorHost = "localhost";
io.snappydata.hydra.cluster.SnappyPrms-primaryLocatorPort = "1527";
io.snappydata.hydra.cluster.SnappyPrms-leadHost = "localhost";
io.snappydata.hydra.cluster.SnappyPrms-leadPort = "8090";
io.snappydata.hydra.cluster.SnappyPrms-hostNames = "localhost localhost";

io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar;