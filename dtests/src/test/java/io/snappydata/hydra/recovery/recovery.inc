hydra.Prms-testRequirement = "Test to verify cluster restart with persistent recovery using ODS
schema";
hydra.Prms-testDescription = "
This test starts the snappy cluster and spark cluster, then initializes snappyContext.
Creates persistent Row Tables required for ods schema use case using SQL way, loads the csv
data into tables using snappy job.
It then restarts the cluster 2-3 times.
Test verifies that each time, the cluster is restarted properly within expected time and
all the tables are recovered with persisted data and also verifies that the primary buckets and
secondary buckets and their corresponding indexes are all containing data as expected.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_3.inc;

hydra.gemfirexd.GfxdHelperPrms-persistDD = true;
hydra.gemfirexd.GfxdHelperPrms-createDiskStore = true;
hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;

THREADGROUP snappyStoreThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP leadThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) -1 " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

THREADGROUP locatorThreads
            totalThreads = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${C}Hosts} * ${${C}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${C}\",
                                ${${C}Hosts}, true)" ncf;

THREADGROUP snappyThreads
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLocatorConfig
            runMode = always
            threadGroups = locatorThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLocatorConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyServerConfig
            runMode = always
            threadGroups = snappyStoreThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeServerConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLeadConfig
            runMode = always
            threadGroups = leadThreads, snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLeadConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.hydra.LoadODSData
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar =    snappydata-store-scala-tests*tests.jar
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "rowTableList=0-1-2,dataLocation=${dataLocation}"
            io.snappydata.hydra.cluster.SnappyPrms-numTimesToRetry = 2
            maxTimesToRun = 1
            threadGroups = snappyThreads
            ;
CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_executeDiagnosticQueriesBeforeRecovery
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_executeDiagnosticQueriesAfterRecovery
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod = HydraTask_verifyTableData
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_executeDiagnosticQueriesAfterRecovery
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod = HydraTask_verifyTableData
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappy
            threadGroups = snappyThreads;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_deleteSnappyConfig
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cleanUpSnappyProcessesOnFailure
            clientNames = locator1;

hydra.Prms-totalTaskTimeSec           = 100;
hydra.Prms-maxResultWaitSec           = 8400;

hydra.Prms-maxCloseTaskResultWaitSec  = 900;
hydra.Prms-serialExecution            = false;

hydra.CachePrms-names = defaultCache;
sql.SQLPrms-useGfxdConfig = true;

/* end task must stop snappy members because they are not stopped by Hydra */
hydra.Prms-alwaysDoEndTasks = true;

hydra.VmPrms-extraVMArgs   += fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx1g \", ${${A}Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx1g \", ${${B}Hosts}, true)"
                             ncf;
hydra.VmPrms-extraVMArgsSUN += "-XX:PermSize=64M -XX:MaxPermSize=256m";

io.snappydata.hydra.cluster.SnappyPrms-forceStart = true;
io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar;
io.snappydata.hydra.cluster.SnappyPrms-columnBatchSize = 100000;
io.snappydata.hydra.cluster.SnappyPrms-tableList = ODS.PERSONS ODS.CLAIM_DETAIL ODS.CLAIM_STATUS;
io.snappydata.hydra.cluster.SnappyPrms-indexList = IX_PERSONS_02 IX_PERSONS_03 IX_PERSONS_04 IX_CLAIM_DETAIL_01 IX_CLAIM_DETAIL_02 IX_CLAIM_STATUS_01 IX_CLAIM_STATUS_02;
io.snappydata.hydra.cluster.SnappyPrms-persistIndexes = ONEOF true false FOENO;
//io.snappydata.hydra.cluster.SnappyPrms-connPropsList = "route-query=false log-level=fine";
io.snappydata.hydra.cluster.SnappyPrms-connPropsList = "route-query=false";