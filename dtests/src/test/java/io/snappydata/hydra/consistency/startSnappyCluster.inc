hydra.Prms-testRequirement = "Test to start the snappy cluster in embedded";
hydra.Prms-testDescription = "This test starts the snappy embedded.";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_3.inc;

hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;

THREADGROUP dmlThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP selectThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

THREADGROUP snappyThreads
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${C}\",
                                ${${C}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = snappyThreads, dmlThreads, selectThreads;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = snappyThreads, dmlThreads, selectThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLocatorConfig
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLocatorConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyServerConfig
            runMode = always
            threadGroups = dmlThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeServerConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLeadConfig
            runMode = always
            threadGroups = selectThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLeadConfigData
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_recordProcessIDWithHost
            runMode = always
            threadGroups = snappyThreads, selectThreads, dmlThreads;
