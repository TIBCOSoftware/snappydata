INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;

hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;


THREADGROUP snappyThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) -1 " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP snappySingleThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP snappyClientAThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = initSnappyArtifacts
            runMode = always
            threadGroups = snappyThreads, snappySingleThread, snappyClientAThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyConcurrencyTest taskMethod  = createViewsAndDependantTablesForStabilityTest
            threadGroups = snappySingleThread;


hydra.Prms-totalTaskTimeSec           = 259200;
hydra.Prms-maxResultWaitSec           = 266400;

hydra.Prms-maxCloseTaskResultWaitSec  = 266400;
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

io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar;
io.snappydata.hydra.cluster.SnappyPrms-isLongRunningTest = true;
io.snappydata.hydra.cluster.SnappyPrms-isStabilityTest = true;
