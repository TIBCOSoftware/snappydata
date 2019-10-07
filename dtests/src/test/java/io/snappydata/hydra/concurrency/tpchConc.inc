INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_5.inc;

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

THREADGROUP snappyClientPThreads
            totalThreads = fcn "(${${D}Hosts} * ${${D}VMsPerHost} *  ${${D}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${D}Hosts} * ${${D}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${D}\",
                                ${${D}Hosts}, true)" ncf;

THREADGROUP snappyClientAThreads
            totalThreads = fcn "(${${E}Hosts} * ${${E}VMsPerHost} *  ${${E}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${E}Hosts} * ${${E}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${E}\",
                                ${${E}Hosts}, true)" ncf;

THREADGROUP snappyThreads
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads,
            snappyClientPThreads, snappyClientAThreads;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
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

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_recordProcessIDWithHost
            runMode = always
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads;

// create and load tables using snappyJob
INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest
            taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.benchmark.snappy.tpch.TableCreationJob
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "dataLocation=${dataDir},Buckets_Order_Lineitem=${buckets_Order_Lineitem},Buckets_Cust_Part_PartSupp=${buckets_Cust_Part_PartSupp},Buckets_Nation_Region_Supp=${buckets_Nation_Region_Supp},Nation_Region_Supp_col=${Nation_Region_Supp_col},Redundancy=${Redundancy},Persistence=${Persistence},Persistence_Type=${Persistence_Type},NumberOfLoadStages=${NumberOfLoadStages},isParquet=${Parquet}"
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-cluster*tests.jar
            threadGroups = snappyThreads
            ;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyConcurrencyTest taskMethod  = validateNumQueriesExecuted
            threadGroups = snappyThreads;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_deleteSnappyConfig
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cleanUpSnappyProcessesOnFailure
            clientNames = locator1;

hydra.Prms-totalTaskTimeSec           = 900;
hydra.Prms-maxResultWaitSec           = 3600;

hydra.Prms-maxCloseTaskResultWaitSec  = 3600;
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
