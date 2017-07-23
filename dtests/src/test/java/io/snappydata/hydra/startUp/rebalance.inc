INCLUDE $JTESTS/hydraconfig/topology_5.inc;

THREADGROUP newStoreThreads
            totalThreads = fcn "(${${E}Hosts} * ${${E}VMsPerHost} *  ${${E}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${E}Hosts} * ${${E}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${E}\",
                                ${${E}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads,
            workerThreads, newStoreThreads;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads, newStoreThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_AddServerNode_Rebalance
            maxThreads = 1
            startInterval = 60
            threadGroups = newStoreThreads
            maxTimesToRun = 1
            ;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.hydra.northwind.ValidateNWQueriesJob
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer =
            "dataFilesLocation=${dataFilesLocation},tableType=${tableType},fullResultSetValidation=${fullResultSetValidation},isSmokeRun=${isSmokeRun},numRowsValidation=${numRowsValidation}"
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
            threadGroups = snappyThreads
            maxTimesToRun = 1;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_reWriteServerConfigData
            threadGroups = snappyThreads;

INCLUDE $JTESTS/io/snappydata/hydra/startUp/clusterRestart.inc;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.hydra.northwind.ValidateNWQueriesJob
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer =
            "dataFilesLocation=${dataFilesLocation},tableType=${tableType},fullResultSetValidation=${fullResultSetValidation},isSmokeRun=${isSmokeRun},numRowsValidation=${numRowsValidation}"
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
            threadGroups = snappyThreads;