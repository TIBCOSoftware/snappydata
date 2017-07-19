io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

hydra.Prms-totalTaskTimeSec           = 240;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_OpsDuringServerHA_clusterRestart
            io.snappydata.hydra.cluster.SnappyPrms-dataLocation = ${dataFilesLocation}
            maxThreads = 1
            startInterval = 60
            threadGroups = snappyThreads
            maxTimesToRun = 1
            ;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyLocatorHATest taskMethod  = HydraTask_validateTableDataOnClusterRestart
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.hydra.northwind.ValidateNWQueriesJob
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer =
            "dataFilesLocation=${dataFilesLocation},tableType=${tableType},fullResultSetValidation=${fullResultSetValidation},isSmokeRun=${isSmokeRun},numRowsValidation=${numRowsValidation}"
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
            threadGroups = snappyThreads;

