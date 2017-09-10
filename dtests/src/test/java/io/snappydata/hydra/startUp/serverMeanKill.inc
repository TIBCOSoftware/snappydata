io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 30;
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = HydraTask_clusterRestartWithRandomOrderForServerStartUp
           maxThreads = 1
           startInterval = 60
           threadGroups = snappyThreads
           maxTimesToRun = ${snappyStoreHosts}
           ;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
           threadGroups = snappyThreads;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyStartUpTest taskMethod  = randomizeServerConfigData
           threadGroups = snappyThreads;
