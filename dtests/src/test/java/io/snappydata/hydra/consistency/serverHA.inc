io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
           maxThreads = 1
           startInterval = 60
           threadGroups = selectThreads
           maxTimesToRun = 3
           ;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
           threadGroups = snappyThreads;
