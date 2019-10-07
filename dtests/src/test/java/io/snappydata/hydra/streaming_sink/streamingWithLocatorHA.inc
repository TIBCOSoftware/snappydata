io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = 1;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  =  backUpLocatorConfigData
            threadGroups = snappyThreads;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLocatorVms
           startInterval = 60
           threadGroups = leadThread
           io.snappydata.hydra.cluster.SnappyPrms-sleepTimeSecsBeforeRestart = 60
           maxTimesToRun = 1;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLocatorConfigData
           threadGroups = snappyThreads;