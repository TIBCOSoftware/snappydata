INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpLeadConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpServerConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  =  backUpLocatorConfigData
            threadGroups = snappyThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLeadVM
            maxThreads = 1
            startInterval = 360  //10 hours
            endInterval = 360
            threadGroups = snappyStoreThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
            startInterval = 1500  //  25 mins
            endInterval = 1500
            maxThreads = 1
            threadGroups = snappyThreads;


TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLocatorVms
            startInterval = 28800  // 24 hours
            endInterval = 28800
            maxThreads = 1
            threadGroups = snappyThreads;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLocatorConfigData
           threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLeadConfigData
            threadGroups = snappyThreads;

io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 10;
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;