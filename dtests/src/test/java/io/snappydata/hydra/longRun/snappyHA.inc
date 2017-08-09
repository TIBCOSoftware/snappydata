INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpLeadConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpServerConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  =  backUpLocatorConfigData
            threadGroups = snappyThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLeadVM
            maxThreads = 1
            startInterval = 3600
            endInterval = 3600
            threadGroups = snappyStoreThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
            startInterval = 1800
            endInterval = 1800
            maxThreads = 1
            threadGroups = leadThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLocatorVms
            startInterval = 5400
            endInterval = 5400
            maxThreads = 1
            threadGroups = snappyThreads;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLocatorConfigData
           threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLeadConfigData
            threadGroups = snappyThreads;

io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 1800;
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;