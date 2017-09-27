INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpLeadConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpServerConfigData
            threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  =  backUpLocatorConfigData
            threadGroups = snappyThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLeadVM
            maxThreads = 1
            startInterval = 21600
            endInterval = 21600
            threadGroups = snappyStoreThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
            startInterval = 14400
            endInterval = 14400
            maxThreads = 1
            threadGroups = leadThreads;

TASK        taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLocatorVms
            startInterval = 28800
            endInterval = 28800
            maxThreads = 1
            threadGroups = snappyThreads;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLocatorConfigData
           threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLeadConfigData
            threadGroups = snappyThreads;

io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 1800;
io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeRestartVM = 600000;
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;