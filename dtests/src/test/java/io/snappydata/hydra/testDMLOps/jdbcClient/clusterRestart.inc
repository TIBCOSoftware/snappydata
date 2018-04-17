CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
           threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = snappyThreads;

io.snappydata.hydra.cluster.SnappyPrms-forceStart = true;