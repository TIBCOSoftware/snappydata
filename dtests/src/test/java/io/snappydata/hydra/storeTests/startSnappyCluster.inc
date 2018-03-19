INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_initializeForSnappy
	threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;


INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyLocatorConfig
            runMode = always
            threadGroups = locator;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeLocatorConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyServerConfig
            runMode = always
            threadGroups = dataStoreThreads;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeServerConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyLeadConfig
            runMode = always
            threadGroups = accessorThreads;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeLeadConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = ddlThread;

hydra.CachePrms-names = defaultCache;
hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;
sql.SQLPrms-useGfxdConfig = true;