ENDTASK     taskClass = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_dumpStacks
            clientNames = locator1;

ENDTASK     taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            clientNames = locator1;

ENDTASK     taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_deleteSnappyConfig
            clientNames = locator1;

ENDTASK     taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_cleanUpSnappyProcessesOnFailure
            clientNames = locator1;

hydra.Prms-alwaysDoEndTasks = true;