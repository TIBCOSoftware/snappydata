CLOSETASK   taskClass   = sql.snappy.SnappyTest taskMethod = HydraTask_stopSnappyCluster
            runMode = always
            threadGroups = ddlThread;

CLOSETASK   taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = ddlThread;
