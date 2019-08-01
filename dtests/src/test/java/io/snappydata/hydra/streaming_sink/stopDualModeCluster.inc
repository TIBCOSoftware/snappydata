ENDTASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_dumpStacks
   clientNames = locator1;

ENDTASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSparkCluster
   clientNames = locator1;

ENDTASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
   clientNames = locator1;

ENDTASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_deleteSnappyConfig
    clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StopKafkaBrokers
    clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StopKafkaZookeeper
    clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cleanUpSnappyProcessesOnFailure
    clientNames = locator1;
