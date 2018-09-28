io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = 1;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = backUpLeadConfigData
            threadGroups = snappyThreads;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleLeadVM
           startInterval = 60
           threadGroups = snappyThreads
           maxTimesToRun = 1;
/*
TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_restartLeadVMWithStreaming
           io.snappydata.hydra.cluster.SnappyPrms-streamingJobClassNames = io.snappydata.hydra.streaming_sink.SnappyStreamingSinkJob
           io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "kafkaTopic=streamingSinkTopic,streaming.batch_interval=100"
           io.snappydata.hydra.cluster.SnappyPrms-userAppName = streamingApp
           io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
           maxThreads = 1
           startInterval = 60
           threadGroups = snappyThreads
           maxTimesToRun = 1;
*/

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreLeadConfigData
           threadGroups = snappyThreads;
