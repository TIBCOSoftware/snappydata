TASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod = HydraTask_restartSnappyClusterForStreaming
    io.snappydata.hydra.cluster.SnappyPrms-streamingJobClassNames = io.snappydata.hydra.streaming_sink.SnappyStreamingSinkJob
    io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "kafkaTopic=streamingSinkTopic,streaming.batch_interval=100"
    io.snappydata.hydra.cluster.SnappyPrms-userAppName = streamingApp
    io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
    threadGroups = snappyThreads
    startInterval = 60
    maxTimesToRun = 1;
