TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -dir=/nfs/users/spillai/tmgdata/lead -locators=dev11:10334 -heap-size=4g -memory-size=4g -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = leads
            startInterval = 64800 //after 18 hours
            endInterval   = 64800
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev7 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev7 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 14400 //after 4 hours
            endInterval   = 14400
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev10 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev10 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 28800  //after 8 hours
            endInterval   = 28800
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev9 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev9-spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 36000 //after 10 hours
            endInterval   = 36000
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev11 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 50400  //after 14 hours
            endInterval   = 50400
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;