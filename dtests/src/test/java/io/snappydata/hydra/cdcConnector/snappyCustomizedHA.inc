TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -dir=/nfs/users/spillai/tmgdata/lead -locators=dev11:10334 -heap-size=4g -memory-size=4g -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = leads
            startInterval = 7200  //after 2 hours
            endInterval   = 7200
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -dir=/nfs/users/spillai/tmgdata/locator"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = locator
            startInterval = 3600 //after 1 hours
            endInterval   = 3600
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev8 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev8 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.108"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 600 //after 10mins
            endInterval   = 600
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev9 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev9 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.109"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 1200  //after 20 mins
            endInterval   = 1200
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev12 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev12 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.112"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 300 //after 5mins
            endInterval   = 300
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev11 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
            startInterval = 1800  //after 30 mins
            endInterval   = 1800
            threadGroups = snappyHAThread
            maxThreads = 1
            maxTimesToRun = 1;

TASK        taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeInfoforHA="dev11 -locators=dev11:10334 -heap-size=10g -memory-size=80g -dir=/nfs/users/spillai/tmgdata/dev11 -spark.executor.cores=16 -J-XX:+HeapDumpOnOutOfMemoryError -critical-heap-percentage=95 -spark.local.dir=/nfs/users/spillai/tmp -hostname-for-clients=10.80.141.111"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = "/export/dev11a/users/spillai/snappydata/build-artifacts/scala-2.11/snappy"
            io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = allNodes
            startInterval = 14400  //after 4 hours
            endInterval   = 14400
            threadGroups = snappyHAThread
            maxThreads = 1;
            //maxTimesToRun = 1;