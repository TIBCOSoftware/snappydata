TASK       taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeName= dev11
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = ${snappyFileLoc}
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = leads
           startInterval = 7200  //after 2 hours
           endInterval   = 7200
           threadGroups = snappyHAThread
           maxThreads = 1
           maxTimesToRun = 1;

TASK       taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeName= dev11
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = ${snappyFileLoc}
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = locator
           startInterval = 3600 //after 1 hours
           endInterval   = 3600
           threadGroups = snappyHAThread
           maxThreads = 1
           maxTimesToRun = 1;

TASK       taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = ${snappyFileLoc}
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeName= dev11 dev7 dev8 dev9
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = servers
           startInterval = 600 //after 10mins
           endInterval   = 600
           threadGroups = snappyThreads
           maxThreads = 1
           maxTimesToRun = 5;

TASK       taskClass  = io.snappydata.hydra.cdcConnector.SnappyCDCTest taskMethod  = performHA
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-snappyFileLoc = ${snappyFileLoc}
           io.snappydata.hydra.cdcConnector.SnappyCDCPrms-nodeType = allNodes
           startInterval = 14400  //after 4 hours
           endInterval   = 14400
           threadGroups = snappyHAThread
           maxThreads = 1;
