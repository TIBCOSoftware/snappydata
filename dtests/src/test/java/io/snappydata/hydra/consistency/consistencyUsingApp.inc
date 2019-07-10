INCLUDE $JTESTS/io/snappydata/hydra/connectionPool/connPool.inc;

INCLUDE $JTESTS/io/snappydata/hydra/consistency/schema.inc;

INCLUDE $JTESTS/io/snappydata/hydra/northwind/startDualModeCluster.conf;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/startDerby.inc;

INITTASK     taskClass   = io.snappydata.hydra.consistency.SnappyConsistencyTest taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads,snappyStoreThreads,leadThreads,locatorThreads,workerThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads,snappyStoreThreads,leadThreads,locatorThreads,workerThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappyTables
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_saveTableMetaDataToBB
  threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.consistency.SnappyConsistencyTest taskMethod  = HydraTask_registerDMLThreads
  threadGroups = snappyStoreThreads;

//Load some data in the data, before starting any operations.
INITTASK     taskClass   = io.snappydata.hydra.consistency.SnappyConsistencyTest taskMethod  = HydraTask_populateTables
  io.snappydata.hydra.testDMLOps.SnappySchemaPrms-batchSize =1000000
  threadGroups = snappyThreads;

//task for verifying the populated data.
INITTASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_verifyResultsInitTask
  threadGroups = snappyThreads;

//task for running dmlOps
TASK        taskClass   = io.snappydata.hydra.consistency.SnappyConsistencyTest taskMethod  = HydraTask_performDMLOPsAndVerifyConsistencyInApp
  io.snappydata.hydra.cluster.SnappyPrms-sparkJobClassNames = io.snappydata.hydra.consistency.ValidateConsistencyWithDMLOpsApp
  io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
  io.snappydata.hydra.cluster.SnappyPrms-hasDynamicAppProps = true
  io.snappydata.hydra.testDMLOps.SnappySchemaPrms-batchSize =100
  threadGroups = snappyStoreThreads;

//task for running select queries at the end of test
CLOSETASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod = HydraTask_verifyResultsInitTask
  threadGroups = snappyThreads;

INCLUDE $JTESTS/io/snappydata/hydra/northwind/stopDualModeCluster.conf;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/stopDerby.inc;

hydra.Prms-totalTaskTimeSec           = 300;
hydra.Prms-maxResultWaitSec           = 900;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-testUniqueKeys = true;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-useCsv = false;
