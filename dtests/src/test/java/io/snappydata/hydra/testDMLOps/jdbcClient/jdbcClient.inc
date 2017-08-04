//task for running select queries at the end of test
CLOSETASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod = HydraTask_verifyResults
  threadGroups = snappyThreads,snappyStoreThreads;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/nw_schema.inc;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/replicatedRowExtn.inc; //change this as per table type

INCLUDE $JTESTS/io/snappydata/hydra/northwind/startDualModeCluster.conf;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads,leadThreads,snappyStoreThreads;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/derby.inc;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappyTables
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initializeTablesMetaData
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initializeDMLThreads
	threadGroups = snappyThreads,leadThreads,snappyStoreThreads;

//Load some data in the data, before starting any operations.
INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_populateTables
	threadGroups = snappyThreads;

//task for verifying the populated data.
INITTASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_verifyResults
  threadGroups = snappyThreads;

INCLUDE $JTESTS/io/snappydata/hydra/northwind/stopDualModeCluster.conf;
hydra.Prms-totalTaskTimeSec           = 600;
hydra.Prms-maxResultWaitSec           = 300;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-testUniqueKeys = true;

