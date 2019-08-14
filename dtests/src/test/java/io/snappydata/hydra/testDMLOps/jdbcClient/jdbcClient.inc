INCLUDE $JTESTS/io/snappydata/hydra/connectionPool/connPool.inc;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/nw_schema.inc;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/columnTableExtn.inc; //change this as per table type

INCLUDE $JTESTS/io/snappydata/hydra/northwind/startDualModeCluster.conf;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads,leadThreads,snappyStoreThreads;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/startDerby.inc;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_createSnappyTables
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_saveTableMetaDataToBB
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_registerDMLThreads
	threadGroups = snappyStoreThreads;

//Load some data in the data, before starting any operations.
INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_populateTablesUsingSysProc
  io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.hydra.testDMLOps.ConvertCsvToParquetAndLoadTablesJob
  io.snappydata.hydra.cluster.SnappyPrms-hasDynamicAppProps = true
  io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
  threadGroups = snappyThreads;

/*
// load data using batch insert
INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_populateTables
	threadGroups = snappyThreads;
*/

//task for verifying the populated data.
INITTASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_verifyResultsInitTask
  threadGroups = snappyThreads;

//task for running select queries at the end of test
CLOSETASK   taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod = HydraTask_verifyResultsInitTask
threadGroups = snappyThreads;

INCLUDE $JTESTS/io/snappydata/hydra/northwind/stopDualModeCluster.conf;
INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/stopDerby.inc;

hydra.Prms-totalTaskTimeSec           = 900;
hydra.Prms-maxResultWaitSec           = 600;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-testUniqueKeys = true;
io.snappydata.hydra.testDMLOps.SnappySchemaPrms-largeDataSet = true;
