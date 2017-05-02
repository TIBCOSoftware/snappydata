
INITTASK     taskClass   = io.snappydata.hydra.snapshotIsolation.SnapshotIsolationTest taskMethod  = HydraTask_createDerbyDB
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.snapshotIsolation.SnapshotIsolationTest taskMethod  = HydraTask_createDerbySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.snapshotIsolation.SnapshotIsolationTest taskMethod  = HydraTask_createDerbyTables
	threadGroups = snappyThreads;

CLOSETASK     taskClass   = io.snappydata.hydra.snapshotIsolation.SnapshotIsolationTest taskMethod  = HydraTask_shutDownDerbyDB
	threadGroups = snappyThreads;

hydra.Prms-manageDerbyServer = true;