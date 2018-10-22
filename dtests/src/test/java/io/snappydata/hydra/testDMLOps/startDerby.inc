hydra.Prms-extraDerbyServerVMArgs += " -Xmx8192m -Dderby.storage.pageCacheSize=32000 -Dderby.locks.waitTimeout=30 -Dderby.locks.deadlockTimeout=20 ";

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbyDB
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbyTables
	threadGroups = snappyThreads;

hydra.Prms-manageDerbyServer = true;