
INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_initialize
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbyDB
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbySchemas
	threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_createDerbyTables
	threadGroups = snappyThreads;

CLOSETASK     taskClass   = io.snappydata.hydra.testDMLOps.DerbyTestUtils taskMethod  = HydraTask_shutDownDerbyDB
	threadGroups = snappyThreads;

hydra.Prms-manageDerbyServer = true;