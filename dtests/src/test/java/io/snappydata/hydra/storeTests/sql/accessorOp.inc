INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams2.inc;
include $JTESTS/hydraconfig/topology_3.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $JTESTS;
hydra.gemfirexd.GfxdHelperPrms-persistDD = fcn "hydra.TestConfigFcns.oneof(\"true\", \"false\")" ncf;

include $JTESTS/io/snappydata/hydra/storeTests/startSnappyCluster.inc;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscDB
    	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscSchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscTables
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFESchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiskStores
		threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFETables
	threadGroups = ddlThread;

/*INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForProcedures
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForSubquery
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFunctionToPopulate
	threadGroups = ddlThread;*/

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = dataStoreThreads;

TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_doDMLOp
	 threadGroups = dataStoreThreads, ddlThread;

CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_verifyResultSets
	 threadGroups = ddlThread;

INCLUDE $JTESTS/io/snappydata/hydra/storeTests/stopSnappyCluster.inc;

hydra.VmPrms-extraVMArgsSUN += "-XX:PermSize=64M -XX:MaxPermSize=256m";

sql.SQLPrms-hasCompanies=false; //true;
sql.SQLPrms-companiesTableDDLExtension = "trade.companies:random";
sql.SQLPrms-numOfWorkers = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf;