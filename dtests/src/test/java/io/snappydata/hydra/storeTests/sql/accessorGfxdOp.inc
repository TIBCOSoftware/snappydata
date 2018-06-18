INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/topology_3.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $JTESTS;
 
include $JTESTS/io/snappydata/hydra/storeTests/startSnappyCluster.inc;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscDB
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscSchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFESchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiskStores
		threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createUDTPriceType
	threadGroups = ddlThread;	

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createUUIDType
	threadGroups = ddlThread;	
/*
INITTASK     taskClass   = sql.SQLTest taskMethod  = hydraTask_createUDTPriceFunctions
	threadGroups = ddlThread;				
*/
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscTables
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFETables
	threadGroups = ddlThread;
/*
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForProcedures
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForSubquery
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFunctionToPopulate
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createCompaniesTable
	threadGroups = ddlThread;
		
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_dropCompaniesFK
	threadGroups = ddlThread;
*/
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = dataStoreThreads;
/*
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_addCompaniesFK
	threadGroups = ddlThread;		

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createIndexOnCompanies
	threadGroups = ddlThread;				
	*/
TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_doDMLOp
	 threadGroups = dataStoreThreads, ddlThread;
			
CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_verifyResultSets
	 threadGroups = ddlThread;

CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_checkConstraints
	 threadGroups = ddlThread;	

INCLUDE $JTESTS/io/snappydata/hydra/storeTests/stopSnappyCluster.inc;

hydra.Prms-totalTaskTimeSec           = 100;
hydra.Prms-maxResultWaitSec           = 3600;

hydra.Prms-maxCloseTaskResultWaitSec  = 3600;
hydra.Prms-serialExecution            = false;

hydra.VmPrms-extraVMArgsSUN += "-XX:PermSize=64M -XX:MaxPermSize=256m";

sql.SQLPrms-hasCompanies=false; //true;
sql.SQLPrms-companiesTableDDLExtension = "trade.companies:random";
sql.SQLPrms-numOfWorkers = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf;
