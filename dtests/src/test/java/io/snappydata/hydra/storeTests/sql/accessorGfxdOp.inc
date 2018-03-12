INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/topology_3.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $JTESTS;
 
THREADGROUP locator
  totalThreads = fcn  ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM} ncf
  totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                                ${${A}Hosts}, true)" ncf;
THREADGROUP dataStoreThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) - 1 " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

THREADGROUP accessorThreads
            totalThreads = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${C}Hosts} * ${${C}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${C}\",
                                ${${C}Hosts}, true)" ncf;

THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                               ${${B}Hosts}, true)" ncf;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_initializeForSnappy
	threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;


INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
            runMode = always
            threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = dataStoreThreads, locator, accessorThreads, ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyLocatorConfig
            runMode = always
            threadGroups = locator;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeLocatorConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyServerConfig
            runMode = always
            threadGroups = dataStoreThreads;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeServerConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_generateSnappyLeadConfig
            runMode = always
            threadGroups = accessorThreads;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_writeLeadConfigData
            runMode = always
            threadGroups = ddlThread;

INITTASK    taskClass   = sql.snappy.SnappyTest taskMethod  = HydraTask_startSnappyCluster
            runMode = always
            threadGroups = ddlThread;


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
*/
/*
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

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappy
            threadGroups = ddlThread;

CLOSETASK   taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSparkCluster
            threadGroups = ddlThread;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_stopSnappyCluster
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_deleteSnappyConfig
            clientNames = locator1;

ENDTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cleanUpSnappyProcessesOnFailure
            clientNames = locator1;

hydra.Prms-totalTaskTimeSec           = 100;
hydra.Prms-maxResultWaitSec           = 3600;

hydra.Prms-maxCloseTaskResultWaitSec  = 3600;
hydra.Prms-serialExecution            = false;

hydra.CachePrms-names = defaultCache;
sql.SQLPrms-useGfxdConfig = true;

/* end task must stop snappy members because they are not stopped by Hydra */
hydra.Prms-alwaysDoEndTasks = true;

hydra.VmPrms-extraVMArgs   += fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx1g \", ${${A}Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx1g \", ${${B}Hosts}, true)"
                             ncf;
hydra.VmPrms-extraVMArgsSUN += "-XX:PermSize=64M -XX:MaxPermSize=256m";

io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar;

hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;

sql.SQLPrms-useGfxdConfig = true;
sql.SQLPrms-hasCompanies=false; //true;
sql.SQLPrms-companiesTableDDLExtension = "trade.companies:random";
sql.SQLPrms-numOfWorkers = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf;
