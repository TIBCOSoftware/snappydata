INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;

hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;


THREADGROUP snappyThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) -1 " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP snappySingleThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP snappyClientAThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = initSnappyArtifacts
            runMode = always
            threadGroups = snappyThreads, snappySingleThread, snappyClientAThreads;

/*INITTASK  taskClass   = io.snappydata.hydra.cluster.SnappyConcurrencyTest taskMethod  = parseOptions
            threadGroups = snappySingleThread;*/

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSnappyJob
            io.snappydata.hydra.cluster.SnappyPrms-jobClassNames = io.snappydata.benchmark.snappy.tpch.TableCreationJob
            io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer =
            "dataLocation=${dataLocation},Buckets_Order_Lineitem=${buckets_Order_Lineitem},Buckets_Cust_Part_PartSupp=${Buckets_Cust_Part_PartSupp},IsSupplierColumnTable=${IsSupplierColumnTable},Buckets_Supplier=${Buckets_Supplier},Redundancy=${Redundancy},Persistence=${Persistence},Persistence_Type=${Persistence_Type},NumberOfLoadStage=${NumberOfLoadStage},isParquet=${isParquet},createParquet=${createParquet},traceEvents=${traceEvents}"
            io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-cluster*tests.jar
            threadGroups = snappyThreads
            ;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyConcurrencyTest taskMethod  = createAndLoadTablesForStabilityTest
            threadGroups = snappySingleThread;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyConcurrencyTest taskMethod  = createViewsAndDependantTablesForStabilityTest
            threadGroups = snappySingleThread;


hydra.Prms-totalTaskTimeSec           = 259200;
hydra.Prms-maxResultWaitSec           = 266400;

hydra.Prms-maxCloseTaskResultWaitSec  = 266400;
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
io.snappydata.hydra.cluster.SnappyPrms-isLongRunningTest = true;
io.snappydata.hydra.cluster.SnappyPrms-isStabilityTest = true;
io.snappydata.hydra.cluster.SnappyPrms-dataPathListForParquet = /export/shared/QA_DATA/NYCBigData2009-2016_Parquet /export/shared/QA_DATA/airlinedata_cleaned_3b /export/shared/QA_DATA/amezon_reviews_160Million_447;
io.snappydata.hydra.cluster.SnappyPrms-dataPathListForCSV = /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/patients.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/encounters.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/allergies.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/careplans.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/conditions.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/imaging_studies.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/immunizations.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/medications.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/observations.csv /export/dev13a/users/skapse/SyntheticData/Large_Data_1B_Dev13-Oct26Build/csv/procedures.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/patients.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/encounters.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/allergies.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/careplans.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/conditions.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/imaging_studies.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/immunizations.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/medications.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/observations.csv /export/dev14a/users/skapse/SyntheticData/Large_Data_1B_Dev14-Oct26Build/csv/procedures.csv;
io.snappydata.hydra.cluster.SnappyPrms-parquetExternalTableList = STAGING_NYCTAXI STAGING_AIRLINE STAGING_REVIEWS;
io.snappydata.hydra.cluster.SnappyPrms-csvExternalTableList = staging_patients staging_encounters staging_allergies staging_careplans staging_conditions staging_imaging_studies staging_immunizations staging_medications staging_observations staging_procedures staging_patients1 staging_encounters1 staging_allergies1 staging_careplans1 staging_conditions1 staging_imaging_studies1 staging_immunizations1 staging_medications1 staging_observations1 staging_procedures1;
io.snappydata.hydra.cluster.SnappyPrms-tableList = NYCTAXI AIRLINE REVIEWS patients encounters allergies careplans conditions imaging_studies immunizations medications observations procedures;
io.snappydata.hydra.cluster.SnappyPrms-tableType = column;
//io.snappydata.hydra.cluster.SnappyPrms-tableOptions = "partition_by 'id'" "buckets '17'" "redundancy '1'";
io.snappydata.hydra.cluster.SnappyPrms-externalTableListForInsert = STAGING_NYCTAXI STAGING_AIRLINE STAGING_REVIEWS staging_patients1 staging_encounters1 staging_allergies1 staging_careplans1 staging_conditions1 staging_imaging_studies1 staging_immunizations1 staging_medications1 staging_observations1 staging_procedures1;
io.snappydata.hydra.cluster.SnappyPrms-insertTableList =  NYCTAXI airline REVIEWS patients encounters allergies careplans conditions imaging_studies immunizations medications observations procedures;
