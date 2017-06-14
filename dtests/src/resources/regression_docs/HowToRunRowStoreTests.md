# How To Run RowStore Hydra Tests

## Running the battery tests

#### Pre-requisites:

1. Build the product using ./gradlew clean buildAll

2. Ensure that no java processes are running on the machines

3. Result directory path needs to be already created for storing the logs of test run

**Note:** SnappyData is build using 64-bit JVM and tests are also run using 64-bit JVM.

#### To run the battery test, execute below commands

```
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
export JTESTS=$SNAPPYDATA_SOURCE_DIR/store/tests/sql/build-artifacts/linux/classes/main
$SNAPPYDATA_SOURCE_DIR/sample-runbt.sh <result-directory-path> $SNAPPYDATA_SOURCE_DIR [-l <local-conf-file-path> -r <num-times-to-run-test> -m <mailAddresses>] <space-separated-list-of-bts>

E.g.  For running sql.bt
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
export JTESTS=$SNAPPY_HOME/store/tests/sql/build-artifacts/linux/classes/main
$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh <logDir> $SNAPPYDATA_SOURCE_DIR -l $JTESTS/sql/snappy.local.conf sql/sql.bt
```

**Note:** The rowStore tests, by default, are run with snappy.local.conf, until another local.conf is required.

#### Options available for  running a battery test

```
 -l <local-conf-file-path> -- path to local conf file

 -r <n>                    -- run test suite n number of times, the default is 1

 -d <boolean>              -- whether to delete passed test run logs, the default value is true

 -m mail_address           -- email address to send results of the run to
```

#### Running single test/selected tests from a bt

If one wants to run just a single test or selected tests from the bt, then all the other tests in the bt file needs to be commented. One can also create a new .bt file, with only required tests and use that bt file while execution. The above mentioned procedure for running a battery test needs to be followed after changes for bt files and local.conf are done.

## Battery tests included in row store regression

#### The following battery tests have been selected to run as a part of regression for RowStore

1. sql.joins.sqlJoin.bt

2. sql.joins.thinClient.sqlJoinThinClien.bt

3. sql.sql.bt

4. sql.sqlBridge.sqlBridge.bt

5. sql.sqlDisk.sqlDisk.bt (Use local.conf - local.sql.disk.conf )

6. sql.sqlDisk.sqlDiskNoIndexPersistence.bt (Use local.conf - local.noIndexPersistence.conf)

7. sql.sqlDisk.sqlDiskWithNoPreAllocate.bt (Use local.conf - local.sql.preAllocate.conf)

8. sql.sqlEviction.sqlEviction.bt

9. sql.sqlTx.repeatableRead.sqlRRTx.bt

10. sql.sqlTx.sqlDistTx.bt

11. sql.sqlTx.sqlTxPersistence.sqlTxPersist.bt

12. sql.sqlTx.thinClient.repeatableRead.thinClientRRTx.bt

13. sql.sqlTx.thinClient.thinClientTx.bt

14. sql.subquery.subquery.bt

15. sql.view.sqlView.bt

16. sql.wan.sqlWan.bt (Use local.conf - local.sql.uniqueKeyOnly.conf)

Each of the above mentioned bts run on a single host only i.e. they need only one host to run.

#### Sample script used in running regression

[Here](../../test/java/io/snappydata/hydra/rowStoreRegressionScript.sh) is the sample for regression script, which includes all the bts to be run in the regression. Please set the following two variables required by the script, before executing:

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```

#### For additional logging any of the following settings can be added to the local.conf files, as per requirement

```
hydra.GemFirePrms-logLevel                 = fine;
hydra.gemfirexd.FabricServerPrms-logLevel = fine;

hydra.VmPrms-extraVMArgs += "-DDistributionManager.VERBOSE=true";
hydra.VmPrms-extraVMArgs += "-Dgemfire.GetInitialImage.TRACE_GII_FINER=true";
hydra.VmPrms-extraVMArgs += "-ea -Dgemfirexd.debug.true=QueryDistribution,TraceQuery,TraceTranVerbose,TraceIndex,TraceFabricServiceBoot";

//to increase VM memory
hydra.VmPrms-extraVMArgs += "-Xms1000m -Xmx1000m";
```
