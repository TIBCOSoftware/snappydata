# How To Run RowStore Hydra Tests

## Running the battery tests

#### Pre-requisites:

* Build the product using the `./gradlew clean buildAll` command.

* Ensure that no java processes are running on the machines.

* The path for the result directory should be created for storing the logs of test run.

**Note:** SnappyData is build using 64-bit JVM and tests are also run using 64-bit JVM.

#### To run the battery test, execute below commands:

```
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
export JTESTS=$SNAPPYDATA_SOURCE_DIR/store/tests/sql/build-artifacts/linux/classes/java/main
$SNAPPYDATA_SOURCE_DIR/sample-runbt.sh <result-directory-path> $SNAPPYDATA_SOURCE_DIR [-l <local-conf-file-path> -r <num-times-to-run-test> -m <mail_address>] <space-separated-list-of-bts>

E.g.  For running sql.bt
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
export JTESTS=$SNAPPY_HOME/store/tests/sql/build-artifacts/linux/classes/java/main
$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh <logDir> $SNAPPYDATA_SOURCE_DIR -l $JTESTS/sql/snappy.local.conf sql/sql.bt
```

**Note:** The RowStore tests, by default, are run with `snappy.local.conf`, until another local.conf is required.

#### Options available for  running a battery test are:

| Options | Description |
|--------|--------|
|`-l <local-conf-file-path>`|Path to local conf file|
|`-r <n>`|Run test suite n number of times, the default is 1|
|`-d <boolean>`|Whether to delete passed test run logs, the default value is true|
|`-m mail_address`|Email address to send results of the run to|

#### Running single test/selected tests from a bt file:

If you want to run just a single test or selected tests from the bt, then all the other tests in the bt file needs to be commented. You can also create a new **.bt** file, with only required tests and use that file during execution.</br>
The product build should be done only after changes to the **bt** files and **local.conf** are done.

## Battery tests included in RowStore regression

#### The following battery tests have been selected to run as a part of regression for RowStore:

* sql.joins.sqlJoin.bt

* sql.joins.thinClient.sqlJoinThinClien.bt

* sql.sql.bt

* sql.sqlBridge.sqlBridge.bt

* sql.sqlDisk.sqlDisk.bt (Use local.conf - local.sql.disk.conf )

* sql.sqlDisk.sqlDiskNoIndexPersistence.bt (Use local.conf - local.noIndexPersistence.conf)

* sql.sqlDisk.sqlDiskWithNoPreAllocate.bt (Use local.conf - local.sql.preAllocate.conf)

* sql.sqlEviction.sqlEviction.bt

* sql.sqlTx.repeatableRead.sqlRRTx.bt

* sql.sqlTx.sqlDistTx.bt

* sql.sqlTx.sqlTxPersistence.sqlTxPersist.bt

* sql.sqlTx.thinClient.repeatableRead.thinClientRRTx.bt

* sql.sqlTx.thinClient.thinClientTx.bt

* sql.subquery.subquery.bt

* sql.view.sqlView.bt

* sql.wan.sqlWan.bt (Use local.conf - local.sql.uniqueKeyOnly.conf)

Each of the above mentioned battery test needs only one host to run.

#### Sample script used in running regression

[Here](../../test/java/io/snappydata/hydra/rowStoreRegressionScript.sh) is the sample for regression script, which includes all the battery tests to be run in the regression. 

Set the following two variables required by the script, before starting the execution:

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```

#### For additional logging any of the following settings can be added to the **local.conf** files, as per requirement:

```
hydra.GemFirePrms-logLevel                 = fine;
hydra.gemfirexd.FabricServerPrms-logLevel = fine;

hydra.VmPrms-extraVMArgs += "-DDistributionManager.VERBOSE=true";
hydra.VmPrms-extraVMArgs += "-Dgemfire.GetInitialImage.TRACE_GII_FINER=true";
hydra.VmPrms-extraVMArgs += "-ea -Dgemfirexd.debug.true=QueryDistribution,TraceJars,TraceTranVerbose,TraceIndex,TraceFabricServiceBoot,TraceExecution,TraceActivation,TraceTran";

//to increase VM memory
hydra.VmPrms-extraVMArgs += "-Xms1000m -Xmx1000m";
```
