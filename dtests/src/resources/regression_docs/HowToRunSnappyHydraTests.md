# How To Run Snappy Hydra Tests

## Running battery tests

#### Pre-requisites:

 1. Build the product using ./gradlew clean buildAll

 2. Modify the local.conf files as per requirement and build the dtests using ./gradlew buildDtests.

 3. Make sure that no java processes are running on the machines

 4. Result directory path needs to be already created for storing the logs of test run

**Note:** SnappyData is build on 64-bit JVM and tests are also run using 64-bit JVM

#### Execute below command to run any snappy hydra bt

```
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
$SNAPPYDATA_SOURCE_DIR/sample-runbt.sh <result-directory-path> $SNAPPYDATA_SOURCE_DIR [-l <local-conf-file-path> -r <num-times-to-run-test> -m <mailAddresses>] <space-separated-list-of-bts>

E.g.
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh <logDir> $SNAPPYDATA_SOURCE_DIR -d false io/snappydata/hydra/northwind/northWind.bt
```

#### Running battery test using local.conf(with modifications in local.conf, as per requirement)

One can also provide a local.conf while running the battery test, to set some global parameters or set multi-hosts parameters to run the test on multiple nodes.

```
export SNAPPYDATA_SOURCE_DIR=<product checkout dir>
$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh <logDir> $SNAPPYDATA_SOURCE_DIR -l <path_for_local_conf> -d false io/snappydata/hydra/northwind/northWind.bt
```

#### Options available for running a battery test

```
 -l <local-conf-file-path> -- path to local conf file

 -r <n>                    -- run test suite n number of times, the default is 1

 -d <boolean>              -- whether to delete passed test run logs, the default value is true

 -m mail_address           -- email address to send results of the run to
```

#### Running single test/selected tests from a bt

If one wants to run just a single test or selected tests from the bt, then all the other tests in the bt file needs to be commented. One can also create a new .bt file with only required tests and use that bt file while execution. The above mentioned procedure for running a battery test needs to be followed after changes for bt files and local.conf are done (You can skip Step 1 if previously done).

## Battery tests included in the Snappy regression

#### The list of bts that are run as a part of regression for snappy can be found at [FunctionalTestList](functionalTestList.md), few of them are listed below

1. northWind.bt

2. ct.bt

3. sample.bt

4. distJoin.bt

5. clusterRestartWithPersistentRecovery.bt

#### Sample script used in running regression

[Here](../../test/java/io/snappydata/hydra/snappyRegressionScript.sh) is the sample for regression script, which includes all the bts to be run in the regression. Please set the following two variables required by the script, before executing:

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```
#### For additional logging, any of the following settings can be added to the local.conf files, as per requirement

```
io.snappydata.hydra.cluster.SnappyPrms-logLevel = fine;

hydra.VmPrms-extraVMArgs += "-DDistributionManager.VERBOSE=true";
hydra.VmPrms-extraVMArgs += "-Dgemfire.GetInitialImage.TRACE_GII_FINER=true";
hydra.VmPrms-extraVMArgs += "-ea -Dgemfirexd.debug.true=QueryDistribution,TraceQuery,TraceTranVerbose,TraceIndex,TraceFabricServiceBoot";

```