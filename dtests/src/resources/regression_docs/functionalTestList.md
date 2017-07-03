# Functional tests to be run in regressions

The list of tests executed in SnappyData regression. Any new bt added and needs to be executed as a part of regression, must be listed below in this file and in [snappyRegressionScript.sh](../../test/java/io/snappydata/hydra/snappyRegressionScript.sh)

* sample.bt

* distJoin.bt

* clusterRestartWithPersistentRecovery.bt

    - Test restart for snappy and spark clusters in any sequence and verify all the tables are recovered with persisted data (22GB of csv data). Modify and use [local.smartConnectorMode.conf](../../test/java/io/snappydata/hydra/local.smartConnectorMode.conf)

* northWind.bt

    - Complete feature testing using northWind schema. Modify and use [local.smartConnectorMode.conf](../../test/java/io/snappydata/hydra/local.smartConnectorMode.conf)

* ct.bt

    - Complete feature testing using ct schema. Modify and use [local.smartConnectorMode.conf](../../test/java/io/snappydata/hydra/local.smartConnectorMode.conf)

* installJar.bt

    - Test installJar feature

* distIndex.bt

    - Test distributed Index feature. Modify and use [local.embeddedMode.conf](../../test/java/io/snappydata/hydra/local.embeddedMode.conf)

* snapshotIsolation.bt

    - Test snapshotIsolation feature

* adAnalytics.bt

    - Test Kafka streaming

## Long running tests

* longRunningTest.bt

    - To test the system behavior after keeping the cluster running for long duration e.g. 40hrs split mode with HA, also test will be using different schemas. Modify and use local.longRun.conf*


#### Sample script used in running regression

[Here](../../test/java/io/snappydata/hydra/snappyRegressionScript.sh) is the sample for regression script, which includes all the battery tests to be run in the regression. 

Set the following two variables required by the script, before starting the execution:

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```

#### For details on how to run a hydra test for snappy, please refer [How To Run Snappy Hydra Tests](HowToRunSnappyHydraTests.md)
