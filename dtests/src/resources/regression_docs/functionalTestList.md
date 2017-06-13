#### This is the list of Hydra tests that should be run for a full regression for Snappy.

## Functional tests to be run in regressions

Following is the list of tests run in snappy regression. Any new bt added and needs to be run must be added here and in the [snappyRegressionScript.sh](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/snappyRegressionScript.sh)

1. sample.bt

2. distJoin.bt

3. clusterRestartWithPersistentRecovery.bt
    *Test upgradtion from gemxd cluster to snappy cluster. Modify and use [local.smartConnectorMode.conf](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/local.smartConnectorMode.conf)*

4. northWind.bt
    *Complete feature testing using northWind schema. Modify and use [local.smartConnectorMode.conf](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/local.smartConnectorMode.conf)*

5. ct.bt
    *Complete feature testing using ct schema. Modify and use [local.smartConnectorMode.conf](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/local.smartConnectorMode.conf)*

6. installJar.bt
    *Test installJar feature*

7. distIndex.bt
    *Test distributed Index feature. Modify and use [local.embeddedMode.conf](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/local.embeddedMode.conf)*

8. snapshotIsolation.bt
    *Test snapshotIsolation feature*

9. adAnalytics.bt
    *Test Kafka streaming*

## Long running tests

1. longRunningTest.bt
    *To test the system behavior after keeping the cluster running for long duration e.g. 40hrs split mode with HA, also test will be using different schemas. Modify and use local.longRun.conf*


#### Sample script used in running regression

[Here](https://github.com/SnappyDataInc/snappydata/tree/master/dtests/src/test/java/io/snappydata/hydra/snappyRegressionScript.sh) is the sample for regression script, which includes all the bts to be run in the regression. Please set the following two variables required by the script, before executing:

```
export SNAPPY_HOME=<checkout_dir>
export OUTPUT_DIR=<result_directory_path>
```