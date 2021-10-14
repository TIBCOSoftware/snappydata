export JTESTS=$SNAPPY_SOURCE/store/tests/sql/build-artifacts/linux/classes/java/main

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -l $JTESTS/io/snappydata/hydra/local.smartConnectorMode.conf -d false io/snappydata/hydra/northwind/northWind.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -l $JTESTS/io/snappydata/hydra/local.smartConnectorMode.conf -d false io/snappydata/hydra/clusterRestartWithPersistentRecovery.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -l $JTESTS/io/snappydata/hydra/local.embeddedMode.conf -d false io/snappydata/hydra/distIndex/distIndex.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -l $JTESTS/io/snappydata/hydra/local.smartConnectorMode.conf -d false io/snappydata/hydra/ct/ct.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -d false io/snappydata/hydra/installJar/installJar.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -d false io/snappydata/hydra/cluster/sample.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -d false io/snappydata/hydra/distJoin/distJoin.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -d false io/snappydata/hydra/snapshotIsolation/snapshotIsolation.bt
sleep 30;

$SNAPPY_SOURCE/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/snappyHydraLogs $SNAPPY_SOURCE -d false io/snappydata/hydra/adAnalytics/adAnalytics.bt

