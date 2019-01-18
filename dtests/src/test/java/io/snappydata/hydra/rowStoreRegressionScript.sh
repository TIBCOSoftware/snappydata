export JTESTS=$SNAPPY_HOME/store/tests/sql/build-artifacts/linux/classes/java/main

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlTx/thinClient/thinClientTx.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/joins/sqlJoin.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlEviction/sqlEviction.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sql.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/joins/thinClient/sqlJoinThinClien.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/view/sqlView.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlDisk/sqlDisk.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/regression/local.noIndexPersistence.conf sql/sqlDisk/sqlDiskNoIndexPersistence.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/regression/local.sql.preAllocate.conf sql/sqlDisk/sqlDiskWithNoPreAllocate.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlBridge/sqlBridge.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/subquery/subquery.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlTx/thinClient/repeatableRead/thinClientRRTx.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/regression/local.sql.uniqueKeyOnly.conf sql/wan/sqlWan.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlTx/sqlDistTx.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlTx/sqlTxPersistence/sqlTxPersist.bt
sleep 30;

$SNAPPY_HOME/store/tests/core/src/main/java/bin/sample-runbt.sh $OUTPUT_DIR/masterLogs $SNAPPY_HOME -l $JTESTS/sql/snappy.local.conf sql/sqlTx/repeatableRead/sqlRRTx.bt

