io.snappydata.hydra.testDMLOps.SnappySchemaPrms-snappyDDLExtn =
"using column options(partition_by 'id', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '12', COLUMN_BATCH_SIZE '1024')"
"using row options(partition_by 'id', redundancy '1', persistent 'sync')"
"using row options(persistent 'sync')"
"using column options(partition_by 'id', buckets '1', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '10', COLUMN_BATCH_SIZE '1024')"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements +=
"create table app.table4 (id int, name varchar(20),code int, code1 double, code2 long, code3 String, tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList +=
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables +=
"app.table4"
;
