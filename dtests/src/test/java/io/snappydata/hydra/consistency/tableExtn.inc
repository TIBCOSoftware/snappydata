io.snappydata.hydra.testDMLOps.SnappySchemaPrms-snappyDDLExtn =
"using column options(partition_by 'id', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '12', COLUMN_BATCH_SIZE '1024')"
"using row options(partition_by 'id', redundancy '1', persistent 'sync')"
"using row options(persistent 'sync')"
"using column options(partition_by 'id', buckets '1', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '10', COLUMN_BATCH_SIZE '1024')"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements +=
"create table app.table4 (id int, name varchar(20),code int, code1 double, code2 bigint, code3
varchar(30), tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList +=
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables +=
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts +=
"insert into table4 values(?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertTables +=
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterInsertSelects +=
"select count(*) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts +=
"update table4 set code = case when code=-1 then 0 else (code+5) end "
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterUpdateSelects +=
"select avg(code) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts +=
"delete from table4 where code < 15 and code > 5"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects +=
"select count(*) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-putIntoStmts +=
"put into table4 values (?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterPutIntoSelects +=
"select count(*) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-batchSize=100;