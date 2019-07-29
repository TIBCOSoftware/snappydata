io.snappydata.hydra.testDMLOps.SnappySchemaPrms-snappyDDLExtn =
"using column options(partition_by 'id', buckets '1', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '12', COLUMN_BATCH_SIZE '1024', KEY_COLUMNS 'id')"
"using row options(partition_by 'id', buckets '1', redundancy '1', persistent 'sync')"
"using row options(persistent 'sync')"
"using column options(partition_by 'id', buckets '1', redundancy '1', persistent 'sync', COLUMN_MAX_DELTA_ROWS '12', COLUMN_BATCH_SIZE '1024', KEY_COLUMNS 'id')"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.table1 (id int, name varchar(20),code int, code1 double, code2 bigint, code3 varchar(30), tid int)"
"create table app.table2 (id int, name varchar(20),code int, code1 double, code2 bigint, code3 varchar(30), tid int)"
"create table app.table3 (id int, name varchar(20),code int, code1 double, code2 bigint, code3 varchar(30), tid int)"
"create table app.table4 (id int, name varchar(20),code int, code1 double, code2 bigint, code3 varchar(30), tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-uniqueColumnName =
"id"
"id"
"id"
"id"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.table1"
"app.table2"
"app.table3"
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.table1"
"app.table2"
"app.table3"
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into table1 values(?,?,?,?,?,?,?)"
"insert into table2 values(?,?,?,?,?,?,?)"
"insert into table3 values(?,?,?,?,?,?,?)"
"insert into table4 values(?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertTables =
"app.table1"
"app.table2"
"app.table3"
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterInsertSelects =
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table1"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table2"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table3"
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"update table1 set code = case when code=-1 then 0 else (code+1) end , code1=code1 + 1"
"update table2 set code = case when code=-1 then 0 else (code+1) end , code2=code2 + 1"
"update table3 set code = case when code=-1 then 0 else (code+1) end , code3=code3 + 1 "
"update table4 set code = case when code=-1 then 0 else (code+1) end , code1=code1 + 1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterUpdateSelects =
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table1"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table2"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table3"
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateTables =
"app.table1"
"app.table2"
"app.table3"
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from table1 where id <= $counter"
"delete from table2 where id <= $counter"
"delete from table3 where id <= $counter"
"delete from table4 where id <= $counter"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects =
"select count(*), avg(code), sum(code) from table1 where id <= $counter"
"select count(*), avg(code), sum(code) from table2 where id <= $counter"
"select count(*), avg(code), sum(code) from table3 where id <= $counter"
"select count(*), avg(code), sum(code) from table4 where id <= $counter"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteTables =
"app.table1"
"app.table2"
"app.table3"
"app.table4"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-putIntoStmts =
"put into table1 values (?,?,?,?,?,?,?)"
"put into table2 values (?,?,?,?,?,?,?)"
"put into table3 values (?,?,?,?,?,?,?)"
"put into table4 values (?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterPutIntoSelects =
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table1"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table2"
"select count(*), avg(code), sum(code), cast(sum(code2) as double), cast(avg(code2) as double) from table3"
"select count(*), avg(code), sum(code), cast(sum(code1) as double), cast(avg(code1) as double) from table4"
;
