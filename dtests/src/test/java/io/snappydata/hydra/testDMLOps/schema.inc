io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema app";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.table1 (id int, name varchar(20),code int, tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-uniqueColumnName =
"id";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.table1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.table1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into table1 values (?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterInsertSelects =
"select count(*) from table1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"update table1 set code = case when code=-1 then 0 else (code+5) end "
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterUpdateSelects =
"select avg(code) from table1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from table1 where id < 100"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects =
"select count(*) from table1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-recreateTablesStatements =
"create table app.table1 (id int, name varchar(20),code int, qty double, tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertAfterRecreateTablesStmts =
"insert into table1 values (?,?,?,?,?)"
;

;