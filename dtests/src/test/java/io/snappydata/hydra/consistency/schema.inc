io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema app";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.table1 (id int, name varchar(20),code int, code1 double, code2 long, code3 String, tid int)"
"create table app.table2 (id int, name varchar(20),code int, code1 double, code2 long, code3 String, tid int)"
"create table app.table3 (id int, name varchar(20),code int, code1 double, code2 long, code3 String, tid int)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.table1"
"app.table2"
"app.table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.table1"
"app.table2"
"app.table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into table1 select id, 'name'||id,-1,-1,-1,'-1',$tid from range $batchSize"
"insert into table2 select id, 'name'||id,-1,-1,-1,'-1',$tid from range $batchSize"
"insert into table3 select id, 'name'||id,-1,-1,-1,'-1',$tid from range $batchSize"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterInsertSelects =
"select count(*) from table1"
"select count(*) from table2"
"select count(*) from table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-updateStmts =
"update table1 set code = case when code=-1 then 0 else (code+5) end "
"update table2 set code = case when code=-1 then 0 else (code+5) end "
"update table3 set code = case when code=-1 then 0 else (code+5) end "
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterUpdateSelects =
"select avg(code) from table1"
"select avg(code) from table2"
"select avg(code) from table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-deleteStmts =
"delete from table1 where code < 15 and code > 5"
"delete from table2 where code < 15 and code > 5"
"delete from table3 where code < 15 and code > 5"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterDeleteSelects =
"select count(*) from table1"
"select count(*) from table2"
"select count(*) from table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-putIntoStmts =
"put into table1 values (?,?,?,?)"
"put into table2 values (?,?,?,?)"
"put into table3 values (?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-afterPutIntoSelects =
"select count(*) from table1"
"select count(*) from table2"
"select count(*) from table3"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-batchSize=100;