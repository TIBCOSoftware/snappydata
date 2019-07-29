
INCLUDE $JTESTS/io/snappydata/hydra/consistency/schema.inc;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmtsNonDMLTables=
"insert into table1 values(?,?,?,?,?,?,?)"
"insert into table2 values(?,?,?,?,?,?,?)"
"insert into table3 values(?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into table1 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"insert into table2 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"insert into table3 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-putIntoStmts =
"put into table1 values select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"put into table2 values select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"put into table3 values select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
;
