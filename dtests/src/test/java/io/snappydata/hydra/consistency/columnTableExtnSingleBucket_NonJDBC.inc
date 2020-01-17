INCLUDE $JTESTS/io/snappydata/hydra/consistency/columnTableExtnSingleBucket.inc;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmtsNonDMLTables=
"insert into table1 values(?,?,?,?,?,?,?)"
"insert into table4 values(?,?,?,?,?,?,?)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into table1 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"insert into table4 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-putIntoStmts =
"put into table1 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
"put into table4 select id,'name'||id,1,1,1,'code3'||id,$tid from range($range)"
;
