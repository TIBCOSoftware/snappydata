io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"trade.portfolio"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"CREATE TABLE trade.portfolio (cid INT NOT NULL, sid INT NOT NULL, qty INT NOT NULL, availQty INT NOT NULL, subTotal DECIMAL(30,20), tid INT, constraint portf_pk PRIMARY KEY (cid, sid),
constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 AND availQty<=qty)) USING ROW OPTIONS(partition_by 'qty',PERSISTENT 'synchronous')"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(7,7,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(1,1,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(2,2,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(3,3,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(4,4,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(5,5,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(6,7,25,12,123.7,2)"
"INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(6,6,25,12,123.7,2)"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-ddlStmts =
"ALTER TABLE trade.portfolio ADD COLUMN name VARCHAR(10) NOT NULL default 'name'"
"ALTER TABLE trade.portfolio ADD COLUMN name1 VARCHAR(10)"
"ALTER TABLE trade.portfolio DROP COLUMN name1"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-selectStmts =
"SELECT * FROM trade.portfolio"
"DESCRIBE trade.portfolio"
;