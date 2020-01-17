io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createSchemas = "create schema app";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-createTablesStatements =
"create table app.temp_persoon (id bigint primary key, firstName varchar(30), middleName varchar(30), lastName varchar(30), title varchar(5), address varchar(40), country varchar(10), phone
varchar(12), dateOfBirth date, age int, status varchar(10), email varchar(30), education varchar(20), gender varchar(10), weight double, height double,
bloodGrp varchar(3), occupation varchar(15), hasChildren boolean, numChild int, hasSiblings boolean)
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList =
"app.temp_persoon"
;
