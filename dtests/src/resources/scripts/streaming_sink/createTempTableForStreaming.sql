elapsedtime on;

create table temp_persoon (
id long,
firstName varchar(30),
middleName varchar(30),
lastName varchar(30),
title varchar(5),
address varchar(40),
country varchar(10),
phone varchar(12),
dateOfBirth date,
birthTime timestamp,
age int,
status varchar(10),
email varchar(30),
education varchar(20),
gender varchar(12),
weight double,
height double,
bloodGrp varchar(3),
occupation varchar(15),
hasChildren boolean,
numChild int,
hasSiblings boolean
,primary key (id)
) using row options (partition_by 'id', redundancy '1');
