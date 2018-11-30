-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS testL;

create table testL (id long, data string, data2 decimal) using column options (partition_by 'id', key_columns 'id');
