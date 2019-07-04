DROP TABLE IF EXISTS colTable;
create table if not exists colTable (id long, data string, company string, creation_date date) using column as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(1000000);
DROP TABLE IF EXISTS rowPartitionedTable;
create table if not exists rowPartitionedTable (id long, data string, company string, creation_date date) using row options (partition_by 'id') as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(1000000);
DROP TABLE IF EXISTS rowReplicatedTable;
create table if not exists rowReplicatedTable (id long, data string, company string, creation_date date) using row as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(1000000);
