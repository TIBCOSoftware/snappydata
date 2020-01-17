elapsedtime on;
create table tabl1 (id long, data string, company string, creation_date date) using column as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(10000);
create table tabl2 (id long, data string, company string, creation_date date) using row options (partition_by 'id') as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(10000);
create table tabl3 (id long, data string, company string, creation_date date) using row as select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(10000);
drop table tabl1;
drop table tabl2;
drop table tabl3;
