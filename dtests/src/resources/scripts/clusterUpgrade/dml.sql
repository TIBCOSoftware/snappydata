insert into colTable select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(2000001,2000201);
insert into rowPartitionedTable select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(2000001,2000101);
insert into rowReplicatedTable select id, 'somedata'||id, 'company'||cast((id/100) as int), date('2019-10-05')  from range(2000001,2000101);
update colTable set COMPANY='company_name_12345' where id> 110099;
update rowPartitionedTable set COMPANY='company_name_12345' where id> 110099;
update rowReplicatedTable set COMPANY='company_name_12345' where id> 110099;
delete from colTable where company='company1094';
delete from rowPartitionedTable where company='company1094';
delete from rowReplicatedTable where company='company1094';
