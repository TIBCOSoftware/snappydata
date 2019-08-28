elapsedtime on;

create table persoon_details (
id long,
languages varchar(10),
primary key (id)
) using row options (partition_by 'id', redundancy '1');

insert into persoon_details select id,case when id % 2 = 0 then 'HINDI' else 'ENGLISH' end from range(1000000);