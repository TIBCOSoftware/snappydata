elapsedtime on;

set spark.sql.shuffle.partitions=8;

select id from persoon order by id;
