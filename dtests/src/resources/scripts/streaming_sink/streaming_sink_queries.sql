elapsedtime on;

set spark.sql.shuffle.partitions=7;

select count(*) from persoon;

--select * from persoon;