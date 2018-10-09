elapsedtime on;

set spark.sql.shuffle.partitions=7;

select id from persoon order by id;