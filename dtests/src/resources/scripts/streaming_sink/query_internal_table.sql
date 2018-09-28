elapsedtime on;

set spark.sql.shuffle.partitions=7;

select * from SNAPPYSYS_INTERNAL____SINK_STATE_TABLE;